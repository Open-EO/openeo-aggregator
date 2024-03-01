import contextlib
import json
import logging
from typing import Dict, List, Optional

from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from openeo_driver.errors import JobNotFoundException

from openeo_aggregator.config import ConfigException, get_backend_config
from openeo_aggregator.partitionedjobs import (
    STATUS_INSERTED,
    PartitionedJob,
    PartitionedJobFailure,
    SubJob,
)
from openeo_aggregator.utils import Clock, strip_join, timestamp_to_rfc3339

_log = logging.getLogger(__name__)


class NoJobIdForSubJobException(PartitionedJobFailure):
    code = "NoJobIdForSubJob"


class ZooKeeperPartitionedJobDB:
    """ZooKeeper based Partitioned job database"""

    # TODO: "database" is a bit of a misnomer, this class is more about a thin abstraction layer above that
    # TODO: extract abstract PartitionedJobDB for other storage backends (e.g. Elastic Search)?

    NAMESPACE = "pj/v2"

    def __init__(self, client: KazooClient, prefix: str = None):
        self._client = client
        self._client_connected = False
        self._prefix = prefix or f"/openeo-aggregator/{self.NAMESPACE}"

    @classmethod
    def from_config(cls) -> "ZooKeeperPartitionedJobDB":
        # Get ZooKeeper client
        pjt_config = get_backend_config().partitioned_job_tracking
        if pjt_config.get("zk_client"):
            zk_client = pjt_config["zk_client"]
        elif pjt_config.get("zk_hosts"):
            zk_client = KazooClient(pjt_config.get("zk_hosts"))
        else:
            raise ConfigException("Failed to construct zk_client")
        # Determine ZooKeeper prefix
        base_prefix = get_backend_config().zookeeper_prefix
        assert len(base_prefix.replace("/", "")) >= 3
        partitioned_jobs_prefix = pjt_config.get("zookeeper_prefix", cls.NAMESPACE)
        prefix = strip_join("/", base_prefix, partitioned_jobs_prefix)
        return cls(client=zk_client, prefix=prefix)

    def _path(self, user_id: str, pjob_id: str = None, *path: str) -> str:
        """Helper to build a zookeeper path"""
        if pjob_id:
            assert pjob_id.startswith("pj-")
            path = (user_id, pjob_id) + path
        else:
            path = (user_id,) + path
        return strip_join("/", self._prefix, *path)

    @contextlib.contextmanager
    def _connect(self):
        """
        Context manager to automatically start and stop zookeeper connection.
        Nesting is supported: only the outer loop will actually start/stop
        """
        # TODO: instead of blindly doing start/stop all the time,
        #       could it be more efficient to keep connection alive for longer time?
        outer = not self._client_connected
        if outer:
            self._client.start()
            self._client_connected = True
        try:
            yield self._client
        finally:
            if outer:
                self._client.stop()
                self._client_connected = False

    @staticmethod
    def serialize(**kwargs) -> bytes:
        """Serialize a dictionary (given as arguments) in JSON (UTF8 byte-encoded)."""
        # TODO: also do compression (e.g. gzip)?
        return json.dumps(kwargs).encode("utf8")

    @staticmethod
    def deserialize(value: bytes) -> dict:
        """Deserialize bytes (assuming UTF8 encoded JSON mapping)"""
        return json.loads(value.decode("utf8"))

    def obtain_new_pjob_id(self, user_id: str, initial_value: bytes = b"", attempts: int = 3) -> str:
        """Obtain new, unique partitioned job id"""
        # A couple of pjob_id attempts: start with current time based name and a suffix to counter collisions (if any)
        base_pjob_id = "pj-" + Clock.utcnow().strftime("%Y%m%d-%H%M%S")
        with self._connect():
            for pjob_id in [base_pjob_id] + [f"{base_pjob_id}-{i}" for i in range(1, attempts)]:
                try:
                    self._client.create(path=self._path(user_id, pjob_id), value=initial_value, makepath=True)
                    # We obtained our unique id
                    return pjob_id
                except NodeExistsError:
                    # TODO: check that NodeExistsError is thrown on existing job_ids
                    # TODO: add a sleep() to back off a bit?
                    continue
        raise PartitionedJobFailure("Too much attempts to create new pjob_id")

    def insert(self, user_id: str, pjob: PartitionedJob) -> str:
        """
        Insert a new partitioned job.

        :return: storage id of the partitioned job
        """
        with self._connect():
            # Insert parent node, with "static" (write once) metadata as associated data
            pjob_node_value = self.serialize(
                user_id=user_id,
                # TODO: more BatchJobMetdata fields
                created=Clock.time(),
                process=pjob.process,
                metadata=pjob.metadata,
                job_options=pjob.job_options,
                # TODO: pjob.dependencies #95
            )
            pjob_id = self.obtain_new_pjob_id(user_id=user_id, initial_value=pjob_node_value)
            # Updatable metadata
            self.set_pjob_status(user_id=user_id, pjob_id=pjob_id, status=STATUS_INSERTED, create=True)

            # Insert subjobs
            # TODO #95 some subjobs are not fully defined if they have dependencies
            #       (e.g. load_result still has to be made concrete)
            #       Only create them when fully concrete,,
            #       or allow updates on this metadata?
            for i, (sjob_id, subjob) in enumerate(pjob.subjobs.items()):
                title = f"Partitioned job {pjob_id} part {sjob_id} ({i + 1}/{len(pjob.subjobs)})"
                self.insert_sjob(user_id=user_id, pjob_id=pjob_id, sjob_id=sjob_id, subjob=subjob, title=title)

        return pjob_id

    def insert_sjob(
        self,
        user_id: str,
        pjob_id: str,
        sjob_id: str,
        subjob: SubJob,
        title: Optional[str] = None,
        status: str = STATUS_INSERTED,
    ):
        with self._connect():
            self._client.create(
                path=self._path(user_id, pjob_id, "sjobs", sjob_id),
                value=self.serialize(process_graph=subjob.process_graph, backend_id=subjob.backend_id, title=title),
                makepath=True,
            )
            self.set_sjob_status(user_id=user_id, pjob_id=pjob_id, sjob_id=sjob_id, status=status, create=True)

    def get_pjob_metadata(self, user_id: str, pjob_id: str) -> dict:
        """Get metadata of partitioned job, given by storage id."""
        with self._connect():
            if not self._client.exists(self._path(user_id, pjob_id)):
                raise JobNotFoundException(job_id=pjob_id)
            value, stat = self._client.get(self._path(user_id, pjob_id))
            return self.deserialize(value)

    def list_subjobs(self, user_id: str, pjob_id: str) -> Dict[str, dict]:
        """
        List subjobs (and their metadata) of given partitioned job.

        :return: dictionary mapping sub-job storage id to the sub-job's metadata.
        """
        listing = {}
        with self._connect():
            if not self._client.exists(self._path(user_id, pjob_id)):
                raise JobNotFoundException(job_id=pjob_id)
            for child in self._client.get_children(self._path(user_id, pjob_id, "sjobs")):
                value, stat = self._client.get(self._path(user_id, pjob_id, "sjobs", child))
                listing[child] = self.deserialize(value)
        return listing

    def set_backend_job_id(self, user_id: str, pjob_id: str, sjob_id: str, job_id: str):
        """
        Store external backend's job id for given sub job

        :param pjob_id: (internal) storage id of partitioned job
        :param sjob_id: (internal) storage id of sub-job
        :param job_id: (external) id of corresponding openEO job on remote back-end.
        """
        with self._connect():
            self._client.create(
                path=self._path(user_id, pjob_id, "sjobs", sjob_id, "job_id"), value=self.serialize(job_id=job_id)
            )

    def get_backend_job_id(self, user_id: str, pjob_id: str, sjob_id: str) -> str:
        """Get external back-end job id of given sub job"""
        with self._connect():
            try:
                value, stat = self._client.get(self._path(user_id, pjob_id, "sjobs", sjob_id, "job_id"))
            except NoNodeError:
                raise NoJobIdForSubJobException(f"No job_id for {pjob_id}:{sjob_id}.")
            return self.deserialize(value)["job_id"]

    def set_pjob_status(
        self,
        user_id: str,
        pjob_id: str,
        status: str,
        message: Optional[str] = None,
        progress: int = None,
        create: bool = False,
    ):
        """
        Store status of partitioned job (with optional message).

        :param pjob_id: (storage) id of partitioned job
        :param status: global status of partitioned job
        :param message: optional message, e.g. describing error
        :param create: whether to create the node instead of updating
        """
        with self._connect():
            kwargs = dict(
                path=self._path(user_id, pjob_id, "status"),
                value=self.serialize(status=status, message=message, timestamp=Clock.time(), progress=progress),
            )
            if create:
                self._client.create(**kwargs)
            else:
                self._client.set(**kwargs)

    def get_pjob_status(self, user_id: str, pjob_id: str) -> dict:
        """
        Get status of partitioned job.

        :param pjob_id: (storage) id if partitioned job
        :return: dictionary with "status" and "message"

        TODO return predefined struct instead of dict with fields "status" and "message"?
        """
        with self._connect():
            value, stat = self._client.get(self._path(user_id, pjob_id, "status"))
            return self.deserialize(value)

    def set_sjob_status(
        self,
        user_id: str,
        pjob_id: str,
        sjob_id: str,
        status: str,
        message: Optional[str] = None,
        create: bool = False,
    ):
        """Store status of sub-job (with optional message)"""
        with self._connect():
            kwargs = dict(
                path=self._path(user_id, pjob_id, "sjobs", sjob_id, "status"),
                value=self.serialize(status=status, message=message, timestamp=Clock.time()),
            )
            if create:
                self._client.create(**kwargs)
            else:
                self._client.set(**kwargs)

    def get_sjob_status(self, user_id: str, pjob_id: str, sjob_id: str) -> dict:
        """Get status of sub-job"""
        with self._connect():
            value, stat = self._client.get(self._path(user_id, pjob_id, "sjobs", sjob_id, "status"))
            return self.deserialize(value)

    def describe_job(self, user_id: str, pjob_id: str) -> dict:
        with self._connect():
            pjob_metadata = self.get_pjob_metadata(user_id=user_id, pjob_id=pjob_id)
            status_data = self.get_pjob_status(user_id=user_id, pjob_id=pjob_id)
        status = status_data["status"]
        status = {STATUS_INSERTED: "created"}.get(status, status)
        return {
            "id": pjob_id,
            "status": status,
            "created": timestamp_to_rfc3339(pjob_metadata["created"]),
            "title": pjob_metadata["metadata"].get("title"),
            "description": pjob_metadata["metadata"].get("description"),
            "process": pjob_metadata["process"],
            "progress": status_data.get("progress"),
        }

    def list_user_jobs(self, user_id: str) -> List[dict]:
        jobs = []
        with self._connect():
            user_path = self._path(user_id=user_id)
            if self._client.exists(user_path):
                for pjob_id in self._client.get_children(path=user_path):
                    jobs.append(self.describe_job(user_id=user_id, pjob_id=pjob_id))
        return jobs
