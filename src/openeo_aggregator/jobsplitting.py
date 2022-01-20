import collections
import contextlib
import datetime
import flask
import json
import logging
import os
import time
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from typing import NamedTuple, List, Dict, Optional, Iterator

from openeo.util import TimingLogger
from openeo_aggregator.config import CONNECTION_TIMEOUT_JOB_START
from openeo_aggregator.connection import MultiBackendConnection
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.users.auth import HttpAuthHandler

# TODO: not only support batch jobs but also sync jobs?


_log = logging.getLogger(__name__)


class SubJob(NamedTuple):
    """A part of a partitioned job, target at a particular, single back-end."""
    # Process graph of the subjob (derived in some way from original parent process graph)
    process_graph: dict
    # Id of target backend
    backend_id: str


class PartitionedJob(NamedTuple):
    """A large or multi-back-end job that is split in several sub jobs"""
    # Original process graph
    process: dict
    metadata: dict
    job_options: dict
    # List of sub-jobs
    subjobs: List[SubJob]


class JobSplitter:
    """
    Split a given "large" batch job in "sub" batch jobs according to a certain strategy,
    for example: split in spatial sub-extents, split temporally, split per input collection, ...
    """

    def __init__(self, backends: MultiBackendConnection):
        self._backends = backends

    def split(self, process: dict, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        # TODO: how to express dependencies? give SubJobs an id for referencing?
        # TODO: how to express combination/aggregation of multiple subjob results as a final result?
        return PartitionedJob(
            process=process,
            metadata=metadata, job_options=job_options,
            subjobs=[
                # TODO: real splitting
                SubJob(process_graph=process["process_graph"], backend_id=self._backends.first().id),
            ]
        )


# Statuses of partitioned jobs and subjobs
STATUS_INSERTED = "inserted"
STATUS_RUNNING = "running"  # Covers openEO batch job statuses "created", "queued", "running"
STATUS_FINISHED = "finished"
STATUS_ERROR = "error"


def generate_id_candidates(prefix: str = "", max_attempts=5) -> Iterator[str]:
    """Generator for storage id candidates."""
    if "{date}" in prefix:
        prefix = prefix.format(date=datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S"))
    for _ in range(max_attempts):
        yield prefix + "%08x" % (int.from_bytes(os.urandom(4), "big"))


class ZooKeeperPartitionedJobDB:
    """ZooKeeper based Partitioned job database"""

    # TODO: support for canceling?
    # TODO: extract abstract PartitionedJobDB for other storage backends?

    # Simplify mocking time for unit tests.
    _clock = time.time  # TODO: centralized helper for this test pattern

    def __init__(self, client: KazooClient, prefix: str = '/openeo-aggregator/jobsplitting/v1'):
        self._client = client
        self._prefix = prefix

    def _path(self, *path: str) -> str:
        """Helper to build a zookeeper path"""
        return "{r}/{p}".format(
            r=self._prefix.rstrip("/"),
            p="/".join(path).lstrip("/")
        )

    @contextlib.contextmanager
    def _connect(self):
        """Context manager to automatically start and stop zookeeper connection."""
        # TODO: handle nesting of this context manager smartly?
        # TODO: instead of blindly doing start/stop all the time,
        #       could it be more efficient to keep connection alive for longer time?
        self._client.start()
        try:
            yield self._client
        finally:
            self._client.stop()

    @staticmethod
    def serialize(**kwargs) -> bytes:
        """Serialize a dictionary (given as arguments) in JSON (UTF8 byte-encoded)."""
        return json.dumps(kwargs).encode("utf8")

    @staticmethod
    def deserialize(value: bytes) -> dict:
        """Deserialize bytes (assuming UTF8 encoded JSON mapping)"""
        return json.loads(value.decode("utf8"))

    def insert(self, job: PartitionedJob) -> str:
        """
        Insert a new partitioned job.

        :return: storage id of the partitioned job
        """

        with self._connect():

            # Insert parent node, with "static" (write once) metadata as associated data
            job_node_value = self.serialize(
                user="TODO",
                # TODO: more BatchJobMetdata fields
                created=self._clock(),
                process=job.process,
                metadata=job.metadata,
                job_options=job.job_options,
            )
            for pjob_id in generate_id_candidates(prefix="pj-{date}-"):
                try:
                    self._client.create(path=self._path(pjob_id), value=job_node_value, makepath=True)
                    break
                except NodeExistsError:
                    # TODO: check that NodeExistsError is thrown on existing job_ids
                    continue
            else:
                raise RuntimeError("Too much attempts to create new pjob_id")

            # Updatable metadata
            self._client.create(
                path=self._path(pjob_id, "status"),
                value=self.serialize(status=STATUS_INSERTED)
            )

            # Insert subjobs
            for i, subjob in enumerate(job.subjobs):
                sjob_id = f"{i:04d}"
                self._client.create(
                    path=self._path(pjob_id, "sjobs", sjob_id),
                    value=self.serialize(
                        process_graph=subjob.process_graph,
                        backend_id=subjob.backend_id,
                        # TODO:  dependencies/constraints between subjobs?
                    ),
                    makepath=True,
                )
                self._client.create(
                    path=self._path(pjob_id, "sjobs", sjob_id, "status"),
                    value=self.serialize(status=STATUS_INSERTED),
                )

        return pjob_id

    def get_pjob_metadata(self, pjob_id: str) -> dict:
        """Get metadata of partitioned job, given by storage id."""
        with self._connect():
            value, stat = self._client.get(self._path(pjob_id))
            return self.deserialize(value)

    def list_subjobs(self, pjob_id: str) -> Dict[str, dict]:
        """
        List subjobs (and their metadata) of given partitioned job.

        :return: dictionary mapping sub-job storage id to the sub-job's metadata.
        """
        listing = {}
        with self._connect():
            for child in self._client.get_children(self._path(pjob_id, "sjobs")):
                value, stat = self._client.get(self._path(pjob_id, "sjobs", child))
                listing[child] = self.deserialize(value)
        return listing

    def set_backend_job_id(self, pjob_id: str, sjob_id: str, job_id: str):
        """
        Store external backend's job id for given sub job

        :param pjob_id: (internal) storage id of partitioned job
        :param sjob_id: (internal) storage id of sub-job
        :param job_id: (external) id of corresponding openEO job on remote back-end.
        """
        with self._connect():
            self._client.create(
                path=self._path(pjob_id, "sjobs", sjob_id, "job_id"),
                value=self.serialize(job_id=job_id)
            )

    def get_backend_job_id(self, pjob_id: str, sjob_id: str) -> str:
        """Get external back-end job id of given sub job"""
        with self._connect():
            value, stat = self._client.get(self._path(pjob_id, "sjobs", sjob_id, "job_id"))
            return self.deserialize(value)["job_id"]

    def set_pjob_status(self, pjob_id: str, status: str, message: Optional[str] = None):
        """
        Store status of partitioned job (with optional message).

        :param pjob_id: (storage) id of partitioned job
        :param status: global status of partitioned job
        :param message: optional message, e.g. describing error
        """
        with self._connect():
            self._client.set(
                path=self._path(pjob_id, "status"),
                value=self.serialize(status=status, message=message, timestamp=self._clock())
            )

    def get_pjob_status(self, pjob_id: str) -> dict:
        """
        Get status of partitioned job.

        :param pjob_id: (storage) id if partitioned job
        :return: dictionary with "status" and "message"

        TODO return predefined struct instead of dict with fields "status" and "message"?
        """
        with self._connect():
            value, stat = self._client.get(self._path(pjob_id, "status"))
            return self.deserialize(value)

    def set_sjob_status(self, pjob_id: str, sjob_id: str, status: str, message: Optional[str] = None):
        """Store status of sub-job (with optional message)"""
        with self._connect():
            self._client.set(
                path=self._path(pjob_id, "sjobs", sjob_id, "status"),
                value=self.serialize(status=status, message=message, timestamp=self._clock()),
            )

    def get_sjob_status(self, pjob_id: str, sjob_id: str) -> dict:
        """Get status of sub-job"""
        with self._connect():
            value, stat = self._client.get(self._path(pjob_id, "sjobs", sjob_id, "status"))
            return self.deserialize(value)


class PartitionedJobTracker:
    """
    Manage (submit/start/collect) and track status of subjobs
    """

    def __init__(self, db: ZooKeeperPartitionedJobDB, backends: MultiBackendConnection):
        self._db = db
        self._backends = backends

    def submit(self, pjob: PartitionedJob) -> str:
        """
        Submit a partitioned job: store metadata of partitioned and related sub-jobs in the database.

        :param pjob:
        :return: (internal) storage id of partitioned job
        """
        pjob_id = self._db.insert(pjob)
        _log.info(f"Inserted partitioned job: {pjob_id}")
        return pjob_id

    def sync(self, pjob_id, flask_request: flask.Request):
        """
        Sync between aggregator's sub-job database and executing backends:
        - submit new (sub) jobs (if possible)
        - poll status of running jobs

        :param pjob_id: storage id of the partitioned job
        :param flask_request: flask request of owning user to use authentication from
        """
        # TODO: (timing) logging

        pjob_metadata = self._db.get_pjob_metadata(pjob_id)
        if self._db.get_pjob_status(pjob_id) == STATUS_INSERTED:
            self._db.set_pjob_status(pjob_id, status=STATUS_RUNNING)

        sjobs = self._db.list_subjobs(pjob_id)
        _log.info(f"Syncing partitioned job {pjob_id!r} with {len(sjobs)} sub-jobs")
        for sjob_id, sjob_metadata in sjobs.items():
            # TODO: limit number of concurrent jobs (per partitioned job, per user, ...)
            # TODO: check job's user against request's user
            con = self._backends.get_connection(sjob_metadata["backend_id"])
            sjob_status = self._db.get_sjob_status(pjob_id, sjob_id)["status"]
            _log.info(f"pjob {pjob_id!r} sjob {sjob_id!r} status {sjob_status}")
            if sjob_status == STATUS_INSERTED:
                try:
                    # TODO: different way to authenticate request?
                    with con.authenticated_from_request(request=flask_request), \
                            con.override(default_timeout=CONNECTION_TIMEOUT_JOB_START):
                        with TimingLogger(title=f"Create subjob {sjob_id} on backend {con.id}", logger=_log.info):
                            job = con.create_job(
                                process_graph=sjob_metadata["process_graph"],
                                title=f"Partitioned job {pjob_id} part {sjob_id}",
                                description=f"Partitioned job {pjob_id!r} ({len(sjobs)} parts): part {sjob_id!r} ",
                                plan=pjob_metadata.get("plan"), budget=pjob_metadata.get("budget"),
                                additional=pjob_metadata.get("job_options"),
                            )
                        _log.info(f"Created batch job {job.job_id} on backend {con.id} for {pjob_id}/{sjob_id}")
                        with TimingLogger(title=f"Start subjob {sjob_id}", logger=_log.info):
                            # TODO: conditionally start?
                            job.start_job()
                except Exception as e:
                    # TODO: detect recoverable issue and keep status INSERTED to retry with next "sync"?
                    self._db.set_sjob_status(
                        pjob_id, sjob_id, status=STATUS_ERROR,
                        message=f"Failed to create subjob: {e}"
                    )
                else:
                    self._db.set_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id, job_id=job.job_id)
                    self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_RUNNING, message="started")

            elif sjob_status == STATUS_RUNNING:
                try:
                    with con.authenticated_from_request(request=flask_request):
                        metadata = con.job(job_id=self._db.get_backend_job_id(pjob_id, sjob_id)).describe_job()
                    status = metadata["status"]
                    _log.info(f"New status for pjob {pjob_id} sjob {sjob_id}: {status}")
                    # TODO: handle job "progress" level?
                except Exception as e:
                    _log.error(f"Unexpected error while polling job status {pjob_id} {sjob_id}", exc_info=True)
                    # Skip unexpected failure for now (could be temporary)
                    # TODO: inspect error and flag as failed, skip temporary glitches, ....
                else:
                    if status == "finished":
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_FINISHED)
                        # TODO: collect results
                    elif status in {"created", "queued", "running"}:
                        # TODO: also store full status metadata result in status?
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_RUNNING, message=status)
                    else:
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=status)
            elif sjob_status == STATUS_ERROR:
                # TODO: is this a final state? https://github.com/Open-EO/openeo-api/issues/436
                pass
            elif sjob_status == STATUS_FINISHED:
                pass

            status_counts = collections.Counter(
                self._db.get_sjob_status(pjob_id, sjob_id)["status"] for sjob_id in sjobs
            )
            status_message = f"subjob stats: {dict(status_counts)}"
            _log.info(f"pjob {pjob_id} sjob status histogram: {status_counts}")
            statusses = set(status_counts)
            if statusses == {STATUS_FINISHED}:
                self._db.set_pjob_status(pjob_id, status=STATUS_FINISHED, message=status_message)
                # TODO: also collect all asset urls
            elif STATUS_RUNNING in statusses:
                self._db.set_pjob_status(pjob_id, status=STATUS_RUNNING, message=status_message)
            elif STATUS_ERROR in statusses:
                self._db.set_pjob_status(pjob_id, status=STATUS_ERROR, message=status_message)
            else:
                raise RuntimeError(f"Unhandled sjob status combination: {statusses}")

    def describe_job(self, pjob_id: str) -> dict:
        """RESTJob.describe_job() interface"""
        # TODO: automatically sync
        # self.sync(pjob_id=pjob_id, flask_request=None)
        metadata = self._db.get_pjob_metadata(pjob_id=pjob_id)
        status = self._db.get_pjob_status(pjob_id=pjob_id)["status"]
        status = {STATUS_INSERTED: "created"}.get(status, status)
        return BatchJobMetadata(
            id="TODO?", status=status,
            created=datetime.datetime.utcfromtimestamp(metadata["created"]),
            title=metadata["metadata"].get("title"),
            description=metadata["metadata"].get("description"),
            process=metadata["process"],
            # TODO more fields?
        ).to_api_dict(full=True)

    def get_status(self, pjob_id: str):
        # TODO
        return self._db.get_pjob_status(pjob_id)


class PartitionedJobConnection:
    """
    Connection-like interface for managing partitioned jobs.

    This fake connection acts as adapter between:
    - the `backend.py` logic that expects `BackendConnection` objects
      to send openEO REST HTTP requests
    - the `PartitionedJobTracker` logic
    """

    class Job:
        """RestJob-like interface"""

        def __init__(self, pjob_id: str, connection: 'PartitionedJobConnection'):
            self.pjob_id = pjob_id
            self.connection = connection

        def describe_job(self) -> dict:
            return self.connection.partitioned_job_tracker.describe_job(pjob_id=self.pjob_id)

    def __init__(self, partitioned_job_tracker: PartitionedJobTracker):
        self.partitioned_job_tracker = partitioned_job_tracker

    @contextlib.contextmanager
    def authenticated_from_request(self, request: flask.Request):
        # TODO
        try:
            yield self
        finally:
            ...

    def job(self, job_id: str):
        """Connection.job() interface"""
        return self.Job(pjob_id=job_id, connection=self)


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger("kazoo").setLevel(logging.WARNING)
    zk = KazooClient()

    process_graph = {"add": {"process_id": "add", "arguments": {"x": 3, "y": 5}, "result": True}}
    splitter = JobSplitter()
    pjob = splitter.split(process_graph=process_graph)

    db = ZooKeeperPartitionedJobDB(zk)

    backends = MultiBackendConnection({"vito": "https://openeo.vito.be/openeo/1.0"}, configured_oidc_providers=[])

    tracker = PartitionedJobTracker(db, backends=backends)
    pjob_id = tracker.submit(pjob)
    print(pjob_id)
    flask_request = flask.Request(
        {"HTTP_AUTHORIZATION": "Bearer basic//" + HttpAuthHandler.build_basic_access_token(user_id="stefaan")})
    tracker.sync(pjob_id, flask_request=flask_request)

    print(db.list_subjobs(pjob_id))

    while True:
        tracker.sync(pjob_id, flask_request=flask_request)
        print(tracker.get_status(pjob_id))
        time.sleep(5)


if __name__ == "__main__":
    main()
