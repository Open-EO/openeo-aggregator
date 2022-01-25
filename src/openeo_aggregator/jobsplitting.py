import collections
import contextlib
import datetime
import flask
import json
import logging
import threading
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from typing import NamedTuple, List, Dict, Optional

from openeo.rest.job import ResultAsset
from openeo.util import TimingLogger
from openeo_aggregator.config import CONNECTION_TIMEOUT_JOB_START
from openeo_aggregator.connection import MultiBackendConnection
from openeo_aggregator.utils import Clock, _UNSET
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import JobNotFinishedException, JobNotFoundException
from openeo_driver.users import User

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
STATUS_INSERTED = "inserted"  # Just inserted in internal storage (zookeeper), not yet created on a remote back-end
STATUS_CREATED = "created"  # Created on remote back-end
STATUS_RUNNING = "running"  # Covers openEO batch job statuses "created", "queued", "running"
STATUS_FINISHED = "finished"
STATUS_ERROR = "error"


class ZooKeeperPartitionedJobDB:
    """ZooKeeper based Partitioned job database"""

    # TODO: support for canceling?
    # TODO: extract abstract PartitionedJobDB for other storage backends (e.g. Elastic Search)?

    def __init__(self, client: KazooClient, prefix: str = '/openeo-aggregator/pj/v1'):
        self._client = client
        self._prefix = prefix

    def _path(self, pjob_id: str, *path: str) -> str:
        """Helper to build a zookeeper path"""
        assert pjob_id.startswith("pj-")
        month = pjob_id[3:9]
        path = (month, pjob_id) + path
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
        # TODO: also do compression (e.g. gzip)?
        return json.dumps(kwargs).encode("utf8")

    @staticmethod
    def deserialize(value: bytes) -> dict:
        """Deserialize bytes (assuming UTF8 encoded JSON mapping)"""
        return json.loads(value.decode("utf8"))

    def insert(self, user_id: str, pjob: PartitionedJob) -> str:
        """
        Insert a new partitioned job.

        :return: storage id of the partitioned job
        """
        with self._connect():
            # Insert parent node, with "static" (write once) metadata as associated data
            job_node_value = self.serialize(
                user_id=user_id,
                # TODO: more BatchJobMetdata fields
                created=Clock.time(),
                process=pjob.process,
                metadata=pjob.metadata,
                job_options=pjob.job_options,
            )
            # A couple of pjob_id attempts: start with current time based name and a suffix to counter collisions (if any)
            base_pjob_id = "pj-" + Clock.utcnow().strftime("%Y%m%d-%H%M%S")
            for pjob_id in [base_pjob_id] + [f"{base_pjob_id}-{i}" for i in range(1, 3)]:
                try:
                    self._client.create(path=self._path(pjob_id), value=job_node_value, makepath=True)
                    break
                except NodeExistsError:
                    # TODO: check that NodeExistsError is thrown on existing job_ids
                    # TODO: add a sleep() to back off a bit?
                    continue
            else:
                raise RuntimeError("Too much attempts to create new pjob_id")

            # Updatable metadata
            self._client.create(
                path=self._path(pjob_id, "status"),
                value=self.serialize(status=STATUS_INSERTED)
            )

            # Insert subjobs
            for i, subjob in enumerate(pjob.subjobs):
                sjob_id = f"{i:04d}"
                self._client.create(
                    path=self._path(pjob_id, "sjobs", sjob_id),
                    value=self.serialize(
                        process_graph=subjob.process_graph,
                        backend_id=subjob.backend_id,
                        title=f"Partitioned job {pjob_id} part {sjob_id} ({i + 1}/{len(pjob.subjobs)})",
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
                value=self.serialize(status=status, message=message, timestamp=Clock.time())
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
                value=self.serialize(status=status, message=message, timestamp=Clock.time()),
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

    def _check_user_access(self, user_id: str, pjob_id: str, pjob_metadata: Optional[dict] = None):
        pjob_metadata = pjob_metadata or self._db.get_pjob_metadata(pjob_id)
        expected_user_id = pjob_metadata["user_id"]
        if expected_user_id != user_id:
            _log.error(f"User access violation for job {pjob_id!r}: {expected_user_id!r} != {user_id!r}")
            # TODO: Better exception for this?
            raise JobNotFoundException(job_id=pjob_id)

    def create(self, user_id: str, pjob: PartitionedJob, flask_request: flask.Request) -> str:
        """
        Submit a partitioned job: store metadata of partitioned and related sub-jobs in the database.

        :param pjob:
        :return: (internal) storage id of partitioned job
        """
        pjob_id = self._db.insert(user_id=user_id, pjob=pjob)
        _log.info(f"Inserted partitioned job: {pjob_id}")
        self.create_sjobs(user_id=user_id, pjob_id=pjob_id, flask_request=flask_request)
        return pjob_id

    def create_sjobs(self, user_id: str, pjob_id: str, flask_request: flask.Request):
        """Create all sub-jobs on remote back-end for given partitioned job"""
        pjob_metadata = self._db.get_pjob_metadata(pjob_id)
        self._check_user_access(user_id=user_id, pjob_id=pjob_id, pjob_metadata=pjob_metadata)
        sjobs = self._db.list_subjobs(pjob_id)
        create_stats = collections.Counter()
        with TimingLogger(title=f"Create partitioned job {pjob_id!r} with {len(sjobs)} sub-jobs", logger=_log.info):
            for sjob_id, sjob_metadata in sjobs.items():
                sjob_status = self._db.get_sjob_status(pjob_id, sjob_id)["status"]
                _log.info(f"To create: {pjob_id!r}:{sjob_id!r} (status {sjob_status})")
                if sjob_status == STATUS_INSERTED:
                    new_status = self._create_sjob(
                        pjob_id=pjob_id, sjob_id=sjob_id,
                        pjob_metadata=pjob_metadata, sjob_metadata=sjob_metadata,
                        flask_request=flask_request,
                    )
                    create_stats[new_status] += 1
                else:
                    _log.warning(f"Not creating {pjob_id!r}:{sjob_id!r} (status {sjob_status})")

        pjob_status = STATUS_CREATED if create_stats[STATUS_CREATED] > 0 else STATUS_ERROR
        self._db.set_pjob_status(pjob_id=pjob_id, status=pjob_status, message=repr(create_stats))

    def _create_sjob(
            self, pjob_id: str, sjob_id: str,
            pjob_metadata: dict, sjob_metadata: dict,
            flask_request: flask.Request,
    ) -> str:
        try:
            con = self._backends.get_connection(sjob_metadata["backend_id"])
            # TODO: different way to authenticate request?
            with con.authenticated_from_request(request=flask_request), \
                    con.override(default_timeout=CONNECTION_TIMEOUT_JOB_START):
                with TimingLogger(title=f"Create {pjob_id}:{sjob_id} on backend {con.id}", logger=_log.info):
                    job = con.create_job(
                        process_graph=sjob_metadata["process_graph"],
                        title=sjob_metadata.get("title"),
                        # TODO: do plan/budget need conversion?
                        plan=pjob_metadata.get("plan"),
                        budget=pjob_metadata.get("budget"),
                        additional=pjob_metadata.get("job_options"),
                    )
            _log.info(f"Created {pjob_id}:{sjob_id} on backend {con.id} as batch job {job.job_id}")
            self._db.set_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id, job_id=job.job_id)
            self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_CREATED, message="created")
            return STATUS_CREATED
        except Exception as e:
            # TODO: detect recoverable issue and allow for retry?
            _log.error(f"Creation of {pjob_id}:{sjob_id} failed", exc_info=True)
            self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=f"Create failed: {e}")
            return STATUS_ERROR

    def start_sjobs(self, user_id: str, pjob_id: str, flask_request: flask.Request):
        """Start all sub-jobs on remote back-end for given partitioned job"""
        self._check_user_access(user_id=user_id, pjob_id=pjob_id)
        # TODO: only start a subset of sub-jobs
        sjobs = self._db.list_subjobs(pjob_id)
        start_stats = collections.Counter()
        with TimingLogger(title=f"Starting partitioned job {pjob_id!r} with {len(sjobs)} sub-jobs", logger=_log.info):
            for sjob_id, sjob_metadata in sjobs.items():
                sjob_status = self._db.get_sjob_status(pjob_id, sjob_id)["status"]
                _log.info(f"To Start: {pjob_id!r}:{sjob_id!r} (status {sjob_status})")
                if sjob_status == STATUS_CREATED:
                    new_status = self._start_sjob(
                        pjob_id=pjob_id, sjob_id=sjob_id,
                        sjob_metadata=sjob_metadata,
                        flask_request=flask_request,
                    )
                    start_stats[new_status] += 1
                else:
                    _log.warning(f"Not starting {pjob_id!r}:{sjob_id!r} (status {sjob_status})")

        pjob_status = STATUS_RUNNING if start_stats[STATUS_RUNNING] > 0 else STATUS_ERROR
        self._db.set_pjob_status(pjob_id=pjob_id, status=pjob_status, message=repr(start_stats))

    def _start_sjob(self, pjob_id: str, sjob_id: str, sjob_metadata: dict, flask_request: flask.Request) -> str:
        try:
            job_id = self._db.get_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id)
            con = self._backends.get_connection(sjob_metadata["backend_id"])
            # TODO: different way to authenticate request?
            with con.authenticated_from_request(request=flask_request), \
                    con.override(default_timeout=CONNECTION_TIMEOUT_JOB_START):
                with TimingLogger(title=f"Start subjob {sjob_id} on backend {con.id}", logger=_log.info):
                    job = con.job(job_id)
                    job.start_job()
            self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_RUNNING, message="started")
            return STATUS_RUNNING
        except Exception as e:
            # TODO: detect recoverable issue and allow for retry?
            _log.error(f"Start of {pjob_id}:{sjob_id} failed", exc_info=True)
            self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=f"Failed to start: {e}")
            return STATUS_ERROR

    def sync(self, user_id: str, pjob_id: str, flask_request: flask.Request):
        """
        Sync between aggregator's sub-job database and executing backends:
        - submit new (sub) jobs (if possible)
        - poll status of running jobs

        :param pjob_id: storage id of the partitioned job
        :param flask_request: flask request of owning user to use authentication from
        """
        # TODO: (timing) logging
        # TODO: limit number of remote back-end requests per sync?
        # TODO: throttle sync logic, or cache per request?
        self._check_user_access(user_id=user_id, pjob_id=pjob_id)

        sjobs = self._db.list_subjobs(pjob_id)
        _log.info(f"Syncing partitioned job {pjob_id!r} with {len(sjobs)} sub-jobs")
        for sjob_id, sjob_metadata in sjobs.items():
            # TODO: limit number of concurrent jobs (per partitioned job, per user, ...)
            con = self._backends.get_connection(sjob_metadata["backend_id"])
            sjob_status = self._db.get_sjob_status(pjob_id, sjob_id)["status"]
            _log.info(f"pjob {pjob_id!r} sjob {sjob_id!r} status {sjob_status}")
            if sjob_status == STATUS_INSERTED:
                _log.warning(f"pjob {pjob_id!r} sjob {sjob_id!r} not yet created on remote back-end")
            elif sjob_status == STATUS_CREATED:
                # TODO: dynamically start subjobs instead of starting all subjobs when partitioned job is started?
                pass
            elif sjob_status == STATUS_RUNNING:
                try:
                    with con.authenticated_from_request(request=flask_request):
                        job_id = self._db.get_backend_job_id(pjob_id, sjob_id)
                        metadata = con.job(job_id).describe_job()
                    status = metadata["status"]
                    _log.info(f"Got status for {pjob_id}:{sjob_id} ({job_id}): {status}")
                    # TODO: handle job "progress" level?
                except Exception as e:
                    _log.error(f"Unexpected error while polling job status {pjob_id}:{sjob_id}", exc_info=True)
                    # Skip unexpected failure for now (could be temporary)
                    # TODO: inspect error and flag as failed, skip temporary glitches, ....
                else:
                    if status == "finished":
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_FINISHED, message=status)
                        # TODO: collect result/asset URLS here already?
                    elif status in {"created", "queued", "running"}:
                        # TODO: also store full status metadata result in status?
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_RUNNING, message=status)
                    elif status in {"error", "canceled"}:
                        # TODO: handle "canceled" state properly?
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=status)
                    else:
                        _log.error(f"Unexpected status for {pjob_id}:{sjob_id} ({job_id}): {status}")
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=status)
            elif sjob_status == STATUS_ERROR:
                # TODO: status "error" is not necessarily final see https://github.com/Open-EO/openeo-api/issues/436
                pass
            elif sjob_status == STATUS_FINISHED:
                pass

        status_counts = collections.Counter(
            self._db.get_sjob_status(pjob_id, sjob_id)["status"] for sjob_id in sjobs
        )
        status_message = repr(status_counts)
        _log.info(f"pjob {pjob_id} sjob status histogram: {status_counts}")
        statusses = set(status_counts)
        if statusses == {STATUS_FINISHED}:
            self._db.set_pjob_status(pjob_id, status=STATUS_FINISHED, message=status_message)
        elif STATUS_RUNNING in statusses:
            self._db.set_pjob_status(pjob_id, status=STATUS_RUNNING, message=status_message)
        elif STATUS_CREATED in statusses or STATUS_INSERTED in statusses:
            pass
        elif STATUS_ERROR in statusses:
            self._db.set_pjob_status(pjob_id, status=STATUS_ERROR, message=status_message)
        else:
            raise RuntimeError(f"Unhandled sjob status combination: {statusses}")

    def describe_job(self, user_id: str, pjob_id: str, flask_request: flask.Request) -> dict:
        """RESTJob.describe_job() interface"""
        self.sync(user_id=user_id, pjob_id=pjob_id, flask_request=flask_request)
        pjob_metadata = self._db.get_pjob_metadata(pjob_id=pjob_id)
        self._check_user_access(user_id=user_id, pjob_id=pjob_id, pjob_metadata=pjob_metadata)
        status = self._db.get_pjob_status(pjob_id=pjob_id)["status"]
        status = {STATUS_INSERTED: "created"}.get(status, status)
        return BatchJobMetadata(
            id=pjob_id, status=status,
            created=datetime.datetime.utcfromtimestamp(pjob_metadata["created"]),
            title=pjob_metadata["metadata"].get("title"),
            description=pjob_metadata["metadata"].get("description"),
            process=pjob_metadata["process"],
            # TODO more fields?
        ).to_api_dict(full=True)

    def get_assets(self, user_id: str, pjob_id: str, flask_request: flask.Request) -> List[ResultAsset]:
        # TODO: do a sync if latest sync is too long ago?
        self._check_user_access(user_id=user_id, pjob_id=pjob_id)
        sjobs = self._db.list_subjobs(pjob_id)
        assets = []
        with TimingLogger(title=f"Collect assets of {pjob_id} ({len(sjobs)} sub-jobs)", logger=_log):
            for sjob_id, sjob_metadata in sjobs.items():
                sjob_status = self._db.get_sjob_status(pjob_id, sjob_id)["status"]
                if sjob_status in {STATUS_INSERTED, STATUS_CREATED, STATUS_RUNNING}:
                    raise JobNotFinishedException
                if sjob_status != STATUS_FINISHED:
                    # TODO Partial result https://github.com/Open-EO/openeo-api/pull/433
                    raise JobNotFinishedException
                # Get assets from remote back-end
                job_id = self._db.get_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id)
                con = self._backends.get_connection(sjob_metadata["backend_id"])
                with con.authenticated_from_request(request=flask_request):
                    # TODO: when some sjob assets fail, still continue with partial results
                    sjob_assets = con.job(job_id).get_results().get_assets()
                # TODO: rename assets to avoid collision across sjobs
                assets.extend(
                    ResultAsset(job=None, name=f"{sjob_id}-{a.name}", href=a.href, metadata=a.metadata)
                    for a in sjob_assets
                )
        _log.info(f"Collected {len(assets)} assets for {pjob_id}")
        return assets


class PartitionedJobConnection:
    """
    Connection-like interface for managing partitioned jobs.

    This fake connection acts as adapter between:
    - the `backend.py` logic that expects `BackendConnection` objects
      to send openEO REST HTTP requests
    - the `PartitionedJobTracker` logic that interacts with internal database
    """

    class JobAdapter:
        """
        Adapter for interfaces: `openeo.rest.RestJob`, `openeo.rest.JobResult`
        """

        def __init__(self, pjob_id: str, connection: 'PartitionedJobConnection'):
            self.pjob_id = pjob_id
            self.connection = connection

        def describe_job(self) -> dict:
            """Interface `RESTJob.describe_job`"""
            return self.connection.partitioned_job_tracker.describe_job(
                user_id=self.connection._user.user_id,
                pjob_id=self.pjob_id,
                flask_request=self.connection._flask_request
            )

        def start_job(self):
            """Interface `RESTJob.start_job`"""
            return self.connection.partitioned_job_tracker.start_sjobs(
                user_id=self.connection._user.user_id,
                pjob_id=self.pjob_id,
                flask_request=self.connection._flask_request
            )

        def get_results(self):
            """Interface `RESTJob.get_results`"""
            return self

        def get_assets(self):
            """Interface `openeo.rest.JobResult.get_asserts`"""
            return self.connection.partitioned_job_tracker.get_assets(
                user_id=self.connection._user.user_id,
                pjob_id=self.pjob_id,
                flask_request=self.connection._flask_request
            )

    def __init__(self, partitioned_job_tracker: PartitionedJobTracker):
        self.partitioned_job_tracker = partitioned_job_tracker
        self._auth_lock = threading.Lock()
        self._flask_request = None
        self._user = None

    @contextlib.contextmanager
    def authenticated_from_request(self, request: flask.Request, user: User = None):
        """Interface `BackendConnection.authenticated_from_request()`"""
        if not self._auth_lock.acquire(blocking=False):
            raise RuntimeError("Reentering authenticated_from_request")
        self._flask_request = request
        self._user = user
        try:
            yield self
        finally:
            self._flask_request = None
            self._user = None
            self._auth_lock.release()

    @contextlib.contextmanager
    def override(self, default_timeout: int = _UNSET, default_headers: dict = _UNSET):
        """Interface `BackendConnection.override()`"""
        try:
            yield self
        finally:
            pass

    def job(self, job_id: str):
        """Interface `Connection.job()`"""
        return self.JobAdapter(pjob_id=job_id, connection=self)
