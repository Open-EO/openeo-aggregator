import collections
import contextlib
import datetime
import flask
import logging
import threading
from typing import List, Optional

from openeo.api.logs import LogEntry
from openeo.rest.job import ResultAsset
from openeo.util import TimingLogger, rfc3339
from openeo_aggregator.config import CONNECTION_TIMEOUT_JOB_START, AggregatorConfig
from openeo_aggregator.connection import MultiBackendConnection
from openeo_aggregator.partitionedjobs import PartitionedJob, STATUS_CREATED, STATUS_ERROR, STATUS_INSERTED, \
    STATUS_RUNNING, STATUS_FINISHED
from openeo_aggregator.partitionedjobs.zookeeper import ZooKeeperPartitionedJobDB, NoJobIdForSubJobException
from openeo_aggregator.utils import _UNSET
from openeo_driver.backend import BatchJobMetadata
from openeo_driver.errors import JobNotFinishedException, JobNotFoundException
from openeo_driver.users import User

# TODO: not only support batch jobs but also sync jobs?


_log = logging.getLogger(__name__)


class PartitionedJobTracker:
    """
    Manage (submit/start/collect) and track status of subjobs
    """

    def __init__(self, db: ZooKeeperPartitionedJobDB, backends: MultiBackendConnection):
        self._db = db
        self._backends = backends

    @classmethod
    def from_config(cls, config: AggregatorConfig, backends: MultiBackendConnection) -> "PartitionedJobTracker":
        return cls(db=ZooKeeperPartitionedJobDB.from_config(config), backends=backends)

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
        self._db.set_pjob_status(pjob_id=pjob_id, status=pjob_status, message=repr(create_stats), progress=0)

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
                with TimingLogger(title=f"Create {pjob_id}:{sjob_id} on backend {con.id}", logger=_log.info) as timer:
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
            self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_CREATED, message=f"Created in {timer.elapsed}")
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
        self._db.set_pjob_status(pjob_id=pjob_id, status=pjob_status, message=repr(start_stats), progress=0)

    def _start_sjob(self, pjob_id: str, sjob_id: str, sjob_metadata: dict, flask_request: flask.Request) -> str:
        try:
            job_id = self._db.get_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id)
            con = self._backends.get_connection(sjob_metadata["backend_id"])
            # TODO: different way to authenticate request?
            with con.authenticated_from_request(request=flask_request), \
                    con.override(default_timeout=CONNECTION_TIMEOUT_JOB_START):
                with TimingLogger(title=f"Start subjob {sjob_id} on backend {con.id}", logger=_log.info) as timer:
                    job = con.job(job_id)
                    job.start_job()
            self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_RUNNING, message=f"Started in {timer.elapsed}")
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
                    msg = f"Upstream status: {status!r}"
                    if status == "finished":
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_FINISHED, message=msg)
                        # TODO: collect result/asset URLS here already?
                    elif status in {"created", "queued", "running"}:
                        # TODO: also store full status metadata result in status?
                        # TODO: differentiate between created queued and running?
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_RUNNING, message=msg)
                    elif status in {"error", "canceled"}:
                        # TODO: handle "canceled" state properly?
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=msg)
                    else:
                        _log.error(f"Unexpected status for {pjob_id}:{sjob_id} ({job_id}): {status}")
                        self._db.set_sjob_status(pjob_id, sjob_id, status=STATUS_ERROR, message=msg)
            elif sjob_status == STATUS_ERROR:
                # TODO: status "error" is not necessarily final see https://github.com/Open-EO/openeo-api/issues/436
                pass
            elif sjob_status == STATUS_FINISHED:
                pass

        status_counts = collections.Counter(
            self._db.get_sjob_status(pjob_id, sjob_id)["status"] for sjob_id in sjobs
        )
        # TODO: more advanced progress metric than just #finished/#total?
        progress = int(100.0 * status_counts.get(STATUS_FINISHED, 0) / sum(status_counts.values()))
        status_message = repr(status_counts)
        _log.info(f"pjob {pjob_id} sjob status histogram: {status_counts}")
        statusses = set(status_counts)
        if statusses == {STATUS_FINISHED}:
            self._db.set_pjob_status(pjob_id, status=STATUS_FINISHED, message=status_message, progress=progress)
        elif STATUS_RUNNING in statusses:
            self._db.set_pjob_status(pjob_id, status=STATUS_RUNNING, message=status_message, progress=progress)
        elif STATUS_CREATED in statusses or STATUS_INSERTED in statusses:
            pass
        elif STATUS_ERROR in statusses:
            self._db.set_pjob_status(pjob_id, status=STATUS_ERROR, message=status_message, progress=progress)
        else:
            raise RuntimeError(f"Unhandled sjob status combination: {statusses}")

    def describe_job(self, user_id: str, pjob_id: str, flask_request: flask.Request) -> dict:
        """RESTJob.describe_job() interface"""
        self.sync(user_id=user_id, pjob_id=pjob_id, flask_request=flask_request)
        pjob_metadata = self._db.get_pjob_metadata(pjob_id=pjob_id)
        self._check_user_access(user_id=user_id, pjob_id=pjob_id, pjob_metadata=pjob_metadata)
        status_data = self._db.get_pjob_status(pjob_id=pjob_id)
        status = status_data["status"]
        status = {STATUS_INSERTED: "created"}.get(status, status)
        return {
            "id": pjob_id,
            "status": status,
            "created": rfc3339.datetime(datetime.datetime.utcfromtimestamp(pjob_metadata["created"])),
            "title": pjob_metadata["metadata"].get("title"),
            "description": pjob_metadata["metadata"].get("description"),
            "process": pjob_metadata["process"],
            "progress": status_data.get("progress"),
            "geometry": pjob_metadata["metadata"].get("_tiling_geometry")
        }

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
                # TODO: handle `get_backend_job_id` failure (e.g. `NoJobIdForSubJobException`)
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

    def get_logs(
            self, user_id: str, pjob_id: str, flask_request: flask.Request, offset: Optional[int] = None
    ) -> List[LogEntry]:
        self._check_user_access(user_id=user_id, pjob_id=pjob_id)
        sjobs = self._db.list_subjobs(pjob_id)
        all_logs = []
        with TimingLogger(title=f"Collect logs for {pjob_id} ({len(sjobs)} sub-jobs)", logger=_log):
            for sjob_id, sjob_metadata in sjobs.items():
                try:
                    job_id = self._db.get_backend_job_id(pjob_id=pjob_id, sjob_id=sjob_id)
                    con = self._backends.get_connection(sjob_metadata["backend_id"])
                    with con.authenticated_from_request(request=flask_request):
                        logs = con.job(job_id).logs(offset=offset)
                        for log in logs:
                            log["id"] = f"{sjob_id}-{log.id}"
                        all_logs.extend(logs)
                except Exception as e:
                    all_logs.append(LogEntry(
                        id=f"{sjob_id}-0", level="error", message=f"Failed to get logs of {pjob_id}:{sjob_id}: {e!r}"
                    ))

        return all_logs


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

        def logs(self, offset=None) -> List[LogEntry]:
            """Interface `RESTJob.logs`"""
            return self.connection.partitioned_job_tracker.get_logs(
                user_id=self.connection._user.user_id,
                pjob_id=self.pjob_id,
                flask_request=self.connection._flask_request,
                offset=offset
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
