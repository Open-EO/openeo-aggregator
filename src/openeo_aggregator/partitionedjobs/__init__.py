from typing import List, NamedTuple

from openeo_driver.errors import OpenEOApiException


class PartitionedJobFailure(OpenEOApiException):
    code = "PartitionedJobFailure"


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


# Statuses of partitioned jobs and subjobs
STATUS_INSERTED = "inserted"  # Just inserted in internal storage (zookeeper), not yet created on a remote back-end
STATUS_CREATED = "created"  # Created on remote back-end
STATUS_RUNNING = "running"  # Covers openEO batch job statuses "created", "queued", "running"
STATUS_FINISHED = "finished"
STATUS_ERROR = "error"
