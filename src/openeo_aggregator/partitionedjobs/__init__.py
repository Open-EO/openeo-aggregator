from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Union

from openeo_driver.errors import OpenEOApiException

from openeo_aggregator.utils import FlatPG, PGWithMetadata


class PartitionedJobFailure(OpenEOApiException):
    code = "PartitionedJobFailure"


class SubJob(NamedTuple):
    """A part of a partitioned job, target at a particular, single back-end."""

    # Process graph of the subjob (derived in some way from original parent process graph)
    process_graph: FlatPG
    # Id of target backend (or None if there is no dedicated backend)
    backend_id: Optional[str]


class PartitionedJob(NamedTuple):
    """A large or multi-back-end job that is split in several sub jobs"""

    # Original process graph
    process: PGWithMetadata
    metadata: dict
    job_options: dict
    subjobs: Dict[str, SubJob]
    # Optional dependencies between sub-jobs (maps subjob id to list of subjob ids it depdends on)
    dependencies: Dict[str, Sequence[str]] = {}

    @staticmethod
    def to_subjobs_dict(subjobs: Union[Sequence[SubJob], Dict[Any, SubJob]]) -> Dict[str, SubJob]:
        """Helper to convert a collection of SubJobs to a dictionary"""
        # TODO: hide this logic in a setter or __init__ (e.g. when outgrowing the constraints of typing.NamedTuple)
        if isinstance(subjobs, Sequence):
            # TODO: eliminate this `Sequence` code path, and just always work with dict?
            return {f"{i:04d}": j for i, j in enumerate(subjobs)}
        elif isinstance(subjobs, dict):
            return {str(k): v for k, v in subjobs.items()}
        else:
            raise ValueError(subjobs)


# Statuses of partitioned jobs and subjobs
STATUS_INSERTED = "inserted"  # Just inserted in internal storage (zookeeper), not yet created on a remote back-end
STATUS_CREATED = "created"  # Created on remote back-end
STATUS_RUNNING = "running"  # Covers openEO batch job statuses "created", "queued", "running"
STATUS_FINISHED = "finished"
STATUS_ERROR = "error"
