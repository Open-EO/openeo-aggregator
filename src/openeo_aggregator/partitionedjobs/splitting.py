from openeo_aggregator.connection import MultiBackendConnection
from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob


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

