import abc
import typing
from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob

if typing.TYPE_CHECKING:
    from openeo_aggregator.backend import AggregatorProcessing


class AbstractJobSplitter(metaclass=abc.ABCMeta):
    """
    Split a given "large" batch job in "sub" batch jobs according to a certain strategy,
    for example: split in spatial sub-extents, split temporally, split per input collection, ...
    """

    def __init__(self, processing: "AggregatorProcessing"):
        self.processing = processing

    @abc.abstractmethod
    def split(self, process: dict, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        # TODO: how to express dependencies? give SubJobs an id for referencing?
        # TODO: how to express combination/aggregation of multiple subjob results as a final result?
        ...


class NoSplitter(AbstractJobSplitter):
    """The simple job splitter that does not split."""

    def split(self, process: dict, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        backend_id = self.processing.get_backend_for_process_graph(
            process_graph=process["process_graph"], api_version="TODO"
        )
        subjobs = [
            SubJob(process_graph=process["process_graph"], backend_id=backend_id),
        ]
        return PartitionedJob(process=process, metadata=metadata, job_options=job_options, subjobs=subjobs)
