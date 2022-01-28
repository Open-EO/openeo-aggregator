import abc
import copy
import math
import re
import typing

from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo_aggregator.partitionedjobs import PartitionedJob, SubJob
from openeo_aggregator.utils import BoundingBox
from openeo_driver.ProcessGraphDeserializer import convert_node, ENV_DRY_RUN_TRACER, ConcreteProcessing
from openeo_driver.backend import OpenEoBackendImplementation, CollectionCatalog, AbstractCollectionCatalog
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.errors import OpenEOApiException
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv, spatial_extent_union, reproject_bounding_box

if typing.TYPE_CHECKING:
    from openeo_aggregator.backend import AggregatorProcessing, AggregatorBackendImplementation


class JobSplittingFailure(OpenEOApiException):
    code = "JobSplitFailure"


# Default maximum number of tiles to split in
# TODO: higher default maximum of tiles
MAX_TILES = 16


class AbstractJobSplitter(metaclass=abc.ABCMeta):
    """
    Split a given "large" batch job in "sub" batch jobs according to a certain strategy,
    for example: split in spatial sub-extents, split temporally, split per input collection, ...
    """

    @abc.abstractmethod
    def split(self, process: dict, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        # TODO: how to express dependencies? give SubJobs an id for referencing?
        # TODO: how to express combination/aggregation of multiple subjob results as a final result?
        ...


class FlimsySplitter(AbstractJobSplitter):
    """A simple job splitter that does not split."""

    def __init__(self, processing: "AggregatorProcessing"):
        self.processing = processing

    def split(self, process: dict, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        backend_id = self.processing.get_backend_for_process_graph(
            process_graph=process["process_graph"], api_version="TODO"
        )
        subjob = SubJob(process_graph=process["process_graph"], backend_id=backend_id)
        return PartitionedJob(process=process, metadata=metadata, job_options=job_options, subjobs=[subjob])


class TileGrid(typing.NamedTuple):
    """
    Specification of a tile grid, parsed from a string, e.g. 'wgs84-1degree', 'utm-100km', 'utm-20km', 'utm-10km'.
    """
    crs_type: str
    size: int
    unit: str

    @classmethod
    def from_string(cls, tile_grid: str) -> "TileGrid":
        """Parse a tile grid string"""
        m = re.match(r"^(?P<crs>[a-z0-9]+)-(?P<size>\d+)(?P<unit>[a-z]+)$", tile_grid.lower())
        if not m:
            raise JobSplittingFailure(f"Failed to parse tile_grid {tile_grid!r}")
        return cls(crs_type=m.group("crs"), size=int(m.group("size")), unit=m.group("unit"))

    def get_tiles(self, bbox: BoundingBox, max_tiles=MAX_TILES) -> typing.Iterator[BoundingBox]:
        """Calculate tiles to cover given bounding box"""
        if self.crs_type == "utm":
            # TODO: properly handle bbox that covers multiple UTM zones
            utm_zone: int = auto_utm_epsg_for_geometry(geometry=bbox.as_polygon(), crs=bbox.crs)
            tiling_crs = f"epsg:{utm_zone}"
            tile_size = self.size * {"km": 1000}[self.unit]
            x_offset = 500_000
        elif self.crs_type == "wgs84":
            tiling_crs = "epsg:4326"
            tile_size = self.size * {"degree": 1}[self.unit]
            x_offset = 0
        else:
            raise JobSplittingFailure(f"Unsupported tile grid {self.crs_type}")

        # Bounding box (in tiling CRS) to cover with tiles.
        to_cover = BoundingBox.from_dict(
            reproject_bounding_box(bbox.as_dict(), from_crs=bbox.crs, to_crs=tiling_crs)
        )
        # Get ranges of tile indices
        xmin, xmax = [int(math.floor((x - x_offset) / tile_size)) for x in [to_cover.west, to_cover.east]]
        ymin, ymax = [int(math.floor(y / tile_size)) for y in [to_cover.south, to_cover.north]]

        tile_count = (xmax - xmin + 1) * (ymax - ymin + 1)
        if tile_count > max_tiles:
            raise JobSplittingFailure(f"Tile count {tile_count} exceeds limit {max_tiles}")

        for x in range(xmin, xmax + 1):
            for y in range(ymin, ymax + 1):
                yield BoundingBox(
                    west=x * tile_size + x_offset,
                    south=y * tile_size,
                    east=(x + 1) * tile_size + x_offset,
                    north=(y + 1) * tile_size,
                    crs=tiling_crs,
                )


def find_new_id(prefix: str, is_new: typing.Callable[[str], bool], limit=100) -> str:
    """Helper to find a new (node) id"""
    if is_new(prefix):
        return prefix
    for i in range(limit):
        x = f"{prefix}_{i}"
        if is_new(x):
            return x
    raise JobSplittingFailure("New id iteration overflow")


class TileGridSplitter(AbstractJobSplitter):
    """Split spatial extent in UTM/LonLat tiles."""

    def __init__(self, processing: "AggregatorProcessing"):
        self.backend_implementation = OpenEoBackendImplementation(
            catalog=processing._catalog,
            processing=processing
        )

    def split(self, process: dict, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        # TODO: pass tile_grid from job_options or from save_result format options?

        global_spatial_extent = self._extract_global_spatial_extent(process)

        tile_grid = TileGrid.from_string(job_options["tile_grid"])
        tiles = tile_grid.get_tiles(bbox=global_spatial_extent, max_tiles=job_options.get("max_tiles", MAX_TILES))
        inject = self._filter_bbox_injector(process_graph=process["process_graph"])

        backend_id = self.backend_implementation.processing.get_backend_for_process_graph(
            process_graph=process["process_graph"], api_version="TODO"
        )
        subjobs = [
            SubJob(process_graph=inject(tile), backend_id=backend_id)
            for tile in tiles
        ]
        return PartitionedJob(process=process, metadata=metadata, job_options=job_options, subjobs=subjobs)

    def _extract_global_spatial_extent(self, process: dict) -> BoundingBox:
        """Extract global spatial extent from given process graph"""
        # TODO: drop deepcopy when `dereference_from_node_arguments` doesn't do
        #       in-place manipulation of original process dict anymore
        process = copy.deepcopy(process)
        process_graph = process["process_graph"]
        top_level_node = ProcessGraphVisitor.dereference_from_node_arguments(process_graph)
        result_node = process_graph[top_level_node]
        dry_run_tracer = DryRunDataTracer()
        backend_implementation = OpenEoBackendImplementation(
            catalog=self.backend_implementation.catalog,
            processing=ConcreteProcessing(),
        )
        convert_node(result_node, env=EvalEnv({
            ENV_DRY_RUN_TRACER: dry_run_tracer,
            "backend_implementation": backend_implementation,
            "version": "1.0.0",  # TODO
        }))
        source_constraints = dry_run_tracer.get_source_constraints()
        # get global spatial extent
        spatial_extents = [c["spatial_extent"] for _, c in source_constraints if "spatial_extent" in c]
        if not spatial_extents:
            raise JobSplittingFailure("No spatial extents found in process graph")
        global_extent = BoundingBox.from_dict(spatial_extent_union(*spatial_extents))
        return global_extent

    def _filter_bbox_injector(self, process_graph: dict) -> typing.Callable[[BoundingBox], dict]:
        """
        Build function that takes a bounding box and injects a filter_bbox node
        just before result the `save_result` node of a "template" process graph.
        """
        # Find result node id
        result_ids = [k for k, v in process_graph.items() if v.get("result")]
        if len(result_ids) != 1:
            raise JobSplittingFailure(f"Expected process graph with 1 result node but got {len(result_ids)}")
        result_id, = result_ids
        if process_graph[result_id]["process_id"] != "save_result":
            raise JobSplittingFailure(f"Expected a save_result node but got {process_graph[result_id]}")
        previous_node_id = process_graph[result_id]["arguments"]["data"]["from_node"]

        # Id of new `filter_bbox` node to inject
        inject_id = find_new_id("_agg_pj_filter_bbox", is_new=lambda s: s not in process_graph)

        def inject(bbox: BoundingBox) -> dict:
            nonlocal process_graph, inject_id, previous_node_id
            new = copy.deepcopy(process_graph)
            new[inject_id] = {
                "process_id": "filter_bbox",
                "arguments": {
                    "data": {"from_node": previous_node_id},
                    "extent": bbox.as_dict(),
                }
            }
            new[result_id]["arguments"]["data"] = {"from_node": inject_id}
            return new

        return inject
