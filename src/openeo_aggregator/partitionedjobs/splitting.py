import abc
import copy
import math
import re
import typing
from typing import List

import pyproj
import shapely.geometry
import shapely.ops
from openeo.internal.process_graph_visitor import ProcessGraphVisitor
from openeo_driver.backend import OpenEoBackendImplementation
from openeo_driver.dry_run import DryRunDataTracer
from openeo_driver.ProcessGraphDeserializer import (
    ENV_DRY_RUN_TRACER,
    ConcreteProcessing,
    convert_node,
)
from openeo_driver.util.geometry import reproject_bounding_box, spatial_extent_union
from openeo_driver.util.utm import auto_utm_epsg_for_geometry
from openeo_driver.utils import EvalEnv

from openeo_aggregator.constants import JOB_OPTION_TILE_GRID
from openeo_aggregator.partitionedjobs import (
    PartitionedJob,
    PartitionedJobFailure,
    SubJob,
)
from openeo_aggregator.utils import BoundingBox, FlatPG, PGWithMetadata

if typing.TYPE_CHECKING:
    from openeo_aggregator.backend import AggregatorProcessing


class JobSplittingFailure(PartitionedJobFailure):
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
    def split(self, process: PGWithMetadata, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        # TODO: how to express combination/aggregation of multiple subjob results as a final result?
        ...


class FlimsySplitter(AbstractJobSplitter):
    """
    A simple job splitter that does not split.

    Mainly intended for testing and debugging purposes.
    """

    def __init__(self, processing: "AggregatorProcessing"):
        self.processing = processing

    def split(self, process: PGWithMetadata, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        process_graph = process["process_graph"]
        backend_id = self.processing.get_backend_for_process_graph(process_graph=process_graph, api_version="TODO")
        process_graph = self.processing.preprocess_process_graph(process_graph, backend_id=backend_id)

        subjob = SubJob(process_graph=process_graph, backend_id=backend_id)
        return PartitionedJob(
            process=process,
            metadata=metadata,
            job_options=job_options,
            subjobs=PartitionedJob.to_subjobs_dict([subjob]),
        )


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

    def get_tiles(self, bbox: BoundingBox, max_tiles=MAX_TILES) -> List[BoundingBox]:
        """Calculate tiles to cover given bounding box"""
        if self.crs_type == "utm":
            # TODO: properly handle bbox that covers multiple UTM zones #36
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
        to_cover = BoundingBox.from_dict(reproject_bounding_box(bbox.as_dict(), from_crs=bbox.crs, to_crs=tiling_crs))
        # Get ranges of tile indices
        xmin, xmax = [int(math.floor((x - x_offset) / tile_size)) for x in [to_cover.west, to_cover.east]]
        ymin, ymax = [int(math.floor(y / tile_size)) for y in [to_cover.south, to_cover.north]]

        tile_count = (xmax - xmin + 1) * (ymax - ymin + 1)
        if tile_count > max_tiles:
            raise JobSplittingFailure(f"Tile count {tile_count} exceeds limit {max_tiles}")

        tiles = []
        for x in range(xmin, xmax + 1):
            for y in range(ymin, ymax + 1):
                tiles.append(
                    BoundingBox(
                        west=x * tile_size + x_offset,
                        south=y * tile_size,
                        east=(x + 1) * tile_size + x_offset,
                        north=(y + 1) * tile_size,
                        crs=tiling_crs,
                    )
                )
        return tiles


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

    # metadata key for tiling grid geometry
    METADATA_KEY = "_tiling_geometry"

    def __init__(self, processing: "AggregatorProcessing"):
        self.backend_implementation = OpenEoBackendImplementation(catalog=processing._catalog, processing=processing)

    def split(self, process: PGWithMetadata, metadata: dict = None, job_options: dict = None) -> PartitionedJob:
        # TODO: refactor process graph preprocessing and backend_id getting in reusable AbstractJobSplitter method?
        processing: AggregatorProcessing = self.backend_implementation.processing
        process_graph = process["process_graph"]
        backend_id = processing.get_backend_for_process_graph(process_graph=process_graph, api_version="TODO")
        process_graph = processing.preprocess_process_graph(process_graph, backend_id=backend_id)

        global_spatial_extent = self._extract_global_spatial_extent(process)
        # TODO: pass tile_grid from job_options or from save_result format options?
        #       see https://github.com/openEOPlatform/architecture-docs/issues/187
        tile_grid = TileGrid.from_string(job_options[JOB_OPTION_TILE_GRID])
        tiles = tile_grid.get_tiles(bbox=global_spatial_extent, max_tiles=job_options.get("max_tiles", MAX_TILES))
        inject = self._filter_bbox_injector(process_graph=process_graph)

        subjobs = [SubJob(process_graph=inject(tile), backend_id=backend_id) for tile in tiles]

        # Store tiling geometry in metadata
        if metadata is None:
            metadata = {}
        metadata[self.METADATA_KEY] = {
            "global_spatial_extent": global_spatial_extent.as_dict(),
            "tiles": [t.as_dict() for t in tiles],
        }

        return PartitionedJob(
            process=process,
            metadata=metadata,
            job_options=job_options,
            subjobs=PartitionedJob.to_subjobs_dict(subjobs),
        )

    def _extract_global_spatial_extent(self, process: PGWithMetadata) -> BoundingBox:
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
        convert_node(
            result_node,
            env=EvalEnv(
                {
                    ENV_DRY_RUN_TRACER: dry_run_tracer,
                    "backend_implementation": backend_implementation,
                    "version": "1.0.0",  # TODO
                    "user": None,  # TODO
                }
            ),
        )
        source_constraints = dry_run_tracer.get_source_constraints()
        # get global spatial extent
        spatial_extents = [c["spatial_extent"] for _, c in source_constraints if "spatial_extent" in c]
        if not spatial_extents:
            raise JobSplittingFailure("No spatial extents found in process graph")
        global_extent = BoundingBox.from_dict(spatial_extent_union(*spatial_extents))
        return global_extent

    def _filter_bbox_injector(self, process_graph: FlatPG) -> typing.Callable[[BoundingBox], dict]:
        """
        Build function that takes a bounding box and injects a filter_bbox node
        just before result the `save_result` node of a "template" process graph.
        """
        # Find result node id
        result_ids = [k for k, v in process_graph.items() if v.get("result")]
        if len(result_ids) != 1:
            raise JobSplittingFailure(f"Expected process graph with 1 result node but got {len(result_ids)}")
        (result_id,) = result_ids
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
                },
            }
            new[result_id]["arguments"]["data"] = {"from_node": inject_id}
            return new

        return inject

    @staticmethod
    def tiling_geometry_to_geojson(geometry: dict, format: str) -> dict:
        """Convert tiling geometry metadata to GeoJSON format (MultiPolygon or FeatureCollection)."""
        # Load BoundingBox objects from metadata
        global_spatial_extent = BoundingBox.from_dict(geometry["global_spatial_extent"])
        tiles = [BoundingBox.from_dict(t) for t in geometry["tiles"]]

        # Reproject to shapely Polygons in lon-lat
        def reproject(bbox: BoundingBox) -> shapely.geometry.Polygon:
            polygon = shapely.ops.transform(
                pyproj.Transformer.from_crs(crs_from=bbox.crs, crs_to="epsg:4326", always_xy=True).transform,
                bbox.as_polygon(),
            )
            return polygon

        global_spatial_extent = reproject(global_spatial_extent)
        tiles = [reproject(t) for t in tiles]

        if format == "GeometryCollection":
            geom = shapely.geometry.GeometryCollection([global_spatial_extent, shapely.geometry.MultiPolygon(tiles)])
            return shapely.geometry.mapping(geom)
        elif format == "FeatureCollection":
            # Feature collection
            def feature(polygon, **kwargs):
                return {"type": "Feature", "geometry": shapely.geometry.mapping(polygon), "properties": kwargs}

            features = [feature(global_spatial_extent, id="global_spatial_extent")]
            for i, tile in enumerate(tiles):
                features.append(feature(tile, id=f"tile{i:04d}"))
            return {"type": "FeatureCollection", "features": features}
        else:
            raise ValueError(format)
