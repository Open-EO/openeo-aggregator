from typing import Any, Dict, List, Tuple, Type, TypeVar

import attr

from openeo_aggregator.metadata.models.spatial_extent import SpatialExtent
from openeo_aggregator.metadata.models.temporal_extent import TemporalExtent

T = TypeVar("T", bound="Extent")


@attr.s(auto_attribs=True)
class Extent:
    """The extent of the data in the collection. Additional members MAY
    be added to represent other extents, for example, thermal or
    pressure ranges.

    The first item in the array always describes the overall extent of
    the data. All subsequent items describe more preciseextents,
    e.g. to identify clusters of data.
    Clients only interested in the overall extent will only need to
    access the first item in each array.

        Attributes:
            spatial (SpatialExtent): The *potential* spatial extents of the features in the collection.
            temporal (TemporalExtent): The *potential* temporal extents of the features in the collection.
    """

    spatial: SpatialExtent
    temporal: TemporalExtent
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        spatial = self.spatial.to_dict()

        temporal = self.temporal.to_dict()

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "spatial": spatial,
                "temporal": temporal,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        if "spatial" not in d or "temporal" not in d:
            raise ValueError("Could not parse Extent object, actual: {}".format(d))
        spatial = SpatialExtent.from_dict(d.pop("spatial"))

        temporal = TemporalExtent.from_dict(d.pop("temporal"))

        collection_extent = cls(
            spatial=spatial,
            temporal=temporal,
        )

        collection_extent.additional_properties = d
        return collection_extent

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties

    @staticmethod
    def merge_all(extent_list: List[Tuple[str, "Extent"]]) -> "Extent":
        """Merge multiple extents into one extent.

        :param extent_list: List of extents to merge each as a tuple of (collection_identifier, extent)

        :return: Merged extent.
        """
        return Extent(
            spatial=SpatialExtent.merge_all([(cid, e.spatial) for cid, e in extent_list]),
            temporal=TemporalExtent.merge_all([(cid, e.temporal) for cid, e in extent_list]),
        )
