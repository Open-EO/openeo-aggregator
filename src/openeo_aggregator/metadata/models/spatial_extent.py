from typing import Any, Dict, List, Set, Tuple, Type, TypeVar, Union, cast

import attr

from openeo_aggregator.caching import UNSET
from openeo_aggregator.metadata.utils import Unset

T = TypeVar("T", bound="SpatialExtent")


@attr.s(auto_attribs=True)
class SpatialExtent:
    """The *potential* spatial extents of the features in the collection.

    Attributes:
        bbox (Union[Unset, List[List[float]]]): One or more bounding boxes that describe the spatial extent
            of the dataset.

            The first bounding box describes the overall spatial extent
            of the data. All subsequent bounding boxes describe more
            precise bounding boxes, e.g. to identify clusters of data.
            Clients only interested in the overall spatial extent will
            only need to access the first item in each array.
    """

    bbox: Union[Unset, List[List[float]]] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        bbox: Union[Unset, List[List[float]]] = UNSET
        if not isinstance(self.bbox, Unset):
            bbox = []
            for bbox_item_data in self.bbox:
                bbox_item = bbox_item_data

                bbox.append(bbox_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if bbox is not UNSET:
            field_dict["bbox"] = bbox

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        if "bbox" not in d:
            raise ValueError("Could not parse SpatialExtent object, actual: {}".format(d))
        bbox = []
        _bbox = d.pop("bbox", UNSET)
        for bbox_item_data in _bbox or []:
            bbox_item = cast(List[float], bbox_item_data)

            bbox.append(bbox_item)

        collection_spatial_extent = cls(
            bbox=bbox,
        )

        collection_spatial_extent.additional_properties = d
        return collection_spatial_extent

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
    def merge_all(spatial_extent_list: List[Tuple[str, "SpatialExtent"]]) -> "SpatialExtent":
        """
        Merge multiple spatial extents into one.

        :param spatial_extent_list: List of extents to merge each as a tuple of (collection_identifier, SpatialExtent)

        :return: Merged spatial extent
        """
        merged_bbox: List[List[float]] = []
        for cid, spatial_extent in spatial_extent_list:
            if spatial_extent.bbox != Unset:
                for bbox in spatial_extent.bbox:
                    if bbox not in merged_bbox:
                        merged_bbox.append(bbox)
        if not merged_bbox:
            merged_bbox.append([-180, -90, 180, 90])
        return SpatialExtent(bbox=list(merged_bbox))
