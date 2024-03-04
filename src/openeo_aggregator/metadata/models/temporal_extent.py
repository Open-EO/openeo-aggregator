import datetime
from typing import Any, Dict, List, Optional, Set, Type, TypeVar, Union

import attr
from dateutil.parser import isoparse

from openeo_aggregator.metadata.utils import UNSET, Unset

T = TypeVar("T", bound="TemporalExtent")


@attr.s(auto_attribs=True)
class TemporalExtent:
    """The *potential* temporal extents of the features in the collection.

    Attributes:
        interval (Union[Unset, List[List[Optional[datetime.datetime]]]]): One or more time intervals that describe the
            temporal extent
            of the dataset.

            The first time interval describes the overall temporal extent
            of the data. All subsequent time intervals describe more
            precise time intervals, e.g. to identify clusters of data.
            Clients only interested in the overall extent will only need
            to access the first item in each array.
    """

    interval: Union[Unset, List[List[Optional[datetime.datetime]]]] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        interval: Union[Unset, List[List[Optional[str]]]] = UNSET
        if not isinstance(self.interval, Unset):
            interval = []
            for interval_item_data in self.interval:
                interval_item = []
                for interval_item_item_data in interval_item_data:
                    interval_item_item = (
                        interval_item_item_data.isoformat().replace("+00:00", "Z") if interval_item_item_data else None
                    )
                    interval_item.append(interval_item_item)

                interval.append(interval_item)

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if interval is not UNSET:
            field_dict["interval"] = interval

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        if "interval" not in d:
            raise ValueError("Could not parse TemporalExtent object, actual: {}".format(d))
        interval = []
        _interval = d.pop("interval", UNSET)
        for interval_item_data in _interval or []:
            interval_item = []
            _interval_item = interval_item_data
            for interval_item_item_data in _interval_item:
                _interval_item_item = interval_item_item_data
                interval_item_item: Optional[datetime.datetime]
                if _interval_item_item is None:
                    interval_item_item = None
                else:
                    interval_item_item = isoparse(_interval_item_item)

                interval_item.append(interval_item_item)

            interval.append(interval_item)

        collection_temporal_extent = cls(
            interval=interval,
        )

        collection_temporal_extent.additional_properties = d
        return collection_temporal_extent

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
    def merge_all(temporal_extent_list: List["TemporalExtent"]) -> "TemporalExtent":
        """
        Merge multiple temporal extents into one.

        :param temporal_extent_list: List of extents to merge each as a tuple of (collection_identifier, TemporalExtent)

        :return: Merged temporal extent
        """
        merged_intervals: List[List[Optional[datetime.datetime]]] = []
        for cid, temporal_extent in temporal_extent_list:
            if temporal_extent.interval is not UNSET:
                for interval in temporal_extent.interval:
                    if all([i is None for i in interval]):
                        continue
                    if interval not in merged_intervals:
                        merged_intervals.append(interval)
        if not merged_intervals:
            merged_intervals.append([None, None])
        return TemporalExtent(interval=list(merged_intervals))
