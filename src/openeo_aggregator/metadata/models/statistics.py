import logging
from typing import Any, Dict, List, Type, TypeVar, Union, cast

import attr

_log = logging.getLogger(__name__)


T = TypeVar("T", bound="Statistics")


@attr.s(auto_attribs=True)
class Statistics:
    """By default, only ranges with a minimum and a maximum value can be specified. Ranges can be specified for ordinal
    values only, which means they need to have a rank order. Therefore, ranges can only be specified for numbers and
    some special types of strings. Examples: grades (A to F), dates or times. Implementors are free to add other derived
    statistical values to the object, for example `mean` or `stddev`.

        Attributes:
            min_ (Union[float, str]):
            max_ (Union[float, str]):
    """

    min_: Union[float, str]
    max_: Union[float, str]
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        min_: Union[float, str]

        min_ = self.min_

        max_: Union[float, str]

        max_ = self.max_

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "min": min_,
                "max": max_,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        if "min" not in d or "max" not in d:
            raise ValueError("Could not parse Statistics object, actual: {}".format(d))

        def _parse_min_(data: object) -> Union[float, str]:
            return cast(Union[float, str], data)

        min_ = _parse_min_(d.pop("min"))

        def _parse_max_(data: object) -> Union[float, str]:
            return cast(Union[float, str], data)

        max_ = _parse_max_(d.pop("max"))

        statistics = cls(
            min_=min_,
            max_=max_,
        )

        statistics.additional_properties = d
        return statistics

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

    def merge(self, other: "Statistics", report=_log.warning) -> "Statistics":
        # TODO: why is `report` different here from all other "reporter" usage?
        if not isinstance(other, Statistics):
            raise TypeError(f"Cannot merge {type(other)} with Statistics")
        # If min and max are strings, we can't merge them
        if isinstance(self.min_, str) or isinstance(self.max_, str):
            report(f"Statistics: Cannot merge {self} with {other} because one of them is a string")
            return self

        min_ = min(self.min_, other.min_)
        max_ = max(self.max_, other.max_)

        return Statistics(min_=min_, max_=max_)
