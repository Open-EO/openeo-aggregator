import functools
import logging
from typing import Any, Dict, List, Type, TypeVar, Union, cast, Set, Tuple

import attr

from openeo_aggregator.metadata.models.statistics import Statistics
from openeo_aggregator.metadata.utils import concat

T = TypeVar("T", bound="StacSummaries")


@attr.s(auto_attribs=True)
class StacSummaries:
    """Collection properties from STAC extensions (e.g. EO,
    SAR, Satellite or Scientific) or even custom extensions.

    Summaries are either a unique set of all available
    values *or* statistics. Statistics by default only
    specify the range (minimum and maximum values), but
    can optionally be accompanied by additional
    statistical values. The range can specify the
    potential range of values, but it is recommended to be
    as precise as possible. The set of values MUST contain
    at least one element and it is strongly RECOMMENDED to
    list all values. It is recommended to list as many
    properties as reasonable so that consumers get a full
    overview of the Collection. Properties that are
    covered by the Collection specification (e.g.
    `providers` and `license`) SHOULD NOT be repeated in the
    summaries.

    Potential fields for the summaries can be found here:

    * **[STAC Common Metadata](https://github.com/radiantearth/stac-spec/blob/v1.0.0-rc.2/item-spec/common-
    metadata.md)**:
      A list of commonly used fields throughout all domains
    * **[Content Extensions](https://github.com/radiantearth/stac-spec/blob/v1.0.0-rc.2/extensions/README.md#list-of-
    content-extensions)**:
      Domain-specific fields for domains such as EO, SAR and point clouds.
    * **Custom Properties**:
      It is generally allowed to add custom fields.

    """
    additional_properties: Dict[str, Union[List[Any], Statistics, Dict]] = attr.ib(init=True, factory=dict)

    def to_dict(self) -> Dict[str, Any]:

        field_dict: Dict[str, Any] = {}
        for prop_name, prop in self.additional_properties.items():

            if isinstance(prop, list):
                field_dict[prop_name] = prop

            else:
                field_dict[prop_name] = prop.to_dict()

        field_dict.update({})

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        collection_stac_summaries_collection_properties = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():

            def _parse_additional_property(data: object) -> Union[List[Any], Statistics, None]:
                try:
                    if not isinstance(data, list):
                        raise TypeError()
                    componentsschemas_stac_summaries_collection_properties_type_0 = cast(List[Any], data)

                    return componentsschemas_stac_summaries_collection_properties_type_0
                except:  # noqa: E722
                    pass
                if not isinstance(data, dict):
                    raise TypeError("Expected dict for '%s' of StacSummaries, actual %s" %
                                    (prop_name, type(data)))
                try:
                    componentsschemas_stac_summaries_collection_properties_type_1 = Statistics.from_dict(data)
                except ValueError as e:
                    raise TypeError("Error parsing '%s' of StacSummaries: %s" %
                                    (prop_name, e))
                return componentsschemas_stac_summaries_collection_properties_type_1

            additional_property = _parse_additional_property(prop_dict)
            if additional_property is not None:
                additional_properties[prop_name] = additional_property

        collection_stac_summaries_collection_properties.additional_properties = additional_properties
        return collection_stac_summaries_collection_properties

    @property
    def additional_keys(self) -> List[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Union[List[Any], Statistics]:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Union[List[Any], Statistics]) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties

    @staticmethod
    def merge_all(
            summaries_list: List[Tuple[str, "StacSummaries"]],
            report=logging.getLogger().warning
    ) -> "StacSummaries":
        """
        Merge all summaries into one.

        :param summaries_list: List of summaries to merge each as a tuple of (collection_identifier, summary).
        :param report: logging function to report errors

        :return: Merged summaries
        """
        additional_properties = [(cid, x.additional_properties) for cid, x in summaries_list]
        # Calculate the unique summary names.
        unique_summary_names: Set[str] = functools.reduce(lambda a, b: a.union(b), (d.keys() for _, d in additional_properties), set())
        
        merged_addition_properties = {}
        for summary_name in unique_summary_names:
            if summary_name in ["constellation", "platform", "instruments"]:
                merged_addition_properties[summary_name] = concat(additional_properties, summary_name, [list], report)
            elif summary_name.startswith("sar:") or summary_name.startswith("sat:"):
                merged_addition_properties[summary_name] = concat(additional_properties, summary_name, [list], report)
            else:
                backends = [cid for cid, d in additional_properties if summary_name in d]
                report(f"{backends}: Unhandled merging of StacSummaries for summary_name: {summary_name!r}", "warning")
        return StacSummaries(additional_properties=merged_addition_properties)
