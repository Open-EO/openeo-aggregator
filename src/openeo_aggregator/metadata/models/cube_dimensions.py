from typing import Any, Dict, List, Type, TypeVar

import attr

from openeo_aggregator.metadata.models.cube_dimension import CubeDimension

T = TypeVar("T", bound="CubeDimensions")


@attr.s(auto_attribs=True)
class CubeDimensions:
    """Uniquely named dimensions of the data cube.

    The keys of the object are the dimension names. For
    interoperability, it is RECOMMENDED to use the
    following dimension names if there is only a single
    dimension with the specified criteria:

    * `x` for the dimension of type `spatial` with the axis set to `x`
    * `y` for the dimension of type `spatial` with the axis set to `y`
    * `z` for the dimension of type `spatial` with the axis set to `z`
    * `t` for the dimension of type `temporal`
    * `bands` for dimensions of type `bands`

    This property REQUIRES to add `datacube` to the list
    of `stac_extensions`.

    """

    dimensions: Dict[str, CubeDimension] = attr.ib(init=True, factory=dict)

    def to_dict(self) -> Dict[str, Any]:

        field_dict: Dict[str, Any] = {}
        for prop_name, prop in self.dimensions.items():
            field_dict[prop_name] = prop.to_dict()

        field_dict.update({})

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        collection_stac_collection_cube_dimensions = cls()

        additional_properties = {}
        for prop_name, prop_dict in d.items():
            try:
                additional_property = CubeDimension.from_dict(prop_dict)
            except Exception as e:
                raise TypeError("Error parsing '%s' of CubeDimensions: %s" % (prop_name, e))
            additional_properties[prop_name] = additional_property

        collection_stac_collection_cube_dimensions.dimensions = additional_properties
        return collection_stac_collection_cube_dimensions

    @property
    def additional_keys(self) -> List[str]:
        return list(self.dimensions.keys())

    def __getitem__(self, key: str) -> CubeDimension:
        return self.dimensions[key]

    def __setitem__(self, key: str, value: CubeDimension) -> None:
        self.dimensions[key] = value

    def __delitem__(self, key: str) -> None:
        del self.dimensions[key]

    def __contains__(self, key: str) -> bool:
        return key in self.dimensions
