from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import attr

from openeo_aggregator.metadata.utils import UNSET, Unset


class DimensionType(str, Enum):
    SPATIAL = "spatial"
    TEMPORAL = "temporal"
    BANDS = "bands"
    OTHER = "other"

    def __str__(self) -> str:
        return str(self.value)


T = TypeVar("T", bound="CubeDimension")


@attr.s(auto_attribs=True)
class CubeDimension:
    """
    Attributes:
        type (DimensionType): Type of the dimension.
        description (Union[Unset, str]): Detailed description to explain the entity.
    """

    type: DimensionType
    description: Union[Unset, str] = UNSET
    # Extent is used for defining temporal, and spatial extent.
    extent: Union[List[Union[Unset, float, int, str]], Unset] = UNSET
    # Values is used for defining available bands.
    values: Union[List[Union[float, int, str]], Unset] = UNSET
    step: Union[float, int, Unset] = UNSET
    unit: Union[Unset, str] = UNSET
    reference_system: Union[Unset, str, float, int, dict] = UNSET
    additional_properties: Dict[str, Any] = attr.ib(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        dim_type = self.type.value

        description = self.description

        field_dict: Dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "type": dim_type,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description

        return field_dict

    def _set_extent_from_dict(self, d, is_required, is_open, is_spatial, identifier):
        """
        An open extent can contain null values.
        Spatial extents can only contain numbers.
        Non spatial extents can only contain strings.
        """
        invalid_error = ValueError(
            "Could not parse CubeDimension object, extent for {} is invalid. " "actual: {}".format(identifier, d)
        )
        # Include identifier
        extent = d.get("extent", UNSET)
        if is_required and (extent is UNSET or extent is None):
            raise ValueError(
                "Could not parse CubeDimension object, extent for {} is required. " "actual: {}".format(identifier, d)
            )
        if extent is not UNSET:
            if not isinstance(extent, list):
                raise invalid_error
            if len(extent) != 2:
                raise invalid_error
            for e in extent:
                type_check = isinstance(e, (float, int)) if is_spatial else isinstance(e, str)
                if is_open:
                    if e is not None and not type_check:
                        raise invalid_error
                else:
                    if not type_check:
                        raise invalid_error
        self.extent = extent

    def _set_values_from_dict(self, d, types, identifier):
        invalid_error = ValueError(
            "Could not parse CubeDimension object, values for {} is invalid. " "actual: {}".format(identifier, d)
        )
        values = d.get("values", UNSET)
        if values is not UNSET:
            if not isinstance(values, list):
                raise invalid_error
            for v in values:
                if not isinstance(v, tuple(types)):
                    raise invalid_error
        self.values = values

    def _set_step_from_dict(self, d, types, identifier):
        invalid_error = ValueError(
            "Could not parse CubeDimension object, step for {} is invalid. " "actual: {}".format(identifier, d)
        )
        step = d.get("step", UNSET)
        if step is not UNSET:
            if not isinstance(step, tuple(types)):
                raise invalid_error
        self.step = step

    def _set_reference_system_from_dict(self, d, types, identifier):
        invalid_error = ValueError(
            "Could not parse CubeDimension object, reference_system for {} is invalid. "
            "actual: {}".format(identifier, d)
        )
        reference_system = d.get("reference_system", UNSET)
        if reference_system is not UNSET:
            if not isinstance(reference_system, tuple(types)):
                raise invalid_error
        self.reference_system = reference_system

    def _set_description_from_dict(self, d, identifier):
        invalid_error = ValueError(
            "Could not parse CubeDimension object, description for {} is invalid. " "actual: {}".format(identifier, d)
        )
        _description = d.get("description", UNSET)
        if _description is not UNSET:
            if not isinstance(_description, str):
                raise invalid_error
        self.description = _description

    def _set_type_from_dict(self, d, expected_type, identifier):
        invalid_error = ValueError(
            "Could not parse CubeDimension object, type for {} is invalid. " "actual: {}".format(identifier, d)
        )
        _type = d.get("type", UNSET)
        if _type is UNSET:
            raise ValueError(
                "Could not parse CubeDimension object, type for {} is required. " "actual: {}".format(identifier, d)
            )
        if not isinstance(_type, str):
            raise invalid_error
        if expected_type == DimensionType.OTHER.value:
            # The other type can be any string, excluding the existing types.
            if _type in DimensionType.__members__:
                raise invalid_error
            self.type = DimensionType(_type)
            return
        if _type != expected_type:
            raise ValueError(
                "Could not parse CubeDimension object, expected type for {} is {}, "
                "actual: {}".format(identifier, expected_type, d)
            )
        self.type = DimensionType(_type)

    def _set_horizontal_spatial_dimension_from_dict(self, src_dict: Dict[str, Any]):
        d = src_dict.copy()
        invalid_error = ValueError(
            "Could not parse CubeDimension object, horizontal dimension is invalid. " "actual: {}".format(d)
        )
        for required_field in ["type", "extent", "axis"]:
            if required_field not in d:
                raise ValueError(
                    "Could not parse CubeDimension object, required field {} for horizontal dimension "
                    "is missing. actual: {}".format(required_field, d)
                )
        _axis = d.get("axis", UNSET)
        if _axis not in ["x", "y"]:
            raise invalid_error
        self.axis = _axis

        identifier = "horizontal dimension"
        self._set_type_from_dict(d, DimensionType.SPATIAL.value, identifier)
        self._set_description_from_dict(d, identifier)
        self._set_extent_from_dict(d, is_required=True, is_open=False, is_spatial=True, identifier=identifier)
        self._set_values_from_dict(
            d,
            types=(
                float,
                int,
            ),
            identifier=identifier,
        )
        self._set_step_from_dict(
            d,
            types=(
                float,
                int,
            ),
            identifier=identifier,
        )
        self._set_reference_system_from_dict(d, types=(str, float, int, dict), identifier=identifier)

    def _set_vertical_spatial_dimension_from_dict(self, src_dict: Dict[str, Any]):
        d = src_dict.copy()
        invalid_error = ValueError(
            "Could not parse CubeDimension object, vertical dimension is invalid. " "actual: {}".format(d)
        )
        for required_field in ["type", "axis"]:
            if required_field not in d:
                raise ValueError(
                    "Could not parse CubeDimension object, required field {} for vertical dimension "
                    "is missing. actual: {}".format(required_field, d)
                )
        _axis = d.get("axis", UNSET)
        if _axis not in ["z"]:
            raise invalid_error
        self.axis = _axis

        identifier = "vertical dimension"
        self._set_type_from_dict(d, DimensionType.SPATIAL.value, identifier)
        self._set_description_from_dict(d, identifier)
        self._set_extent_from_dict(d, is_required=False, is_open=True, is_spatial=True, identifier=identifier)
        self._set_values_from_dict(d, types=(float, int, str), identifier=identifier)
        self._set_step_from_dict(d, types=(float, int, type(None)), identifier=identifier)
        self._set_reference_system_from_dict(d, types=(str, float, int, dict), identifier=identifier)

    def _set_temporal_dimension_from_dict(self, src_dict: Dict[str, Any]):
        d = src_dict.copy()
        for required_field in ["type", "extent"]:
            if required_field not in d:
                raise ValueError(
                    "Could not parse CubeDimension object, required field {} for temporal dimension "
                    "is missing. actual: {}".format(required_field, d)
                )
        identifier = "temporal dimension"
        self._set_type_from_dict(d, DimensionType.TEMPORAL.value, identifier)
        self._set_description_from_dict(d, identifier)
        self._set_extent_from_dict(d, is_required=True, is_open=True, is_spatial=False, identifier=identifier)
        self._set_values_from_dict(d, types=(str,), identifier=identifier)
        self._set_step_from_dict(d, types=(str, type(None)), identifier=identifier)

    def _set_other_dimension_from_dict(self, src_dict: Dict[str, Any]):
        d = src_dict.copy()
        for required_field in ["type"]:
            if required_field not in d:
                raise ValueError(
                    "Could not parse CubeDimension object, required field {} for other dimension "
                    "is missing. actual: {}".format(required_field, d)
                )
        identifier = "other dimension"
        self._set_type_from_dict(d, DimensionType.OTHER.value, identifier)
        self._set_description_from_dict(d, identifier)
        self._set_extent_from_dict(d, is_required=False, is_open=True, is_spatial=True, identifier=identifier)
        self._set_values_from_dict(d, types=(float, int, str), identifier=identifier)
        self._set_step_from_dict(d, types=(float, int, type(None)), identifier=identifier)
        self._set_reference_system_from_dict(d, types=(str), identifier=identifier)

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        if "type" not in d:
            raise ValueError("Could not parse CubeDimension object, type is a required field. actual: {}".format(d))
        tp = DimensionType(d.get("type"))
        dimension = cls(type=tp)
        if tp == DimensionType.SPATIAL:
            _axis = d.get("axis", UNSET)
            if _axis in ["x", "y"]:
                return dimension._set_horizontal_spatial_dimension_from_dict(d)
            elif _axis == "z":
                return dimension._set_vertical_spatial_dimension_from_dict(d)
            else:
                raise ValueError("Could not parse CubeDimension object, actual: {}".format(d))
        elif tp == DimensionType.TEMPORAL:
            return dimension._set_temporal_dimension_from_dict(d)
        elif tp == DimensionType.OTHER:
            return dimension._set_other_dimension_from_dict(d)
        else:
            return dimension._set_other_dimension_from_dict(d)

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
