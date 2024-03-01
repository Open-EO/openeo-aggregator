# This implementation is based on
# code generated with https://app.quicktype.io/
# using the spec of STAC extension "eo"
# https://github.com/stac-extensions/eo/blob/main/json-schema/schema.json

from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, cast

from openeo.util import dict_no_none

T = TypeVar("T")
EnumT = TypeVar("EnumT", bound=Enum)


def from_float(x: Any) -> float:
    assert isinstance(x, (float, int)) and not isinstance(x, bool)
    return float(x)


def from_none(x: Any) -> Any:
    assert x is None
    return x


def from_union(fs, x):
    for f in fs:
        try:
            return f(x)
        except:
            pass
    assert False


def from_str(x: Any) -> str:
    assert isinstance(x, str)
    return x


def to_float(x: Any) -> float:
    assert isinstance(x, float)
    return x


def to_enum(c: Type[EnumT], x: Any) -> EnumT:
    assert isinstance(x, c)
    return x.value


def from_list(f: Callable[[Any], T], x: Any) -> List[T]:
    assert isinstance(x, list)
    return [f(y) for y in x]


def to_class(c: Type[T], x: Any) -> dict:
    assert isinstance(x, c)
    return cast(Any, x).to_dict()


def from_dict(f: Callable[[Any], T], x: Any) -> Dict[str, T]:
    assert isinstance(x, dict)
    return {k: f(v) for (k, v) in x.items()}


class CommonNameOfTheBand(Enum):
    BLUE = "blue"
    CIRRUS = "cirrus"
    COASTAL = "coastal"
    GREEN = "green"
    LWIR = "lwir"
    LWIR11 = "lwir11"
    LWIR12 = "lwir12"
    NIR = "nir"
    NIR08 = "nir08"
    NIR09 = "nir09"
    PAN = "pan"
    RED = "red"
    REDEDGE = "rededge"
    SWIR16 = "swir16"
    SWIR22 = "swir22"
    YELLOW = "yellow"


@dataclass
class EoBand:
    center_wavelength: Optional[float] = None
    common_name: Optional[CommonNameOfTheBand] = None
    full_width_half_max: Optional[float] = None
    name: Optional[str] = None
    solar_illumination: Optional[float] = None

    @staticmethod
    def from_dict(obj: Any) -> "EoBand":
        assert isinstance(obj, dict)
        center_wavelength = from_union([from_float, from_none], obj.get("center_wavelength"))
        common_name = from_union([CommonNameOfTheBand, from_none], obj.get("common_name"))
        full_width_half_max = from_union([from_float, from_none], obj.get("full_width_half_max"))
        name = from_union([from_str, from_none], obj.get("name"))
        solar_illumination = from_union([from_float, from_none], obj.get("solar_illumination"))
        return EoBand(
            center_wavelength,
            common_name,
            full_width_half_max,
            name,
            solar_illumination,
        )

    def to_dict(self) -> dict:
        return dict_no_none(
            {
                "center_wavelength": from_union([to_float, from_none], self.center_wavelength),
                "common_name": from_union(
                    [lambda x: to_enum(CommonNameOfTheBand, x), from_none],
                    self.common_name,
                ),
                "full_width_half_max": from_union([to_float, from_none], self.full_width_half_max),
                "name": from_union([from_str, from_none], self.name),
                "solar_illumination": from_union([to_float, from_none], self.solar_illumination),
            }
        )
