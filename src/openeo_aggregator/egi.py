"""
EGI Check-in and Virtual Organisation related functionality
"""

from collections import namedtuple

import re

# Regex to parse eduperson_entitlement strings,
# like for example "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu"
_eduperson_entitlement_regex = re.compile(
    r"""^
        (?P<namespace>[a-z0-9:._-]+)
        :group:
        (?P<vo>[a-z0-9._-]+)
        (:(?P<group>[a-z0-9:._-]+))?
        (:role=(?P<role>[a-z0-9._-]+))?
        \#(?P<authority>[a-z0-9._-]+)
        $
    """,
    flags=re.VERBOSE | re.IGNORECASE
)

Entitlement = namedtuple("Entitlement", ["namespace", "vo", "group", "role", "authority"])


def parse_eduperson_entitlement(entitlement: str, strict=True) -> Entitlement:
    """Parse eduperson_entitlement string to named tuple object"""
    m = _eduperson_entitlement_regex.match(entitlement)
    if not m:
        if strict:
            raise ValueError(f"Failed to parse eduperson_entitlement {entitlement}")
        else:
            return Entitlement(None, None, None, None, None)
    return Entitlement(*m.group("namespace", "vo", "group", "role", "authority"))


def is_early_adopter(entitlement: str) -> bool:
    """Check if given eduperson_entitlement corresponds to an early adopter role."""
    e = parse_eduperson_entitlement(entitlement, strict=False)
    return (
            e.namespace in {"urn:mace:egi.eu", "urn:mace:egi-dev.eu"}
            and e.vo in {"vo.openeo.cloud"}
            and e.role and e.role.lower() in {"early_adopter", "early-adopter", "earlyadopter"}
            and e.authority in {"aai.egi.eu", "aai.egi-dev.eu"}
    )


def is_free_tier(entitlement: str) -> bool:
    e = parse_eduperson_entitlement(entitlement, strict=False)
    return (
            e.namespace in {"urn:mace:egi.eu", "urn:mace:egi-dev.eu"}
            and e.vo in {"vo.openeo.cloud"}
            # TODO: no role check yet?
            and e.authority in {"aai.egi.eu", "aai.egi-dev.eu"}
    )
