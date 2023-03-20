"""
Functionality and definitions related to
openEO Platform and its EGI Virtual Organisation
"""

import functools
import re
from collections import namedtuple
from typing import List, Union

BillingPlan = namedtuple("BillingPlan", ["name", "description", "url", "paid"])


_BILLING_PLAN_30DAY_TRIAL = BillingPlan(
    name="30day-trial",
    description="openEO.cloud 30 day free trial plan (experimental)",
    url="https://docs.openeo.cloud/join/free_trial.html",
    paid=False,
)

_BILLING_PLAN_EARLY_ADOPTER = BillingPlan(
    name="early-adopter",
    description="openEO.cloud early adopter plan",
    url="https://openeo.cloud/early-adopters/",
    paid=True,
)

# TODO: avoid using generic billing plan
_BILLING_PLAN_GENERIC = BillingPlan(
    name="generic",
    description="openEO.cloud generic plan",
    url=None,
    paid=True,
)

OPENEO_PLATFORM_BILLING_PLANS = [
    _BILLING_PLAN_30DAY_TRIAL,
    _BILLING_PLAN_EARLY_ADOPTER,
    _BILLING_PLAN_GENERIC,
]


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

Entitlement = namedtuple(
    "Entitlement", ["namespace", "vo", "group", "role", "authority"]
)


@functools.lru_cache(maxsize=100)
def parse_eduperson_entitlement(entitlement: str, strict=True) -> Entitlement:
    """Parse eduperson_entitlement string to named tuple object"""
    m = _eduperson_entitlement_regex.match(entitlement)
    if not m:
        if strict:
            raise ValueError(f"Failed to parse eduperson_entitlement {entitlement}")
        else:
            return Entitlement(None, None, None, None, None)
    return Entitlement(*m.group("namespace", "vo", "group", "role", "authority"))


class UserRole:
    __slots__ = [
        # Name that should be used when assigning a role to a user in the EGI VO admin tool
        "_title",
        # CamelCase version of role name (exposed to user)
        "_id",
        # Normalized version of role name (for case/whitespace-insensitive comparison)
        "_normalized",
        # Associated billing plan
        "_billing_plan",
    ]

    def __init__(self, title: str, billing_plan: BillingPlan = _BILLING_PLAN_GENERIC):
        self._title = title
        self._id = "".join(
            w.title() if w.islower() else w
            for w in self._title.replace("-", " ").replace("_", " ").split()
        )
        self._normalized = self.normalize_role(self._title)
        self._billing_plan = billing_plan


    @property
    def id(self) -> str:
        return self._id

    @property
    def billing_plan(self) -> BillingPlan:
        return self._billing_plan

    @staticmethod
    def normalize_role(role: Union[str, None]) -> Union[str, None]:
        if role:
            return role.lower().replace("-", "").replace("_", "").replace(" ", "")

    def entitlement_match(self, entitlement: str):
        """Check if given eduperson_entitlement corresponds to this role."""
        e = parse_eduperson_entitlement(entitlement, strict=False)
        return (
            e.namespace in {"urn:mace:egi.eu", "urn:mace:egi-dev.eu"}
            and e.vo in {"vo.openeo.cloud"}
            and self.normalize_role(e.role) == self._normalized
            and e.authority in {"aai.egi.eu", "aai.egi-dev.eu"}
        )




class OpeneoPlatformUserRoles:
    def __init__(self, roles: List[UserRole]):
        self.roles = roles

    def extract_roles(self, entitlements: List[str]) -> List[UserRole]:
        """Extract user roles based on list of eduperson_entitlement values"""
        return [
            role
            for role in self.roles
            if any(role.entitlement_match(e) for e in entitlements)
        ]


# Standardized roles in openEO Platform EGI Virtual Organisation
# Based on https://github.com/openEOPlatform/documentation/issues/48
OPENEO_PLATFORM_USER_ROLES = OpeneoPlatformUserRoles(
    [
        UserRole("30-Day-Trial", billing_plan=_BILLING_PLAN_30DAY_TRIAL),
        UserRole("Early_Adopter", billing_plan=_BILLING_PLAN_EARLY_ADOPTER),
        # TODO: define a dedicated billing plan for each user role?
        UserRole("Basic_User"),
        UserRole("Professional_User"),
        UserRole("University_Student"),
        UserRole("University_Group_License"),
        UserRole("Platform_Developer"),
    ]
)
