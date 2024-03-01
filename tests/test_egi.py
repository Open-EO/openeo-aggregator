import pytest

from openeo_aggregator.egi import (
    OPENEO_PLATFORM_USER_ROLES,
    Entitlement,
    UserRole,
    parse_eduperson_entitlement,
)


def test_parse_eduperson_entitlement():
    assert parse_eduperson_entitlement("urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu") == Entitlement(
        namespace="urn:mace:egi.eu", vo="vo.openeo.cloud", group=None, role=None, authority="aai.egi.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu", vo="vo.openeo.cloud", group=None, role="early_adopter", authority="aai.egi.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:vo.openeo.cloud:vo.openeo-sub.cloud:role=early_adopter#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu",
        vo="vo.openeo.cloud",
        group="vo.openeo-sub.cloud",
        role="early_adopter",
        authority="aai.egi.eu",
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi-dev.eu:group:vo.openeo-dev.cloud:vo.openeo-sub.cloud:role=Early-Adop.ter#aai.egi-dev.eu"
    ) == Entitlement(
        namespace="urn:mace:egi-dev.eu",
        vo="vo.openeo-dev.cloud",
        group="vo.openeo-sub.cloud",
        role="Early-Adop.ter",
        authority="aai.egi-dev.eu",
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:openEO_test:education_package.openEO_test:admins:role=member#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu",
        vo="openEO_test",
        group="education_package.openEO_test:admins",
        role="member",
        authority="aai.egi.eu",
    )


def test_parse_eduperson_entitlement_strict():
    with pytest.raises(ValueError, match="Failed to parse"):
        parse_eduperson_entitlement("foobar")


def test_parse_eduperson_entitlement_loose():
    e = parse_eduperson_entitlement("foobar", strict=False)
    assert e == Entitlement(None, None, None, None, None)


class TestUserRole:
    def test_basic(self):
        role = UserRole("Foo")
        assert role.id == "Foo"
        assert role.entitlement_match("urn:mace:egi.eu:group:vo.openeo.cloud:role=Foo#aai.egi.eu")

    @pytest.mark.parametrize("title", ["Foo-Bar", "Foo_Bar", "FooBar", "Foo Bar", "foo bar", "foo_bar"])
    @pytest.mark.parametrize(
        "entitlement_role",
        ["Foo-Bar", "FooBar", "Foo_Bar", "foobar", "foo-bar", "foo_bar"],
    )
    def test_normalization(self, title, entitlement_role):
        role = UserRole(title)
        assert role.id == "FooBar"
        assert role.entitlement_match(f"urn:mace:egi.eu:group:vo.openeo.cloud:role={entitlement_role}#aai.egi.eu")

    def test_is_early_adopter(self):
        (role,) = [r for r in OPENEO_PLATFORM_USER_ROLES.roles if r.id == "EarlyAdopter"]
        is_early_adopter = role.entitlement_match

        assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu")
        assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=Early_Adopter#aai.egi.eu")
        assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=Early-Adopter#aai.egi.eu")
        assert is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=EarlyAdopter#aai.egi.eu")

        assert not is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu")
        assert not is_early_adopter("urn:mace:uho.ai:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu")
        assert not is_early_adopter("urn:mace:egi.eu:group:vo.kleurenwiezen.be:role=early_adopter#aai.egi.eu")
        assert not is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=member#aai.egi.eu")
        assert not is_early_adopter("urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#ooi.egi.eu")

        assert not is_early_adopter("foobar")
        assert not is_early_adopter("")

    def test_is_30day_trial(self):
        (role,) = [r for r in OPENEO_PLATFORM_USER_ROLES.roles if r.id == "30DayTrial"]
        is_30day_trial = role.entitlement_match

        assert is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30day_trial#aai.egi.eu")
        assert is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30Day_Trial#aai.egi.eu")
        assert is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30Day-Trial#aai.egi.eu")
        assert is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30-Day-Trial#aai.egi.eu")
        assert is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30DayTrial#aai.egi.eu")

        assert not is_30day_trial("urn:mace:uho.ai:group:vo.openeo.cloud:role=30day_trial#aai.egi.eu")
        assert not is_30day_trial("urn:mace:egi.eu:group:vo.kleurenwiezen.be:role=30day_trial#aai.egi.eu")
        assert not is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30day_trial#ooi.egi.eu")
        assert not is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu")
        assert not is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=member#aai.egi.eu")
        assert not is_30day_trial("urn:mace:egi.eu:group:vo.openeo.cloud:role=30day_trial#ooi.egi.eu")

        assert not is_30day_trial("foobar")
        assert not is_30day_trial("")
