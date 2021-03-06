import pytest

from openeo_aggregator.egi import parse_eduperson_entitlement, Entitlement, is_early_adopter, is_free_tier


def test_parse_eduperson_entitlement():
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu"
    ) == Entitlement(
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
        namespace="urn:mace:egi.eu", vo="vo.openeo.cloud", group="vo.openeo-sub.cloud", role="early_adopter",
        authority="aai.egi.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi-dev.eu:group:vo.openeo-dev.cloud:vo.openeo-sub.cloud:role=Early-Adop.ter#aai.egi-dev.eu"
    ) == Entitlement(
        namespace="urn:mace:egi-dev.eu", vo="vo.openeo-dev.cloud", group="vo.openeo-sub.cloud", role="Early-Adop.ter",
        authority="aai.egi-dev.eu"
    )
    assert parse_eduperson_entitlement(
        "urn:mace:egi.eu:group:openEO_test:education_package.openEO_test:admins:role=member#aai.egi.eu"
    ) == Entitlement(
        namespace="urn:mace:egi.eu", vo="openEO_test", group="education_package.openEO_test:admins", role="member",
        authority="aai.egi.eu"
    )


def test_parse_eduperson_entitlement_strict():
    with pytest.raises(ValueError, match="Failed to parse"):
        parse_eduperson_entitlement("foobar")


def test_parse_eduperson_entitlement_loose():
    e = parse_eduperson_entitlement("foobar", strict=False)
    assert e == Entitlement(None, None, None, None, None)


def test_is_early_adopter():
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


def test_is_free_tier():
    assert is_free_tier("urn:mace:egi.eu:group:vo.openeo.cloud:role=early_adopter#aai.egi.eu")
    assert is_free_tier("urn:mace:egi.eu:group:vo.openeo.cloud:role=free_tier#aai.egi.eu")
    assert is_free_tier("urn:mace:egi.eu:group:vo.openeo.cloud:role=free-tier#aai.egi.eu")
    assert is_free_tier("urn:mace:egi.eu:group:vo.openeo.cloud#aai.egi.eu")

    assert not is_free_tier("urn:mace:uho.ai:group:vo.openeo.cloud:role=free_tier#aai.egi.eu")
    assert not is_free_tier("urn:mace:egi.eu:group:vo.kleurenwiezen.be:role=free_tier#aai.egi.eu")
    assert not is_free_tier("urn:mace:egi.eu:group:vo.openeo.cloud:role=free_tier#ooi.egi.eu")

    assert not is_free_tier("foobar")
    assert not is_free_tier("")
