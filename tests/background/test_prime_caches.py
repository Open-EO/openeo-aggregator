from openeo_aggregator.background.prime_caches import AttrStatsProxy, prime_caches


class TestAttrStatsProxy:
    def test_basic(self):
        class Foo:
            def bar(self, x):
                return x + 1

            def meh(self, x):
                return x * 2

        foo = AttrStatsProxy(target=Foo(), to_track=["bar"])

        assert foo.bar(3) == 4
        assert foo.meh(6) == 12

        assert foo.stats == {"bar": 1}


def test_prime_caches_basic(config, backend1, backend2, requests_mock, mbldr, caplog):
    """Just check that bare basics of `prime_caches` work."""
    # TODO: check that (zookeeper) caches are actually updated/written.
    just_geotiff = {
        "input": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
        "output": {"GTiff": {"gis_data_types": ["raster"], "parameters": {}, "title": "GeoTiff"}},
    }
    mocks = [
        requests_mock.get(backend1 + "/file_formats", json=just_geotiff),
        requests_mock.get(backend2 + "/file_formats", json=just_geotiff),
        requests_mock.get(backend1 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend1 + "/collections/S2", json=mbldr.collection("S2")),
        requests_mock.get(backend2 + "/collections", json=mbldr.collections("S2")),
        requests_mock.get(backend2 + "/collections/S2", json=mbldr.collection("S2")),
    ]

    prime_caches(config=config)

    assert all([m.call_count == 1 for m in mocks])
