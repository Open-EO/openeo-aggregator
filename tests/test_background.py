from openeo_aggregator.background import AttrStatsProxy


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
