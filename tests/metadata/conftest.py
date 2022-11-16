import pytest


class ListReporter:
    """Reporter for testing"""

    def __init__(self):
        self.logs = []

    def __call__(self, msg, **kwargs):
        self.logs.append({"msg": msg, **kwargs})


@pytest.fixture
def reporter() -> ListReporter:
    return ListReporter()
