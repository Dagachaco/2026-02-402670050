import pytest
from session import get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark(master="local[1]")
    yield s
    s.stop()