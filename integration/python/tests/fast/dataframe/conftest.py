import pytest

import otterbrix


@pytest.fixture(scope="session")
def conn():
    """A single otterbrix connection for the conn-API DataFrame tests."""
    connection = otterbrix.connect("default")
    yield connection
    connection.close()
