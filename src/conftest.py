import pytest
import os

def pytest_addoption(parser):
    parser.addoption('--keepdb', action="store_true", default=False, help="Reuse test database")


@pytest.fixture(scope='session')
def root_path():
    return os.getcwd()
