import os

import pytest

from app import constants


def pytest_addoption(parser):
    parser.addoption('--keepdb', action="store_true", default=False, help="Reuse test database")


@pytest.fixture(scope='session')
def root_path():
    return os.getcwd()


@pytest.fixture(scope='session')
def conf_path(root_path):
    return os.path.join(root_path, constants.DEFAULT_CONFIG_DIR)
