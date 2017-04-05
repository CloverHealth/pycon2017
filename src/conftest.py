
def pytest_addoption(parser):
    parser.addoption('--keepdb', action="store_true", default=False, help="Reuse test database")
