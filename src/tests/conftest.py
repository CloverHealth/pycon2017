from collections import namedtuple
import os

import pytest
import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
import sqlalchemy.orm as sa_orm
import testing.postgresql

from app import models
from tests import constants

# Test database options
DatabaseConfig = namedtuple(
    'DatabaseConfig',
    ['keepdb_active', 'keepdb_path']
)


@pytest.fixture(scope='session')
def db_options(request) -> DatabaseConfig:
    """
    Fixture of test database options for the entire pytest session
    
    :param request: pytest fixture request (FixtureRequest)
    """
    keepdb_active = request.config.getoption('--keepdb')

    if keepdb_active:
        keepdb_path = os.path.join(os.getcwd(), constants.KEEPDB_PATH)
    else:
        keepdb_path = None

    return DatabaseConfig(keepdb_active, keepdb_path)


@pytest.fixture(scope='session')
def db_url(db_options: DatabaseConfig):
    """
    Postgres conninfo URL for the test database.
    
    This URL is usually a transient database managed by the 'testing.postgres' library.
    If the '--keepdb' option is specified, it will force it to be persistent at a known local path.
    
    :param db_options: test database options
    """
    testdb_kwargs = {}
    if db_options.keepdb_path:
        testdb_kwargs['base_dir'] = db_options.keepdb_path

    with testing.postgresql.Postgresql(**testdb_kwargs) as postgresql:
        yield postgresql.url()


@pytest.fixture(scope='session')
def db_engine(db_options:DatabaseConfig, db_url: str):
    """
    Fixture providing SQLAlchemy test database connectivity 
    
    :param db_options: database options 
    :param db_url: test database conninfo URL
    """
    db_engine = sa.create_engine(db_url)

    # speed up tests by only installing schema if there was no prior database created with --keepdb
    if not db_options.keepdb_active or os.path.exists(db_options.keepdb_path):
        models.init_database(db_engine)

    yield db_engine

    db_engine.dispose()


@pytest.fixture
def session(db_engine: sa_engine.Engine):
    """
    Fixture providing SQLAlchemy session for operations on ORM-mapped objects
    
    :param db_engine: test database connectivity instance 
    """
    sessionmaker = sa.orm.sessionmaker(db_engine)

    # session is automatically rolled back regardless of test result
    # if an uncaught exception occurred, ensure it is still propagated to pytest with the original traceback
    session = None
    try:
        session = sessionmaker()
        yield session
    except:
        raise
    finally:
        session.rollback()
