import csv
import functools
import os
from collections import namedtuple

import pytest
import sqlalchemy.orm as sa_orm

from app import factories
from app.util.json import load_json_file
from app.etl import extractors, transformers, loaders
from tests import mocks


@pytest.fixture
def user(session):
    user = factories.UserFactory()
    session.add(user)
    session.flush()
    return user


@pytest.fixture(scope='session')
def data_dir(root_path):
    return os.path.join(root_path, 'src', 'tests', 'data')


RawData = namedtuple('RawData', ['schema', 'responses', 'events'])


def _load_raw_data(data_dir, fixture_name):
    def _testfile_path(type_name:str, ext:str='json'):
        return os.path.join(data_dir, '{}_{}.{}'.format(fixture_name, type_name, ext))

    schema_data = load_json_file(_testfile_path('schema'))
    responses_data = load_json_file(_testfile_path('responses'))
    with open(_testfile_path('events', ext='csv')) as f:
        events_data = sorted(csv.DictReader(f, fieldnames=['schema_path', 'value', 'answer_type']),
                             key=lambda e: e['schema_path'])

    return RawData(schema_data, responses_data, events_data)


@pytest.fixture(
    params=[
        'empty',
        'simple',
        'general',
        'nested'
    ]
)
def raw_data(request, data_dir: str):
    fixture_name = request.param
    return _load_raw_data(data_dir, fixture_name)


@pytest.fixture(scope='session')
def simple_form_schema(data_dir):
    raw_data = _load_raw_data(data_dir, 'simple')
    return raw_data.schema


@pytest.fixture()
def simple_form(session, simple_form_schema):
    form = factories.FormFactory(schema=simple_form_schema)
    session.add(form)
    session.flush()
    return form


@pytest.fixture(scope='session')
def simple_response_data(data_dir):
    raw_data = _load_raw_data(data_dir, 'simple')
    return raw_data.responses


@pytest.fixture(scope='function')
def mock_logger(monkeypatch):
    logger = mocks.MockLogger()
    monkeypatch.setattr(transformers, 'LOGGER', logger)
    monkeypatch.setattr(loaders, 'LOGGER', logger)
    return logger


ExtractorParams = namedtuple('ExtractorParams', ['func', 'kwargs'])
LoaderParams = namedtuple('LoaderParams', ['func', 'kwargs', 'check_logs'])


@pytest.fixture(
    params=[
        LoaderParams(loaders.naive_loader, {}, True),
        LoaderParams(loaders.naive_add_all_loader, {}, False),
        # NOTE: 'return_defaults' must be set to True for regression tests, but not for normal execution
        LoaderParams(loaders.chunked_bulk_save_objects_loader, {'return_defaults': True, 'chunk_size': 2}, True),
        LoaderParams(loaders.chunked_bulk_save_objects_loader, {'return_defaults': True, 'chunk_size': 10}, True)
    ],
    ids=[
        'naive_iterator',
        'naive_insert_all',
        'chunked_bulk_save_objects_2',
        'chunked_bulk_save_objects_10'
    ]
)
def loader_params(request):
    return request.param


@pytest.fixture()
def loader(session: sa_orm.Session, loader_params):
    return functools.partial(loader_params.func, session, **loader_params.kwargs)


@pytest.fixture(
    params=[
        ExtractorParams(extractors.naive_extractor, {}),
        ExtractorParams(extractors.naive_load_all_extractor, {}),
        ExtractorParams(extractors.chunked_extractor, {'chunk_size': 2}),
        ExtractorParams(extractors.chunked_extractor, {'chunk_size': 10}),
    ],
    ids=[
        'naive_iterator',
        'naive_load_all',
        'chunked_2',
        'chunked_10'
    ]
)
def extractor(request, session: sa_orm.Session):
    params = request.param
    return functools.partial(params.func, session, **params.kwargs)


@pytest.fixture()
def transformer(session):
    return functools.partial(transformers.transform_submissions, session)


@pytest.fixture(
    params=[
        factories.SourceDataMetrics(forms=0, users=0, submissions=0),
        factories.SourceDataMetrics(forms=1, users=1, submissions=1),
        factories.SourceDataMetrics(forms=2, users=1, submissions=2),
        factories.SourceDataMetrics(forms=1, users=1, submissions=2),
        factories.SourceDataMetrics(forms=1, users=2, submissions=10),
        factories.SourceDataMetrics(forms=4, users=10, submissions=100),
    ],
    ids=[
        'empty_database',
        'single_submission',
        'two_submissions_different_forms',
        'two_submissions_same_form',
        'complex_setup_1',
        'complex_setup_2'
    ]
)
def source_data(request, session: sa_orm.Session, simple_form_schema: dict):
    source_data_metrics = request.param
    factories.make_source_data(session, source_data_metrics, [simple_form_schema])
    return source_data_metrics
