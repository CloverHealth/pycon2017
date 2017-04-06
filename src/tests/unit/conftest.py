from collections import namedtuple, defaultdict
import csv
import functools
import json
import os

import pytest
import sqlalchemy.orm as sa_orm

from app.etl import extractors, transformers, loaders
from app import models
from tests import factories, mocks


@pytest.fixture
def form_type(session):
    form_type = factories.FormTypeFactory()
    session.add(form_type)
    session.flush()
    return form_type


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

    with open(_testfile_path('schema')) as f:
        schema_data = json.load(f)
    with open(_testfile_path('responses')) as f:
        responses_data = json.load(f)
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
def simple_form_schema_data(data_dir):
    raw_data = _load_raw_data(data_dir, 'simple')
    return raw_data.schema


@pytest.fixture()
def simple_form_schema(session, data_dir, form_type, simple_form_schema_data):
    schema = factories.FormSchemaFactory(form_type=form_type, data=simple_form_schema_data)
    session.add(schema)
    session.flush()

    return schema


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
        LoaderParams(loaders.naive_all_loader, {}, False),
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


SubmissionMetrics = namedtuple('SubmissionMetrics', ['form_types', 'schemas', 'users', 'submissions'])


@pytest.fixture(
    params=[
        SubmissionMetrics(form_types=0, schemas=0, users=0, submissions=0),
        SubmissionMetrics(form_types=1, schemas=1, users=1, submissions=1),
        SubmissionMetrics(form_types=2, schemas=2, users=1, submissions=2),
        SubmissionMetrics(form_types=1, schemas=1, users=1, submissions=2),
        SubmissionMetrics(form_types=1, schemas=2, users=1, submissions=2),
        SubmissionMetrics(form_types=1, schemas=1, users=2, submissions=10),
        SubmissionMetrics(form_types=2, schemas=4, users=10, submissions=100),
    ],
    ids=[
        'empty_database',
        'single_submission',
        'two_submissions_different_schemas',
        'two_submissions_same_schema',
        'two_submissions_different_versions',
        'complex_setup_1',
        'complex_setup_2'
    ]
)
def submission_metrics(request, session: sa_orm.Session, simple_form_schema_data,
                       simple_response_data) -> SubmissionMetrics:
    metrics = request.param

    session.add_all(
        factories.FormTypeFactory.build_batch(metrics.form_types)
    )
    session.add_all(
        factories.UserFactory.build_batch(metrics.users)
    )
    session.flush()

    form_type_iterator = factories.cyclic_model_iterator(session, models.FormType)
    latest_form_type_versions = defaultdict(lambda: 1)
    for _ in range(metrics.schemas):
        form_type = next(form_type_iterator)
        curr_version = latest_form_type_versions[form_type.id]

        session.add(factories.FormSchemaFactory(form_type=form_type,
                                                version=curr_version,
                                                data=simple_form_schema_data))
        latest_form_type_versions[form_type.id] = curr_version + 1

    session.flush()

    form_schema_iterator = factories.cyclic_model_iterator(session, models.FormSchema)
    user_iterator = factories.cyclic_model_iterator(session, models.User)

    session.add_all(
        factories.SubmissionFactory(
            schema=next(form_schema_iterator),
            user=next(user_iterator),
            responses=simple_response_data
        ) for _ in range(metrics.submissions)
    )
    session.flush()

    return metrics
