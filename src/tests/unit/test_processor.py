import functools
import os

import pytest
import sqlalchemy.orm as sa_orm

from app import constants, models, processor, factories
from app.etl import transformers, loaders
from app.util.json import load_json_file


@pytest.fixture(scope='module')
def all_processor_configs(conf_path):
    processors_conf_path = os.path.join(conf_path, constants.PROCESSOR_CONFIG_FILE)
    return load_json_file(processors_conf_path)


# TODO extend to retrieve keys dynamically from all_processor_configs fixture
@pytest.fixture(
    scope='module',
    params=[
        'naive-single',
        'naive-all',
        'chunked-objects-small',
        'chunked-objects-no-join',
        'chunked-objects-with-join',
        'chunked-mappings-with-join'
    ]
)
def processor_config_name(request):
    return request.param


@pytest.fixture(scope='module')
def processor_config(all_processor_configs, processor_config_name):
    return all_processor_configs[processor_config_name]


@pytest.mark.usefixtures('mock_logger')
def test_processor_with_models(session: sa_orm.Session, extractor, transformer, loader, source_data):
    processor.process(extractor, transformer, loader)
    session.flush()

    # one event per submission
    expected_num_events = source_data.submissions
    actual_num_events = session.query(models.ResponseEvent).count()

    assert actual_num_events == expected_num_events


@pytest.mark.usefixtures('mock_logger')
def test_process_with_mappings(session: sa_orm.Session, extractor, source_data):
    transformer = functools.partial(transformers.transform_submissions, session, to_dict=True)
    loader = functools.partial(loaders.chunked_bulk_insert_mappings, session, chunk_size=10)

    processor.process(extractor, transformer, loader)
    session.flush()

    # one event per submission
    expected_num_events = source_data.submissions
    actual_num_events = session.query(models.ResponseEvent).count()

    assert actual_num_events == expected_num_events


@pytest.mark.usefixtures('mock_logger')
def test_make_processor(session: sa_orm.Session, processor_config,
                        simple_form, simple_response_data):
    submission = factories.SubmissionFactory(form=simple_form, responses=simple_response_data)
    session.add(submission)
    session.flush()

    test_processor = factories.make_processor(session, processor_config)
    assert test_processor

    test_processor()
    session.flush()

    actual_num_events = session.query(models.ResponseEvent).count()
    assert actual_num_events == 1
