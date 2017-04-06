import copy
import logging
import random
import types

from freezegun import freeze_time
import pytest

from app import models
from app.etl import transformers
from app.timestamp_utils import utc_now
from tests import factories


SAMPLE_OBJECTS = [
    {},
    {'foo': 'bar'},
    {
        'foo': 'bar',
        'hello': 'world',
        'clover': 'health'
    },
    {
        'foo': 'bar',
        'child': {
            'hello': 'world'
        }
    },
    {
        'a': {
            'foo': 'bar'
        },
        'b': {
            'foo': 'bar',
            'deep': {
                'hello': 'world'
            }
        }
    }
]

# pre-order by path components
EXPECTED_PATHS = [
    [],
    [['foo']],
    [
        ['clover'],
        ['foo'],
        ['hello']
    ],
    [
        ['child', 'hello'],
        ['foo']
    ],
    [
        ['a', 'foo'],
        ['b', 'deep', 'hello'],
        ['b', 'foo']
    ]
]

# pre-order by path keys
EXPECTED_VALUES = [
    [],
    ['bar'],
    ['health', 'bar', 'world'],
    ['world', 'bar'],
    ['bar', 'world', 'bar']
]


@pytest.fixture(
    params=zip(SAMPLE_OBJECTS, EXPECTED_PATHS, EXPECTED_VALUES),
    ids=[
        'empty',
        'single_pair',
        'multi_pair',
        'simple_nested',
        'multi_nested'
    ]
)
def nested_data(request):
    return request.param


def test_map_nested(nested_data:tuple):
    def _gen_values(node:dict, path:list):
        for k, v in node.items():
            if not isinstance(v, dict):
                yield (path+[k], v, )

    def _gen_children(node: dict, path:list):
        for k, v in node.items():
            if isinstance(v, dict):
                yield path + [k], v

    obj = nested_data[0]
    actual_results = transformers.map_nested(obj, _gen_values, _gen_children)
    actual_results = sorted(actual_results, key=lambda r: r[0])

    expected_paths = nested_data[1]
    expected_values = nested_data[2]
    assert actual_results == list(zip(expected_paths, expected_values))


def _verify_logged_metrics(mock_logger, expected_submissions_processed):
    assert len(mock_logger.messages) == 1
    summary_record = mock_logger.messages[0]
    assert summary_record.level == logging.INFO
    assert summary_record.msg == 'Transformed %d JSON submissions'
    assert summary_record.args
    assert summary_record.args[0] == expected_submissions_processed


def test_json_transform(session, raw_data, transformer, mock_logger):
    timestamp_submission = utc_now()
    with freeze_time(timestamp_submission):
        schema = factories.FormSchemaFactory(data=raw_data.schema)
        submission = factories.SubmissionFactory(schema=schema, responses=raw_data.responses)
        session.add_all([schema, submission])
        session.flush()

    timestamp_transformation = utc_now()
    with freeze_time(timestamp_transformation):
        # TODO remove explicit processed_on to verify default
        results = transformer([submission], processed_on=timestamp_transformation)

    assert results
    assert isinstance(results, types.GeneratorType)

    results = list(results)
    assert len(results) == len(raw_data.events)

    # verify all standard fields
    for actual_event in results:
        assert isinstance(actual_event, models.ResponseEvent)
        assert actual_event.form_type_id == schema.form_type.id
        assert actual_event.form_type_name == schema.form_type.name
        assert actual_event.schema_id == schema.id
        assert actual_event.schema_version == schema.version

        assert actual_event.user_id == submission.user.id
        assert actual_event.user_full_name == submission.user.full_name

        assert actual_event.submission_id == submission.id
        assert actual_event.submission_created == timestamp_submission

        assert actual_event.processed_on == timestamp_transformation

    # convert results to a dictionary of only the data we care about
    sorted_actual_events = sorted(
        ({'schema_path': e.schema_path, 'value': e.value, 'answer_type': e.answer_type.name} for e in results),
        key=lambda e: e['schema_path']
    )
    assert sorted_actual_events == raw_data.events

    _verify_logged_metrics(mock_logger, 1)


@pytest.mark.parametrize('num_responses_with_same_schema', [
    0,
    1,
    2,
    5
])
def test_node_path_map_caching(monkeypatch, session, transformer, mock_logger,
                               simple_form_schema, num_responses_with_same_schema):
    # create responses for the basic form schema
    submissions = factories.SubmissionFactory.build_batch(num_responses_with_same_schema,
                                                          schema=simple_form_schema, responses={})
    for submission in submissions:
        submission.responses = {
            'basic_info': {
                'bmi': random.randint(10, 40)
            }
        }
    session.add_all(submissions)

    # create some responses with a newer schema version that will not yield a cache hit
    adjusted_schema_data = copy.deepcopy(simple_form_schema.data)
    adjusted_schema_data['children'][0]['children'][0]['answerType'] = 'text'
    newer_form_schema = factories.FormSchemaFactory.build(
        form_type=simple_form_schema.form_type,
        version=simple_form_schema.version + 1,
        data=simple_form_schema.data
    )
    session.add(newer_form_schema)
    newer_submission = factories.SubmissionFactory.build(
        schema=newer_form_schema,
        responses={
            'basic_info': {
                'bmi': 'my BMI is 40'
            }
        }
    )
    session.add(newer_submission)

    # create a response with an unrelated form type that will not yield a cache hit
    empty_schema = factories.FormSchemaFactory.build(data={})
    session.add(empty_schema)
    empty_response = factories.SubmissionFactory.build(schema=empty_schema, responses={})
    session.add(empty_response)

    all_submissions = session.query(models.Submission)

    original_cache_func = transformers.get_node_path_map_cache
    generated_cache = None

    def _patched_make_cache_wrapper(session):
        nonlocal generated_cache
        generated_cache = original_cache_func(session)
        return generated_cache

    monkeypatch.setattr(transformers, 'get_node_path_map_cache', _patched_make_cache_wrapper)

    results = list(transformer(all_submissions))

    assert results
    # NOTE: empty schema should not produce an event
    assert len(results) == 1 + num_responses_with_same_schema

    # verify the events were correctly processed
    same_schema_events = {e.submission_id: e for e in results if e.schema_id == simple_form_schema.id}
    for expected_submission in submissions:
        actual_event = same_schema_events.get(expected_submission.id)
        assert actual_event
        assert actual_event.schema_path == 'basic_info.bmi'
        assert int(actual_event.value) == expected_submission.responses['basic_info']['bmi']

    other_schema_events = {e.submission_id: e for e in results if e.schema_id != simple_form_schema.id}
    assert newer_submission.id in other_schema_events
    newer_submission_event = other_schema_events.get(newer_submission.id)
    assert newer_submission_event
    assert newer_submission_event.schema_path == 'basic_info.bmi'
    assert newer_submission_event.value == 'my BMI is 40'

    # verify that the cache was used as expected
    assert generated_cache
    expected_cache_hits = max(0, num_responses_with_same_schema - 1)
    cache_info = generated_cache.cache_info()
    assert cache_info.hits == expected_cache_hits

    _verify_logged_metrics(mock_logger, num_responses_with_same_schema + 2)
