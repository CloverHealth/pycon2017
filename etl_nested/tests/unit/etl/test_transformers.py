import copy
import logging
import random
import types

import pytest
from app import models, factories
from app.etl import transformers
from app.util.timestamps import utc_now
from freezegun import freeze_time



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


def test_json_transform_to_model(session, raw_data, transformer, mock_logger):
    timestamp_submission = utc_now()
    with freeze_time(timestamp_submission):
        form = factories.FormFactory(schema=raw_data.schema)
        submission = factories.SubmissionFactory(form=form, responses=raw_data.responses)
        session.add_all([form, submission])
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
        assert actual_event.form_id == form.id
        assert actual_event.form_name == form.name

        assert actual_event.user_id == submission.user.id
        assert actual_event.user_full_name == submission.user.full_name

        assert actual_event.submission_id == submission.id
        assert actual_event.submission_created == timestamp_submission

        assert actual_event.processed_on == timestamp_transformation

    # convert results to a dictionary of only the data we care about
    sorted_actual_events = sorted(
        ({
            'schema_path': e.schema_path,
            'value': e.value,
            'answer_type': e.answer_type.name,
            'tag': None if not e.tag else e.tag
        } for e in results),
        key=lambda e: e['schema_path']
    )
    assert sorted_actual_events == raw_data.events

    _verify_logged_metrics(mock_logger, 1)


def test_json_transform_to_dict(session, raw_data, transformer):
    timestamp_submission = utc_now()
    with freeze_time(timestamp_submission):
        form = factories.FormFactory(schema=raw_data.schema)
        submission = factories.SubmissionFactory(form=form, responses=raw_data.responses)
        session.add_all([form, submission])
        session.flush()

    timestamp_transformation = utc_now()
    with freeze_time(timestamp_transformation):
        # TODO remove explicit processed_on to verify default
        results = transformer([submission], processed_on=timestamp_transformation, to_dict=True)

    assert results
    assert isinstance(results, types.GeneratorType)

    results = list(results)
    assert len(results) == len(raw_data.events)

    # verify all standard fields
    for actual_event in results:
        assert isinstance(actual_event, dict)
        assert actual_event['form_id'] == form.id
        assert actual_event['form_name'] == form.name

        assert actual_event['user_id'] == submission.user.id
        assert actual_event['user_full_name'] == submission.user.full_name

        assert actual_event['submission_id'] == submission.id
        assert actual_event['submission_created'] == timestamp_submission

        assert actual_event['processed_on'] == timestamp_transformation

    sorted_actual_events = sorted(
        ({
            'schema_path': e['schema_path'],
            'value': e['value'],
            'answer_type': e['answer_type'],
            'tag': None if not e['tag'] else e['tag']
        } for e in results),
        key=lambda e: e['schema_path']
    )
    assert sorted_actual_events == raw_data.events


@pytest.mark.parametrize('num_responses_with_same_schema', [
    0,
    1,
    2,
    5
])
def test_node_path_map_caching(monkeypatch, session, transformer, mock_logger,
                               simple_form, num_responses_with_same_schema):
    # create responses for the basic form schema
    submissions = factories.SubmissionFactory.build_batch(num_responses_with_same_schema,
                                                          form=simple_form, responses={})
    for submission in submissions:
        submission.responses = {
            'basic_info': {
                'bmi': random.randint(10, 40)
            }
        }
    session.add_all(submissions)

    # create an unrelated response to a different form (uses a string response instead)
    adjusted_schema = copy.deepcopy(simple_form.schema)
    adjusted_schema['children'][0]['children'][0]['answerType'] = 'text'
    adjusted_schema['children'][0]['children'][0]['tag'] = 'member_bmi_as_text'
    unrelated_form = factories.FormFactory.build(name='unrelated', schema=adjusted_schema)
    session.add(unrelated_form)
    unreleated_submission = factories.SubmissionFactory(form=unrelated_form, responses={
        'basic_info': {
            'bmi': 'my BMI is 40'
        }
    })
    session.add(unreleated_submission)

    # create a response with an unrelated form that will not yield a cache hit
    empty_form = factories.FormFactory.build(schema={})
    session.add(empty_form)
    empty_response = factories.SubmissionFactory.build(form=empty_form, responses={})
    session.add(empty_response)

    all_submissions = session.query(models.Submission)

    # patch the cache handler so that we have direct access to the internal LRU cache for test assertions
    original_cache_func = transformers.get_node_path_map_cache
    generated_cache = None
    def _patched_make_cache_wrapper(session):
        nonlocal generated_cache
        generated_cache = original_cache_func(session)
        return generated_cache

    monkeypatch.setattr(transformers, 'get_node_path_map_cache', _patched_make_cache_wrapper)

    results = list(transformer(all_submissions))

    # verify the logger recorded the correct number of responses processed
    _verify_logged_metrics(mock_logger, num_responses_with_same_schema + 2)

    # verify transformed events
    #
    # NOTES: only the unrelated form produces an additional event
    #        the empty schema should not produce an event since there were no actual responses
    assert results
    assert len(results) == 1 + num_responses_with_same_schema

    same_schema_events = {e.submission_id: e for e in results if e.form_id == simple_form.id}
    for expected_submission in submissions:
        actual_event = same_schema_events.get(expected_submission.id)
        assert actual_event
        assert actual_event.schema_path == 'basic_info.bmi'
        assert int(actual_event.value) == expected_submission.responses['basic_info']['bmi']
        assert actual_event.tag == 'member_bmi'

    other_schema_events = {e.submission_id: e for e in results if e.form_id != simple_form.id}
    assert unreleated_submission.id in other_schema_events
    newer_submission_event = other_schema_events.get(unreleated_submission.id)
    assert newer_submission_event
    assert newer_submission_event.schema_path == 'basic_info.bmi'
    assert newer_submission_event.value == 'my BMI is 40'
    assert newer_submission_event.tag == 'member_bmi_as_text'

    # verify that the cache was used as expected
    assert generated_cache
    expected_cache_hits = max(0, num_responses_with_same_schema - 1)
    cache_info = generated_cache.cache_info()
    assert cache_info.hits == expected_cache_hits
