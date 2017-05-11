import uuid

import pytest
from app import constants, factories
from app import models
from app.util.timestamps import utc_now
from freezegun import freeze_time


number = 1
text = 2
boolean = 3
date = 4


@pytest.fixture(
    params=[
        (constants.AnswerType.number, 5),
        (constants.AnswerType.text, 'hello, world!'),
        (constants.AnswerType.boolean, True,),
        (constants.AnswerType.date, utc_now().date())
    ],
    ids=[
        'number',
        'text',
        'boolean',
        'date'
    ]
)
def variant_answer(request):
    return {
        'answer_type': request.param[0],
        'value': str(request.param[1])
    }


def test_form_basic(session, simple_form):
    results = session.query(models.Form).all()
    assert len(results) == 1

    actual_schema = results[0]
    assert str(actual_schema) == 'Form(name={})'.format(simple_form.name)
    assert actual_schema.schema == simple_form.schema


def test_submission_timestamp(session, simple_form, user):
    now = utc_now()
    with freeze_time(now):
        submission = factories.SubmissionFactory(form=simple_form, user=user, responses={})
        session.add(submission)
        session.flush()

    results = session.query(models.Submission).all()
    assert len(results) == 1

    actual_submission = results[0]
    assert actual_submission.form == simple_form
    assert actual_submission.user == user
    assert actual_submission.date_created == now
    assert actual_submission.responses == {}


def test_submission_answer_type(session, simple_form, user, variant_answer):
    event = models.ResponseEvent(
        form_id=simple_form.id,
        form_name=simple_form.name,
        user_id=user.id,
        user_full_name=user.full_name,
        submission_id=uuid.uuid4(),
        submission_created=utc_now(),
        processed_on=utc_now(),
        schema_path='test.path',
        value=variant_answer['value'],
        answer_type=variant_answer['answer_type']
    )
    session.add(event)
    session.flush()

    serialized_event = session.query(models.ResponseEvent).get(event.id)
    assert serialized_event
    assert serialized_event.answer_type == variant_answer['answer_type']
    assert serialized_event.value == variant_answer['value']
