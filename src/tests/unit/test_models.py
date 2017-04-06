import uuid

from freezegun import freeze_time
import pytest
import sqlalchemy.exc as sa_exceptions

from app import constants


from app import models
from app.timestamp_utils import utc_now
from tests import factories

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


def test_form_type_basic(form_type):
    assert str(form_type) == 'FormType(name={})'.format(form_type.name)


def test_schema_basic(session, form_type, simple_form_schema):
    results = session.query(models.FormSchema).all()
    assert len(results) == 1

    actual_schema = results[0]
    assert str(actual_schema) == \
           'FormSchema(type_name={}, version={})'.format(form_type.name, simple_form_schema.version)
    assert actual_schema.data == simple_form_schema.data
    assert actual_schema.version == 1
    assert actual_schema.form_type == form_type


def test_schema_unique_versions(session, form_type):
    duplicate_versions = [
        factories.FormSchemaFactory(form_type=form_type, version=1, data={}) for _ in range(2)
    ]

    session.add_all(duplicate_versions)
    with pytest.raises(sa_exceptions.IntegrityError) as excinfo:
        session.flush()

    ex = excinfo.value
    assert 'violates unique constraint' in str(ex)


@pytest.mark.parametrize('invalid_version', [0, -1, -2, -10])
def test_schema_version_constraint(session, form_type, invalid_version):
    session.add(
        factories.FormSchemaFactory(form_type=form_type, version=invalid_version, data={})
    )

    with pytest.raises(sa_exceptions.IntegrityError) as excinfo:
        session.flush()

    ex = excinfo.value
    assert 'violates check constraint' in str(ex)


def test_submission_timestamp(session, form_type, user):
    schema = factories.FormSchemaFactory(form_type=form_type)
    session.add(schema)
    session.flush()

    now = utc_now()
    with freeze_time(now):
        submission = factories.SubmissionFactory(schema=schema, user=user, responses={})
        session.add(submission)
        session.flush()

    results = session.query(models.Submission).all()
    assert len(results) == 1

    actual_submission = results[0]
    assert actual_submission.schema == schema
    assert actual_submission.user == user
    assert actual_submission.date_created == now
    assert actual_submission.responses == {}


def test_submission_answer_type(session, simple_form_schema, user, variant_answer):
    event = models.ResponseEvent(
        form_type_id=simple_form_schema.form_type.id,
        form_type_name=simple_form_schema.type_name,
        schema_id=simple_form_schema.id,
        schema_version=simple_form_schema.version,
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
