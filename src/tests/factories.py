import itertools
import random
import uuid

import factory
from faker import Faker
import sqlalchemy.orm as sa_orm

from app import models, constants
from app.timestamp_utils import UTC_TZ

FAKE = Faker()

class FormTypeFactory(factory.Factory):
    class Meta:
        model = models.FormType

    name = factory.Faker('slug')
    description = factory.Faker('text')


class FormSchemaFactory(factory.Factory):
    class Meta:
        model = models.FormSchema

    form_type = factory.SubFactory(FormTypeFactory)
    version = 1
    data = None  # must be provided


class UserFactory(factory.Factory):
    class Meta:
        model = models.User

    given_name = factory.Faker('first_name')
    family_name = factory.Faker('last_name')


class SubmissionFactory(factory.Factory):
    class Meta:
        model = models.Submission

    schema = factory.SubFactory(FormSchemaFactory)
    user = factory.SubFactory(UserFactory)
    responses = None  # must be provided


_VALUE_FAKERS = {
    constants.AnswerType.number: lambda: str(random.randint(1, 1000)),
    constants.AnswerType.text: FAKE.text,
    constants.AnswerType.boolean: lambda: 'true' if FAKE.pybool() else 'false',
    constants.AnswerType.date: lambda: FAKE.date(pattern="%Y-%m-%d")
}


class ResponseEventFactory(factory.Factory):
    class Meta:
        model = models.ResponseEvent

    # these can be randomized without constraints as they are not foreign keys
    form_type_id = factory.LazyFunction(uuid.uuid4)
    form_type_name = factory.Faker('slug')
    schema_id = factory.LazyFunction(uuid.uuid4)
    schema_version = factory.LazyFunction(lambda: random.randint(1, 10))
    user_id = factory.LazyFunction(uuid.uuid4)
    user_full_name = factory.Faker('name')
    submission_id = factory.LazyFunction(uuid.uuid4)
    submission_created = factory.Faker('date_time_this_decade', tzinfo=UTC_TZ)
    processed_on = factory.Faker('date_time_this_decade', tzinfo=UTC_TZ)
    schema_path = factory.Faker('slug')
    answer_type = factory.Iterator(constants.AnswerType)

    @factory.lazy_attribute
    def value(self):
        value_faker = _VALUE_FAKERS[self.answer_type]
        return value_faker()


# TODO replace this with parameterized factories.Iterator on the factories
def cyclic_model_iterator(session: sa_orm.Session, model_cls):
    """
    Constructs a cyclic iterator to choose an instance of a model

    :param session: SQLAlchemy Session
    :param model_cls: model class to iterate through
    :return: iterator of model IDs
    """
    return itertools.cycle(o for o in session.query(model_cls))
