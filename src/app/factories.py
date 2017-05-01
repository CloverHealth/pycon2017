import enum
import functools
import random
import uuid
from collections import namedtuple

import factory
import sqlalchemy.orm as sa_orm
from faker import Faker

from app import models, constants, processor
from app.etl import extractors, transformers, loaders
from app.util.timestamps import UTC_TZ

FAKE = Faker()


class FormFactory(factory.Factory):
    class Meta:
        model = models.Form

    name = factory.Faker('slug')
    description = factory.Faker('text')
    schema = None  # must be provided


class UserFactory(factory.Factory):
    class Meta:
        model = models.User

    given_name = factory.Faker('first_name')
    family_name = factory.Faker('last_name')


class OutputFormat(enum.Enum):
    json = 1
    value_strings = 2


class AnswerTypeFaker:
    def __init__(self, output_format:OutputFormat):
        self.fakers = {
            constants.AnswerType.text: lambda: FAKE.sentence(nb_words=4, variable_nb_words=True),
            constants.AnswerType.date: lambda: FAKE.date(pattern="%Y-%m-%d")
        }

        if output_format == OutputFormat.json:
            self.fakers.update({
                constants.AnswerType.number: lambda: random.randint(1, 1000),
                constants.AnswerType.boolean: lambda: FAKE.pybool()
            })
        elif output_format == OutputFormat.value_strings:
            self.fakers.update({
                constants.AnswerType.number: lambda: str(random.randint(1, 1000)),
                constants.AnswerType.boolean: lambda: 'true' if FAKE.pybool() else 'false'
            })
        else:
            raise ValueError('Invalid output_format')

    def make_fake_value(self, answer_type):
        faker = self.fakers[answer_type]
        return faker()


JSON_FAKERS = AnswerTypeFaker(OutputFormat.json)
KEY_VALUE_FAKERS = AnswerTypeFaker(OutputFormat.value_strings)


class SubmissionFactory(factory.Factory):
    class Meta:
        model = models.Submission

    class Params:
        f_make_response = None

    form = factory.SubFactory(FormFactory)
    user = factory.SubFactory(UserFactory)

    @factory.lazy_attribute
    def responses(self):
        return self.f_make_response(self.form.id) if self.f_make_response else None


class ResponseEventFactory(factory.Factory):
    class Meta:
        model = models.ResponseEvent

    # these can be randomized without constraints as they are not foreign keys
    form_id = factory.LazyFunction(uuid.uuid4)
    form_name = factory.Faker('slug')
    user_id = factory.LazyFunction(uuid.uuid4)
    user_full_name = factory.Faker('name')
    submission_id = factory.LazyFunction(uuid.uuid4)
    submission_created = factory.Faker('date_time_this_decade', tzinfo=UTC_TZ)
    processed_on = factory.Faker('date_time_this_decade', tzinfo=UTC_TZ)
    schema_path = factory.Faker('slug')
    answer_type = factory.Iterator(constants.AnswerType)

    @factory.lazy_attribute
    def value(self):
        return KEY_VALUE_FAKERS.make_fake_value(self.answer_type)


SourceDataMetrics = namedtuple(
    'SourceDataMetrics',
    ['forms', 'users', 'submissions']
)


def make_source_data(session: sa_orm.Session, metrics: SourceDataMetrics, available_schemas: list):
    """
    Creates source data based on metrics and available schemas

    :param session: SQLAlchemy session
    :param metrics: determines how many instances of each model to create
    :param available_schemas: list of dictionaries describing form schemas
    """

    # create all the forms from the available schemas
    schema_iterator = factory.Iterator(available_schemas, cycle=True)
    forms = FormFactory.build_batch(metrics.forms, schema=schema_iterator)
    session.add_all(forms)
    session.flush()

    # create all the users
    users = UserFactory.build_batch(metrics.users)
    session.add_all(users)
    session.flush()

    # use the node path map cache to generate submission responses JSON
    get_node_path_map = transformers.get_node_path_map_cache(session)
    _cached_make_response = functools.partial(make_response, get_node_path_map)

    # create all the submissions
    submissions = SubmissionFactory.build_batch(
        metrics.submissions,
        f_make_response=_cached_make_response,
        form=factory.Iterator(forms, cycle=True),
        user=factory.Iterator(users, cycle=True),
    )
    session.add_all(submissions)
    session.flush()


def make_processor(session: sa_orm.Session, processor_config: dict, use_memory_profiler:bool=False):
    """
    Factory method to create a processor using partial function

    :param session: SQLAlchemy session
    :param processor_config: processor configuration dictionary
    :return: processor function
    """
    extractor_config = processor_config['extractor']
    extractor_func = getattr(extractors, extractor_config['name'])
    extractor = functools.partial(extractor_func, session, **extractor_config.get('kwargs', {}))

    loader_config = processor_config['loader']
    loader_func = getattr(loaders, loader_config['name'])
    loader = functools.partial(loader_func, session, **loader_config.get('kwargs', {}))

    transformer_config = processor_config['transformer']
    transformer = functools.partial(transformers.transform_submissions, session, **transformer_config)

    processor_func = processor.process
    if use_memory_profiler:
        from memory_profiler import profile
        processor_func = profile(processor_func)

    return functools.partial(processor_func, extractor, transformer, loader)


def make_response(get_node_path_map, form_id):
    """
    Construct a fake dictionary response (to be stored as JSON) for a form.  This requires the get_node_path_map
    partial function to retrieve the node path map

    :param get_node_path_map: return the node path map (requires only the form_id as a parameter)
    :param form_id: form id
    :return: dict
    """

    # retrieve the node path map for this form so we know it's answer types
    node_map = get_node_path_map(form_id)

    responses = {}
    for path, answer_type in node_map.items():
        components = path.split('.')
        leaf_key = components.pop()

        # build the tree to until the leaf node
        d = responses
        for c in components:
            d[c] = d.get(c, {})
            d = d[c]

        # generate the fake leaf value and inject it
        d[leaf_key] = JSON_FAKERS.make_fake_value(answer_type)

    return responses
