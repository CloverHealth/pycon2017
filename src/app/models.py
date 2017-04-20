import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg
import sqlalchemy.orm as sa_orm
from sqlalchemy.ext import declarative

from app import constants
from app.util.timestamps import utc_now

__all__ = [
    'init_database'
    'Form'
]

SCHEMAS = {
    'app': 'clover_app',
    'dwh': 'clover_dwh'
}

METADATA = sa.MetaData()
BaseModel = declarative.declarative_base(metadata=METADATA)


def init_database(db: sa.engine.Connectable):
    """
    Initializes the database to support the models

    :param db: SQLAlchemy connectable instance
    """

    # setup the Postgres extensions and schema
    db.execute("""
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp" WITH SCHEMA public;
    """)
    db.execute(
        ';\n'.join(
            'CREATE SCHEMA IF NOT EXISTS {}'.format(s) for s in SCHEMAS.values()
        )
    )

    # create the schema from the models
    METADATA.create_all(bind=db)


class PrimaryKeyUUIDMixin:
    """
    Includes an 'id' primary key UUID column
     
    This is used to generate primary keys using the Postgres database server rather than the application
    """
    __repr_details__ = ['id']


    id = sa.Column(sa_pg.UUID(as_uuid=True), primary_key=True, server_default=sa.text('uuid_generate_v4()'))

    def __repr__(self):
        return '{}({})'.format(
            self.__class__.__name__,
            ', '.join('{}={}'.format(d, str(getattr(self, d))) for d in self.__repr_details__)
        )


class ApplicationModel(BaseModel, PrimaryKeyUUIDMixin):
    __abstract__ = True
    __table_args__ = (
        {'schema': SCHEMAS['app']},
    )


class DataWarehouseModel(BaseModel, PrimaryKeyUUIDMixin):
    __abstract__ = True
    __table_args__ = (
        {'schema': SCHEMAS['dwh']},
    )


class Form(ApplicationModel):
    __tablename__ = 'form_schemas'
    __repr_details__ = ['name']

    name = sa.Column(sa.Text(), nullable=False, unique=True, index=True)
    description = sa.Column(sa.Text(), nullable=False)
    schema = sa.Column(sa_pg.JSONB, nullable=False)


class User(ApplicationModel):
    """
    A user who can create responses to forms
    """
    __tablename__ = 'users'
    __repr_details__ = ['full_name']

    given_name = sa.Column(sa.Text, nullable=False)
    family_name = sa.Column(sa.Text, nullable=False)

    @property
    def full_name(self):
        return "%s %s" % (self.given_name, self.family_name)


class Submission(ApplicationModel):
    """
    Answers to a form created by a user
    """
    __tablename__ = 'form_responses'

    form_id = sa.Column(sa.ForeignKey(Form.id), nullable=False)
    user_id = sa.Column(sa.ForeignKey(User.id), nullable=False)
    responses = sa.Column(sa_pg.JSON, nullable=False)
    date_created = sa.Column(sa.DateTime(timezone=True), default=utc_now, nullable=False)

    form = sa_orm.relationship(Form)
    user = sa_orm.relationship(User)


class ResponseEvent(DataWarehouseModel):
    """
    Represents an ETL transform of individual responses

    This is an OLAP table where the following is expected:
    - No foreign keys
    - Redundant data (for faster analytical queries)
    """
    __tablename__ = 'response_events'

    # form schema information
    form_id = sa.Column(sa_pg.UUID(as_uuid=True), nullable=False)  # FormType.id
    form_name = sa.Column(sa.Text, nullable=False)  # FormType.name

    # user information
    user_id = sa.Column(sa_pg.UUID(as_uuid=True), nullable=False)  # User.id
    user_full_name = sa.Column(sa.Text, nullable=False)  # User.full_name (property)

    # submission information
    submission_id = sa.Column(sa_pg.UUID(as_uuid=True), nullable=False)
    submission_created = sa.Column(sa.DateTime(timezone=True), nullable=False)  # Submission.date_created

    # transformed properties
    processed_on = sa.Column(sa.DateTime(timezone=True), nullable=False)  # when this event was created
    schema_path = sa.Column(sa.Text, nullable=False)  # dot separated path to node in Submission.responses
    value = sa.Column(sa.Text, nullable=False)  # value of node in Submission.responses
    answer_type = sa.Column(sa.Enum(constants.AnswerType), nullable=False)  # answerType from node in Schema
