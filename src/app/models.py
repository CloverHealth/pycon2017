
import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg
from sqlalchemy.ext import declarative

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


def auto_uuid():
    """
    Create a UUID primary key column where Postgres manages creating new keys
    """
    # TODO should thie be declarative?
    uuid_gen_expr = sa.text('uuid_generate_v4()')
    return sa.Column(sa_pg.UUID(as_uuid=True), primary_key=True, server_default=uuid_gen_expr)


class Form(BaseModel):
    """
    Form questions and structure
    """
    __tablename__ = 'forms'
    __table_args__ = {'schema': SCHEMAS['app']}

    id = auto_uuid()
    version = sa.Column(sa.Integer, nullable=False)
    # TODO decide on JSON versus JSONB
    schema = sa.Column(sa_pg.JSONB, nullable=False)


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
