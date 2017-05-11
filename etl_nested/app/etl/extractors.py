import enum

import sqlalchemy.orm as sa_orm

from app import models


class RelatedLoadType(enum.Enum):
    default = 'default'
    joined_load = 'joined_load'
    explicit_join = 'explicit_join'


def _submission_query(session: sa_orm.Session, related:RelatedLoadType=None):
    assert related is not None
    related = RelatedLoadType(related)

    # this is the default query
    query = session.query(models.Submission)

    if related == RelatedLoadType.joined_load:
        # Here we only eager-load Submission.user and rely on SQLAlchemy to lazy-load Submission.form
        # This is acceptable as Form.schema is large and there are generally few forms
        query = query.options(
            sa_orm.joinedload(models.Submission.user, innerjoin=True)
        )
    elif related == RelatedLoadType.explicit_join:
        # Here, we eager-load both Submission.user and Submission.form.
        # However, we restrict the form to only load the 'name' column
        query = query \
            .join(models.User) \
            .join(models.Form) \
            .options(
                sa_orm.contains_eager(models.Submission.user),
                sa_orm.contains_eager(models.Submission.form).load_only('name'),
            )

    return query


def naive_extractor(session: sa_orm.Session, related:RelatedLoadType=None):
    return _submission_query(session, related)


def naive_load_all_extractor(session: sa_orm.Session, related:RelatedLoadType=None):
    return _submission_query(session, related).all()


def chunked_extractor(session: sa_orm.Session, related:RelatedLoadType=None, chunk_size:int=None):
    assert chunk_size

    return _submission_query(session, related).yield_per(chunk_size)
