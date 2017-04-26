import sqlalchemy.orm as sa_orm

from app import models


def _submission_query(session: sa_orm.Session, joined_load=None):
    assert joined_load is not None
    query = session.query(models.Submission)

    # use eager loading on user table if enabled
    if joined_load:
        query = query.options(sa_orm.joinedload(models.Submission.user, innerjoin=True))

    return query


def naive_extractor(session: sa_orm.Session, joined_load=None):
    return _submission_query(session, joined_load=joined_load)


def naive_load_all_extractor(session: sa_orm.Session, joined_load=None):
    return _submission_query(session, joined_load=joined_load).all()


def chunked_extractor(session: sa_orm.Session, joined_load=None, chunk_size=None):
    assert chunk_size

    return _submission_query(session, joined_load=joined_load).yield_per(chunk_size)
