import datetime

import sqlalchemy.orm as sa_orm

from app import models
from tests import factories


def test_simple_submission_response(session: sa_orm.Session,
                                    extractor,
                                    simple_form_schema: models.FormSchema,
                                    simple_response_data: dict,
                                    user: models.User):
    expected_submission = factories.SubmissionFactory(schema=simple_form_schema, user=user,
                                                      responses=simple_response_data)
    session.add(expected_submission)
    session.flush()

    actual_submissions = [s for s in extractor()]
    assert len(actual_submissions) == 1

    actual_submission = actual_submissions[0]
    assert actual_submission == expected_submission


def test_load_submission_range(submission_metrics, extractor):
    num_iterations = 0
    submission_ids = set()
    form_type_ids = set()
    schema_ids = set()
    user_ids = set()
    for extracted_submission in extractor():
        # basic sanity check
        assert extracted_submission
        assert isinstance(extracted_submission, models.Submission)
        assert isinstance(extracted_submission.responses, dict)
        assert isinstance(extracted_submission.date_created, datetime.datetime)

        # accumulate metrics while iterating
        submission_ids.add(extracted_submission.id)
        schema_ids.add(extracted_submission.schema_id)
        form_type_ids.add(extracted_submission.schema.form_type.id)
        user_ids.add(extracted_submission.user_id)
        num_iterations += 1

    # verify metrics
    # TODO load back actual IDs from database??
    assert num_iterations == submission_metrics.submissions
    assert len(submission_ids) == submission_metrics.submissions
    assert len(schema_ids) == submission_metrics.schemas
    assert len(form_type_ids) == submission_metrics.form_types
    assert len(user_ids) == submission_metrics.users
