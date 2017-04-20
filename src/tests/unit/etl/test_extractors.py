import datetime

import sqlalchemy.orm as sa_orm

from app import models, factories


def test_simple_submission_response(session: sa_orm.Session,
                                    extractor,
                                    simple_form: models.Form,
                                    simple_response_data: dict,
                                    user: models.User):
    expected_submission = factories.SubmissionFactory(form=simple_form, user=user,
                                                      responses=simple_response_data)
    session.add(expected_submission)
    session.flush()

    actual_submissions = [s for s in extractor()]
    assert len(actual_submissions) == 1

    actual_submission = actual_submissions[0]
    assert actual_submission == expected_submission


def test_load_submission_range(source_data, extractor):
    num_iterations = 0
    submission_ids = set()
    form_ids = set()
    user_ids = set()
    for extracted_submission in extractor():
        # basic sanity check
        assert extracted_submission
        assert isinstance(extracted_submission, models.Submission)
        assert isinstance(extracted_submission.responses, dict)
        assert isinstance(extracted_submission.date_created, datetime.datetime)

        # accumulate metrics while iterating
        submission_ids.add(extracted_submission.id)
        form_ids.add(extracted_submission.form_id)
        user_ids.add(extracted_submission.user_id)
        num_iterations += 1

    # verify metrics
    # TODO load back actual IDs from database??
    assert num_iterations == source_data.submissions
    assert len(submission_ids) == source_data.submissions
    assert len(form_ids) == source_data.forms
    assert len(user_ids) == source_data.users
