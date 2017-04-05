from app import models


def test_schema(session):
    schema_data = {
        "label": "Basic Info",
        "slug": "basic_info",
        "widget": "panel",
        "nodeType": "section",
        "children": [
            {
                "slug": "bmi",
                "label": "BMI (NOTE: MORBID OBESITY > 34.99)",
                "nodeType": "question",
                "answerType": "number",
                "widget": "text",
                "tag": "member_bmi"
            },
        ]
    }
    form = models.Form(schema=schema_data, version=1)
    session.add(form)

    session.flush()

    results = session.query(models.Form).all()
    assert results
    assert len(results) == 1

    actual_form = results[0]
    assert actual_form.schema == schema_data
    assert actual_form.version == 1
