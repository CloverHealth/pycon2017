import datetime
import os

import sqlalchemy.orm as sa_orm

from app import factories
from app.etl import transformers
from app.util.json import load_json_file


def test_make_response_from_nested_schema(session: sa_orm.Session, data_dir):
    schema = load_json_file(os.path.join(data_dir, 'nested_schema.json'))
    form = factories.FormFactory(schema=schema)

    user = factories.UserFactory()

    session.add_all([form, user])
    session.flush()

    get_node_path_map = transformers.get_node_path_map_cache(session)
    result = factories.make_response(get_node_path_map, form.id)

    assert result
    assert type(result) == dict

    # test a selection of the nested values
    assert result['root']
    assert type(result['root']) == dict
    assert result['root']['primary_care_doctor_phone'] is not None
    assert type(result['root']['primary_care_doctor_phone']) == str
    assert result['root']['emergency_visits'] is not None
    assert type(result['root']['emergency_visits']) == int

    assert result['root']['medical_conditions']['mobility_help']['walker'] is not None
    assert type(result['root']['medical_conditions']['mobility_help']['walker']) == bool

    assert result['root']['date_of_birth'] is not None
    assert type(result['root']['date_of_birth']) == str

    parsed_date = datetime.datetime.strptime(result['root']['date_of_birth'], '%Y-%m-%d')
    assert parsed_date
