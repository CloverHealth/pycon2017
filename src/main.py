import argparse
import logging
import os
import sys

import sqlalchemy as sa
import sqlalchemy.orm as sa_orm

from app import models, constants, factories
from app.util.json import load_json_file
from app.logs import setup_logging


LOGGER = logging.getLogger(__name__)


def generate_data(session:sa_orm.Session, scenario_name:dict,
                  conf_dir:str=constants.DEFAULT_CONFIG_DIR):
    config = load_json_file(os.path.join(conf_dir, 'perfdata.conf.json'))
    scenario = config[scenario_name]

    # parse metrics
    metrics = factories.SourceDataMetrics(**scenario['metrics'])

    # load all available schemas
    schemas = []
    for schema_name in scenario['schemas']:
        schema_filename = os.path.join(conf_dir, 'schemas', schema_name + '.json')
        schemas.append(load_json_file(schema_filename))

    # generate the data
    factories.make_source_data(session, metrics, schemas)


def process_data(session:sa_orm.Session, config_name:str,
                 conf_dir:str=constants.DEFAULT_CONFIG_DIR):
    # constructor the processor
    config = load_json_file(os.path.join(conf_dir, constants.PROCESSOR_CONFIG_FILE))
    processor = factories.make_processor(session, config[config_name])

    # run the processor
    processor()


def main(args):
    db_url = args.db_url
    if not db_url.startswith('postgresql://'):
        db_url = 'postgresql://localhost/' + db_url
    setup_logging(sql_logging=args.sql_logging)

    db = sa.create_engine(db_url)
    session = None
    try:
        models.init_database(db)
        sessionmaker = sa_orm.sessionmaker(db)
        session = sessionmaker()

        if args.command == 'generate':
            generate_data(session, args.scenario_name)
        elif args.command == 'process':
            process_data(session, args.config_name)

        session.commit()
    except:
        if session:
            session.rollback()
        raise
    finally:
        db.dispose()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Clover PyCon ETL Workshop')
    parser.add_argument('db_url', help='Postgres database URL', type=str)
    parser.add_argument('--show-sql', help='Enable SQL logging', action='store_true', default=False,
                        dest='sql_logging')
    subparsers = parser.add_subparsers(dest='command', help='sub-command help')

    generate_command = subparsers.add_parser('generate', help='Generate data')
    generate_command.add_argument('scenario_name', help='Scenario name', type=str, metavar='scenario')

    process_command = subparsers.add_parser('process', help='Process data')
    process_command.add_argument('config_name', help='Name for processor configuration', type=str)

    args = parser.parse_args()

    try:
        main(args)
    except:
        LOGGER.exception('Command failed')
        sys.exit(1)
