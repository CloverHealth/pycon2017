import argparse
import contextlib
import enum
import logging
import os
import shutil
import sys
import time

import sqlalchemy as sa
import sqlalchemy.orm as sa_orm
import testing.postgresql

from app import models, constants, factories
from app.util.json import load_json_file
from app.logs import setup_logging


LOGGER = logging.getLogger(__name__)


class PerfSessionType(enum.Enum):
    create_scenario = 1
    copy_scenario = 2

MAX_DATA_DIR_PATH_LEN = 70


@contextlib.contextmanager
def make_perf_session(root_data_dir:str, scenario_name:str, session_type:PerfSessionType)-> sa_orm.Session:
    # limit the length of the data directory root due to length limitations of UNIX socket names
    if len(root_data_dir) > MAX_DATA_DIR_PATH_LEN:
        LOGGER.error('Root data directory path length is too long: %s', root_data_dir)
        LOGGER.error('Please select a different path using the --data-dir option')
        raise RuntimeError('Root data directory path too long')

    source_db_data_path = os.path.join(root_data_dir, 'templates', scenario_name)

    # if creating a new scenario, regenerate the template directory
    if session_type == PerfSessionType.create_scenario:
        shutil.rmtree(source_db_data_path, ignore_errors=True)
        os.makedirs(source_db_data_path)
        session_db_data_path = source_db_data_path
    # otherwise, copy from the template directory
    else:
        if not os.path.exists(source_db_data_path):
            LOGGER.error("You need to generate scenario '%s' first", scenario_name)
            raise RuntimeError('Template directory missing')

        session_db_data_path = os.path.join(root_data_dir, 'test_runs', scenario_name)

        LOGGER.info("Copying scenario '%s' from template", scenario_name)
        shutil.rmtree(session_db_data_path, ignore_errors=True)
        shutil.copytree(source_db_data_path, session_db_data_path)

    with testing.postgresql.Postgresql(base_dir=session_db_data_path) as postgresql:
        db = None
        try:
            db_url = postgresql.url()
            db = sa.create_engine(db_url)
            if session_type == PerfSessionType.create_scenario:
                LOGGER.info('Creating database schema')
                models.init_database(db)
            sessionmaker = sa_orm.sessionmaker(db)
            session = sessionmaker()

            yield session

        finally:
            if db:
                db.dispose()


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
    LOGGER.info('Generating data')
    factories.make_source_data(session, metrics, schemas)


def process_data(session:sa_orm.Session, config_name:str, use_memory_profiler:bool,
                 conf_dir:str=constants.DEFAULT_CONFIG_DIR):
    # constructor the processor
    config = load_json_file(os.path.join(conf_dir, constants.PROCESSOR_CONFIG_FILE))
    processor = factories.make_processor(session, config[config_name], use_memory_profiler=use_memory_profiler)

    # run the processor
    if use_memory_profiler:
        LOGGER.warning('NOTE: Using memory profiler instruments code and will result in slower runtime')
    processor()


def main(args):
    if args.command == 'clean':
        LOGGER.info('Removing perf data directory')
        shutil.rmtree(args.data_dir, ignore_errors=True)
        return

    setup_logging(sql_logging=args.sql_logging)

    session_type = PerfSessionType.create_scenario if args.command == 'generate' else PerfSessionType.copy_scenario

    with make_perf_session(args.data_dir, args.scenario_name, session_type) as session:
        # start the timer
        # NOTE: we do not included database connection and initialization in our timing measurements
        start_counter = time.perf_counter()

        if args.command == 'generate':
            generate_data(session, args.scenario_name)
        elif args.command == 'process':
            process_data(session, args.config_name, args.profile_mem)

        session.commit()

        # stop the counter and log the elapsed time
        end_counter = time.perf_counter()
        LOGGER.info('Elapsed time (seconds): %s', '{:.03f}'.format(end_counter - start_counter))


if __name__ == '__main__':
    default_root_dir = os.path.join(os.getcwd(), '.perf_dbs')

    parser = argparse.ArgumentParser(description='Clover PyCon ETL Workshop')
    parser.add_argument('--debug-sql', help='Enable SQL logging', action='store_true',
                        default=False, dest='sql_logging')
    parser.add_argument('--data-dir', help='Root directory for all perf test databases', type=str,
                        default=default_root_dir)
    subparsers = parser.add_subparsers(dest='command', help='sub-command help')

    generate_command = subparsers.add_parser('generate', help='Generate data')
    generate_command.add_argument('scenario_name', help='Scenario name', type=str, metavar='scenario')

    process_command = subparsers.add_parser('process', help='Process data')
    process_command.add_argument('--debug-mem', help='Show memory profiling for processor', action='store_true',
                                 default=False, dest='profile_mem')
    process_command.add_argument('scenario_name', help='Scenario name', type=str, metavar='scenario')
    process_command.add_argument('config_name', help='Name for processor configuration', type=str)

    subparsers.add_parser('clean', help='Removes all perf test data')

    args = parser.parse_args()

    try:
        main(args)
    except:
        LOGGER.exception('Command failed')
        sys.exit(1)
