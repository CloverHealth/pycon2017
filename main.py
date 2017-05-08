import argparse
import contextlib
import logging
import os
import shutil
import subprocess
import sys
import time

import sqlalchemy as sa
import sqlalchemy.orm as sa_orm

from app import db as perf_db
from app import models, constants, factories
from app.logs import setup_logging
from app.util.json import load_json_file

LOGGER = logging.getLogger(__name__)


@contextlib.contextmanager
def make_perf_session(test_db)-> sa_orm.Session:
    db = None
    session = None
    try:
        db_url = test_db.url()
        db = sa.create_engine(db_url)
        sessionmaker = sa_orm.sessionmaker(db)
        session = sessionmaker()

        yield session

    finally:
        if session:
            session.close()
        if db:
            db.dispose()


def generate_data(session:sa_orm.Session, scenario_name:dict,
                  conf_dir:str= constants.DEFAULT_CONFIG_DIR):
    config = load_json_file(os.path.join(conf_dir, 'perfdata.conf.json'))
    scenario = config[scenario_name]

    # parse metrics
    metrics = factories.SourceDataMetrics(**scenario['metrics'])

    # load all available schemas
    schemas = []
    for schema_name in scenario['schemas']:
        schema_filename = os.path.join(conf_dir, 'schemas', schema_name + '.json')
        schemas.append(load_json_file(schema_filename))

    LOGGER.info('Creating database schema...')
    models.init_database(session.connection())

    # generate the data
    LOGGER.info('Generating data...')
    factories.make_source_data(session, metrics, schemas)

    session.commit()


def process_data(session:sa_orm.Session, config_name:str, use_memory_profiler:bool,
                 conf_dir:str= constants.DEFAULT_CONFIG_DIR):
    # constructor the processor
    config = load_json_file(os.path.join(conf_dir, constants.PROCESSOR_CONFIG_FILE))
    processor = factories.make_processor(session, config[config_name], use_memory_profiler=use_memory_profiler)

    # run the processor
    if use_memory_profiler:
        LOGGER.warning('NOTE: Using memory profiler instruments code and will result in slower runtime')
    LOGGER.info('Processing...')
    processor()

    session.commit()


def review_data(db_url:str):
    # open a separate psql client to the existing database
    run_args = ['psql', db_url]
    subprocess.run(run_args)


def remove_data(data_dir:str):
    LOGGER.info('Removing perf data directory')
    shutil.rmtree(data_dir, ignore_errors=True)


def main(args):
    if args.command == 'clean':
        remove_data(args.data_dir)
        return

    setup_logging(sql_logging=args.sql_logging)

    perf_db_kwargs = {
        'root_dir': args.data_dir,
        'scenario_name': args.scenario_name
    }
    if args.command == 'generate':
        perf_db_kwargs['db_type'] = perf_db.DatabaseType.template
    else:
        perf_db_kwargs['db_type'] = perf_db.DatabaseType.test_run

    if args.command == 'process':
        perf_db_kwargs['copy_from_template'] = True

    show_elapsed_time = True
    if args.command == 'psql':
        show_elapsed_time = False

    with perf_db.PerfTestDatabase(**perf_db_kwargs) as postgresql:
        with make_perf_session(postgresql) as session:
            # start the timer
            # NOTE: we do not included database connection and initialization in our timing measurements
            start_counter = time.perf_counter()

            if args.command == 'generate':
                generate_data(session, args.scenario_name)
            elif args.command == 'process':
                process_data(session, args.config_name, args.profile_mem)
            elif args.command == 'psql':
                review_data(postgresql.url())

            # stop the counter and log the elapsed time
            end_counter = time.perf_counter()
            if show_elapsed_time:
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

    psql_command = subparsers.add_parser('psql', help='Connect to processed database using psql')
    psql_command.add_argument('scenario_name', help='Scenario name', type=str, metavar='scenario')

    args = parser.parse_args()

    try:
        main(args)
    except:
        LOGGER.exception('Command failed')
        sys.exit(1)
