import enum
import logging
import os
import shutil

import testing.postgresql


LOGGER = logging.getLogger(__name__)


class DatabaseType(enum.Enum):
    template = 0
    test_run = 1


class PerfTestDatabase(testing.postgresql.Postgresql):
    TEMPLATES_SUBDIR = 'templates'
    TESTRUN_SUBDIR = 'test_runs'

    # limit the length of the data directory root due to length limitations of UNIX socket names
    MAX_DATA_DIR_PATH_LEN = 70

    def __init__(self, *,
                 root_dir:str=None,
                 scenario_name:str=None,
                 db_type:DatabaseType=None,
                 copy_from_template:bool=False,
                 **kwargs):
        assert root_dir is not None
        assert scenario_name is not None
        assert db_type is not None

        if db_type == DatabaseType.template and copy_from_template:
            raise ValueError('Setting copy_from_template only valid for test run databases')

        if len(root_dir) > self.MAX_DATA_DIR_PATH_LEN:
            LOGGER.error('Root directory path length is too long: %s', root_dir)
            LOGGER.error('Please select a different path using the --data-dir option')
            raise RuntimeError('Root data directory path too long')

        # determine the template database root directory
        self._source_db_path = os.path.join(root_dir, self.TEMPLATES_SUBDIR, scenario_name)

        kwargs = kwargs.copy()
        if db_type == DatabaseType.template:
            kwargs['base_dir'] = self._source_db_path
            self._erase_source_db = True
        else:
            kwargs['base_dir'] = os.path.join(root_dir, self.TESTRUN_SUBDIR, scenario_name)
            self._erase_source_db = False

        if copy_from_template:
            if not os.path.exists(self._source_db_path):
                LOGGER.error("You need to generate scenario '%s' first", scenario_name)
                raise RuntimeError('Template directory missing')
            kwargs['copy_data_from'] = os.path.join(self._source_db_path, 'data')

        super().__init__(**kwargs)

    def setup(self):
        # if creating a new scenario, regenerate an empty template directory
        if self._erase_source_db:
            LOGGER.info("Recreating scenario template dir: %s", self._source_db_path)
            shutil.rmtree(self._source_db_path, ignore_errors=True)
            os.makedirs(self._source_db_path)

        # if copying data from the template, clear out the directory first
        elif self.settings.get('copy_data_from'):
            LOGGER.info("Copying scenario from template dir: %s", self._source_db_path)
            shutil.rmtree(self.base_dir, ignore_errors=True)

        super().setup()
