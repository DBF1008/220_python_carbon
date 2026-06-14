import os
import sys
from unittest import TestCase
from mock import patch, MagicMock

from carbon.tests.util import TestSettings
from carbon.database import WhisperDatabase

# carbon.writer calls loadStorageSchemas() and loadAggregationSchemas() at
# module level, so settings and database must be configured *before* the
# import.  These module-level patches are stopped right after the import;
# individual tests apply their own patches as needed.
_test_dir = os.path.dirname(os.path.realpath(__file__))
_conf_dir = os.path.join(_test_dir, 'data', 'conf-directory')
_init_settings = TestSettings()
_init_settings['CONF_DIR'] = _conf_dir
_init_settings['LOCAL_DATA_DIR'] = ''

_sp = patch('carbon.conf.settings', _init_settings)
_sp.start()
_dp = patch('carbon.state.database', new=WhisperDatabase(_init_settings))
_dp.start()

# Force re-import so the module-level calls use our test settings.
if 'carbon.writer' in sys.modules:
    del sys.modules['carbon.writer']

import carbon.writer  # noqa: E402

_dp.stop()
_sp.stop()


class ReloadAggregationSchemasTest(TestCase):
    """Regression tests for writer.reloadAggregationSchemas.

    Verifies that a failed reload preserves the previous good schema list
    and that a successful reload replaces it.
    """

    def test_reload_preserves_schemas_on_exception(self):
        """When loadAggregationSchemas raises, the global AGGREGATION_SCHEMAS
        must remain unchanged — the previous good set is not polluted."""
        old_schemas = [MagicMock(name='old_schema')]
        carbon.writer.AGGREGATION_SCHEMAS = old_schemas

        with patch.object(carbon.writer, 'loadAggregationSchemas',
                          side_effect=Exception("config error")):
            carbon.writer.reloadAggregationSchemas()

        self.assertIs(carbon.writer.AGGREGATION_SCHEMAS, old_schemas)

    def test_reload_updates_schemas_on_success(self):
        """When loadAggregationSchemas succeeds, the global is replaced."""
        old_schemas = [MagicMock(name='old_schema')]
        new_schemas = [MagicMock(name='new_schema')]
        carbon.writer.AGGREGATION_SCHEMAS = old_schemas

        with patch.object(carbon.writer, 'loadAggregationSchemas',
                          return_value=new_schemas):
            carbon.writer.reloadAggregationSchemas()

        self.assertIs(carbon.writer.AGGREGATION_SCHEMAS, new_schemas)

    def test_reload_preserves_schemas_on_assertion_error(self):
        """AssertionError from loadAggregationSchemas (the specific bug being
        fixed) must also be caught and must not corrupt the global."""
        old_schemas = [MagicMock(name='old_schema')]
        carbon.writer.AGGREGATION_SCHEMAS = old_schemas

        with patch.object(carbon.writer, 'loadAggregationSchemas',
                          side_effect=AssertionError("xFilesFactor out of bounds")):
            carbon.writer.reloadAggregationSchemas()

        self.assertIs(carbon.writer.AGGREGATION_SCHEMAS, old_schemas)
