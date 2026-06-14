import os
from unittest import TestCase
from mock import patch

from carbon.tests.util import TestSettings
from carbon.database import WhisperDatabase


TEST_DIRECTORY = os.path.dirname(os.path.realpath(__file__))
GOOD_CONF_DIR = os.path.join(TEST_DIRECTORY, 'data', 'conf-directory')
BAD_AGGREGATION_CONF = os.path.join(
    TEST_DIRECTORY, 'data', 'conf-directory-bad-aggregation', 'storage-aggregation.conf')


class ReloadAggregationSchemasTest(TestCase):
    """Running-server reload path for storage-aggregation.conf.

    carbon.writer.reloadAggregationSchemas() runs on a timer. A single broken
    section must not abort the round, and a wholesale load failure must leave the
    last known-good schema set in place rather than clobber it.
    """

    def setUp(self):
        settings = TestSettings()
        # A valid CONF_DIR is required because carbon.storage resolves its
        # config-path constants at import time and carbon.writer loads schemas in
        # its module body; both are imported (below) only after this patch is live.
        settings['CONF_DIR'] = GOOD_CONF_DIR
        settings['LOCAL_DATA_DIR'] = ''
        self._settings_patch = patch('carbon.conf.settings', settings)
        self._settings_patch.start()
        self._database_patch = patch('carbon.state.database', new=WhisperDatabase(settings))
        self._database_patch.start()
        import carbon.writer as writer
        self.writer = writer

    def tearDown(self):
        self._database_patch.stop()
        self._settings_patch.stop()

    def test_reload_skips_invalid_sections_and_keeps_valid(self):
        from carbon.storage import DefaultSchema
        writer = self.writer
        # A recognizable placeholder standing in for the previously loaded set.
        stale = DefaultSchema('__stale__', (None, None))
        with patch.object(writer, 'AGGREGATION_SCHEMAS', [stale]), \
                patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', BAD_AGGREGATION_CONF):
            # The broken sections must not abort the reload.
            writer.reloadAggregationSchemas()
            names = [schema.name for schema in writer.AGGREGATION_SCHEMAS]
        # Valid sections from the fixture are applied...
        self.assertIn('good_min', names)
        self.assertIn('good_sum', names)
        # ...invalid sections are skipped...
        self.assertNotIn('bad_xff_out_of_range', names)
        self.assertNotIn('bad_method', names)
        self.assertNotIn('bad_xff_not_a_number', names)
        # ...the trailing default is present and the stale placeholder is gone.
        self.assertEqual(names[-1], 'default')
        self.assertNotIn('__stale__', names)

    def test_reload_preserves_previous_schemas_on_failure(self):
        writer = self.writer
        previous = ['__previous_good__']

        def explode():
            raise RuntimeError('catastrophic load failure')

        with patch.object(writer, 'AGGREGATION_SCHEMAS', previous), \
                patch.object(writer, 'loadAggregationSchemas', explode):
            # A total load failure must not pollute the last good schema set.
            writer.reloadAggregationSchemas()
            self.assertIs(writer.AGGREGATION_SCHEMAS, previous)
