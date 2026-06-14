import os
import tempfile
from unittest import TestCase
from mock import patch, MagicMock

from carbon.tests.util import TestSettings
from carbon.database import WhisperDatabase


# class NoConfigSchemaLoadingTest(TestCase):

#     def setUp(self):
#         settings = {
#             'CONF_DIR': '',
#         }
#         self._settings_patch = patch.dict('carbon.conf.settings', settings)
#         self._settings_patch.start()

#     def tearDown(self):
#         self._settings_patch.stop()

#     def test_loadAggregationSchemas_load_default_schema(self):
#         from carbon.storage import loadAggregationSchemas, defaultAggregation
#         schema_list = loadAggregationSchemas()
#         self.assertEqual(len(schema_list), 1)
#         schema = schema_list[0]
#         self.assertEqual(schema, defaultAggregation)

#     def test_loadStorageSchemas_raise_CarbonConfigException(self):
#         from carbon.storage import loadStorageSchemas
#         from carbon.exceptions import CarbonConfigException
#         with self.assertRaises(CarbonConfigException):
#             loadStorageSchemas()


class ExistingConfigSchemaLoadingTest(TestCase):

    def setUp(self):
        test_directory = os.path.dirname(os.path.realpath(__file__))
        settings = TestSettings()
        settings['CONF_DIR'] = os.path.join(test_directory, 'data', 'conf-directory')
        settings['LOCAL_DATA_DIR'] = ''
        self._settings_patch = patch('carbon.conf.settings', settings)
        self._settings_patch.start()
        self._database_patch = patch('carbon.state.database', new=WhisperDatabase(settings))
        self._database_patch.start()

    def tearDown(self):
        self._database_patch.stop()
        self._settings_patch.stop()

    def test_loadStorageSchemas_return_schemas(self):
        from carbon.storage import loadStorageSchemas, PatternSchema, Archive
        schema_list = loadStorageSchemas()
        self.assertEqual(len(schema_list), 3)
        expected = [
            PatternSchema('carbon', r'^carbon\.', [Archive.fromString('60:90d')]),
            PatternSchema('default_1min_for_1day', '.*', [Archive.fromString('60s:1d')])
        ]
        for schema, expected_schema in zip(schema_list[:-1], expected):
            self.assertEqual(schema.name, expected_schema.name)
            self.assertEqual(schema.pattern, expected_schema.pattern)
            for (archive, expected_archive) in zip(schema.archives, expected_schema.archives):
                self.assertEqual(archive.getTuple(), expected_archive.getTuple())

    def test_loadStorageSchemas_return_the_default_schema_last(self):
        from carbon.storage import loadStorageSchemas, defaultSchema
        schema_list = loadStorageSchemas()
        last_schema = schema_list[-1]
        self.assertEqual(last_schema.name, defaultSchema.name)
        self.assertEqual(last_schema.archives, defaultSchema.archives)

    def test_loadAggregationSchemas_return_schemas(self):
        from carbon.storage import loadAggregationSchemas, PatternSchema
        schema_list = loadAggregationSchemas()
        self.assertEqual(len(schema_list), 5)
        expected = [
            PatternSchema('min', r'\.min$', (0.1, 'min')),
            PatternSchema('max', r'\.max$', (0.1, 'max')),
            PatternSchema('sum', r'\.count$', (0, 'sum')),
            PatternSchema('default_average', '.*', (0.5, 'average'))
        ]
        for schema, expected_schema in zip(schema_list[:-1], expected):
            self.assertEqual(schema.name, expected_schema.name)
            self.assertEqual(schema.pattern, expected_schema.pattern)
            self.assertEqual(schema.archives, expected_schema.archives)

    def test_loadAggregationSchema_return_the_default_schema_last(self):
        from carbon.storage import loadAggregationSchemas, defaultAggregation
        schema_list = loadAggregationSchemas()
        last_schema = schema_list[-1]
        self.assertEqual(last_schema, defaultAggregation)


class AggregationSchemaErrorHandlingTest(TestCase):
    """Regression tests for loadAggregationSchemas error handling.

    Verifies that invalid config sections are logged and skipped rather than
    causing the entire load to fail.  Covers both ValueError (non-numeric
    xFilesFactor) and AssertionError (xFilesFactor out of [0,1] bounds,
    aggregationMethod not supported by the database backend).
    """

    def setUp(self):
        test_directory = os.path.dirname(os.path.realpath(__file__))
        self.settings = TestSettings()
        self.settings['CONF_DIR'] = os.path.join(test_directory, 'data', 'conf-directory')
        self.settings['LOCAL_DATA_DIR'] = ''
        self._settings_patch = patch('carbon.conf.settings', self.settings)
        self._settings_patch.start()
        self._database_patch = patch('carbon.state.database', new=WhisperDatabase(self.settings))
        self._database_patch.start()
        self._tmpfile = tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False)

    def tearDown(self):
        self._database_patch.stop()
        self._settings_patch.stop()
        os.unlink(self._tmpfile.name)

    def _write_config(self, content):
        self._tmpfile.seek(0)
        self._tmpfile.truncate()
        self._tmpfile.write(content)
        self._tmpfile.flush()

    def test_xFilesFactor_out_of_bounds_skips_section(self):
        """xFilesFactor > 1 raises AssertionError internally; the section must
        be skipped while valid sections and the default still load."""
        from carbon.storage import loadAggregationSchemas, defaultAggregation, PatternSchema
        self._write_config(
            "[bad]\n"
            "pattern = \\.bad$\n"
            "xFilesFactor = 1.5\n"
            "aggregationMethod = average\n"
            "\n"
            "[good]\n"
            "pattern = \\.good$\n"
            "xFilesFactor = 0.5\n"
            "aggregationMethod = average\n"
        )
        with patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', self._tmpfile.name):
            result = loadAggregationSchemas()
        # bad section skipped, good section present, default appended
        self.assertEqual(len(result), 2)
        pattern_schemas = [s for s in result if isinstance(s, PatternSchema)]
        self.assertEqual(len(pattern_schemas), 1)
        self.assertEqual(pattern_schemas[0].name, 'good')
        self.assertEqual(result[-1], defaultAggregation)

    def test_xFilesFactor_negative_skips_section(self):
        """Negative xFilesFactor is also out of [0,1] bounds."""
        from carbon.storage import loadAggregationSchemas, defaultAggregation, PatternSchema
        self._write_config(
            "[negative]\n"
            "pattern = \\.neg$\n"
            "xFilesFactor = -0.1\n"
            "aggregationMethod = average\n"
        )
        with patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', self._tmpfile.name):
            result = loadAggregationSchemas()
        pattern_schemas = [s for s in result if isinstance(s, PatternSchema)]
        self.assertEqual(len(pattern_schemas), 0)
        self.assertEqual(result, [defaultAggregation])

    def test_xFilesFactor_non_numeric_skips_section(self):
        """Non-numeric xFilesFactor raises ValueError; section must be skipped."""
        from carbon.storage import loadAggregationSchemas, defaultAggregation, PatternSchema
        self._write_config(
            "[bad]\n"
            "pattern = \\.bad$\n"
            "xFilesFactor = not_a_number\n"
            "aggregationMethod = average\n"
        )
        with patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', self._tmpfile.name):
            result = loadAggregationSchemas()
        pattern_schemas = [s for s in result if isinstance(s, PatternSchema)]
        self.assertEqual(len(pattern_schemas), 0)
        self.assertEqual(result, [defaultAggregation])

    def test_unsupported_aggregation_method_skips_section(self):
        """aggregationMethod not in database.aggregationMethods raises
        AssertionError internally; section must be skipped."""
        from carbon.storage import loadAggregationSchemas, defaultAggregation, PatternSchema
        self._write_config(
            "[bad]\n"
            "pattern = \\.bad$\n"
            "xFilesFactor = 0.5\n"
            "aggregationMethod = bogus_method\n"
        )
        with patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', self._tmpfile.name):
            result = loadAggregationSchemas()
        pattern_schemas = [s for s in result if isinstance(s, PatternSchema)]
        self.assertEqual(len(pattern_schemas), 0)
        self.assertEqual(result, [defaultAggregation])

    def test_valid_and_invalid_sections_mixed(self):
        """With a mix of valid and invalid sections, only valid ones survive."""
        from carbon.storage import loadAggregationSchemas, PatternSchema
        self._write_config(
            "[valid1]\n"
            "pattern = \\.v1$\n"
            "xFilesFactor = 0.1\n"
            "aggregationMethod = average\n"
            "\n"
            "[bad_xff]\n"
            "pattern = \\.bad1$\n"
            "xFilesFactor = 2.0\n"
            "aggregationMethod = average\n"
            "\n"
            "[valid2]\n"
            "pattern = \\.v2$\n"
            "xFilesFactor = 0.5\n"
            "aggregationMethod = sum\n"
            "\n"
            "[bad_agg]\n"
            "pattern = \\.bad2$\n"
            "xFilesFactor = 0.5\n"
            "aggregationMethod = median\n"
        )
        with patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', self._tmpfile.name):
            result = loadAggregationSchemas()
        # 2 valid + 1 default = 3
        self.assertEqual(len(result), 3)
        pattern_schemas = [s for s in result if isinstance(s, PatternSchema)]
        names = [s.name for s in pattern_schemas]
        self.assertIn('valid1', names)
        self.assertIn('valid2', names)
        self.assertNotIn('bad_xff', names)
        self.assertNotIn('bad_agg', names)

    def test_all_invalid_sections_returns_only_default(self):
        """When every section is invalid, the result contains only the default."""
        from carbon.storage import loadAggregationSchemas, defaultAggregation, PatternSchema
        self._write_config(
            "[bad1]\n"
            "pattern = \\.bad1$\n"
            "xFilesFactor = 5.0\n"
            "aggregationMethod = average\n"
            "\n"
            "[bad2]\n"
            "pattern = \\.bad2$\n"
            "xFilesFactor = 0.5\n"
            "aggregationMethod = totally_bogus\n"
        )
        with patch('carbon.storage.STORAGE_AGGREGATION_CONFIG', self._tmpfile.name):
            result = loadAggregationSchemas()
        pattern_schemas = [s for s in result if isinstance(s, PatternSchema)]
        self.assertEqual(len(pattern_schemas), 0)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0], defaultAggregation)
