import os
from unittest import TestCase

import mock

from carbon.tests.util import TestSettings


def _patched_settings():
  """A TestSettings pointed at the test conf-directory.

  Settings() preloads the defaults (rate limits, tag queue sizes, ...) that
  carbon.writer reads at import time; we only add the CONF_DIR / LOCAL_DATA_DIR
  the schema loaders need.
  """
  test_directory = os.path.dirname(os.path.realpath(__file__))
  settings = TestSettings()
  settings['CONF_DIR'] = os.path.join(test_directory, 'data', 'conf-directory')
  settings['LOCAL_DATA_DIR'] = ''
  return settings


def _fake_schema(name, matches):
  """A stand-in schema whose matches() is a Mock so call counts are observable."""
  schema = mock.Mock()
  schema.name = name
  schema.matches = mock.Mock(return_value=matches)
  return schema


class _WriterTestCase(TestCase):
  """Patches settings/database, then imports carbon.writer (and the storage
  classes) under the patch so their import-time schema load is safe and does not
  require the optional whisper/ceres backends (state.database stays None, so
  loadStorageSchemas skips backend validation)."""

  def setUp(self):
    settings = _patched_settings()
    self._settings_patch = mock.patch('carbon.conf.settings', settings)
    self._settings_patch.start()
    self._database_patch = mock.patch('carbon.state.database', new=None)
    self._database_patch.start()

    import carbon.writer as writer
    from carbon.storage import PatternSchema, DefaultSchema, Archive
    self.writer = writer
    self.SchemaMatchCache = writer.SchemaMatchCache
    self.PatternSchema = PatternSchema
    self.DefaultSchema = DefaultSchema
    self.Archive = Archive

  def tearDown(self):
    self._database_patch.stop()
    self._settings_patch.stop()


class SchemaMatchCacheTest(_WriterTestCase):

  def test_match_returns_first_matching_schema(self):
    carbon_schema = self.PatternSchema('carbon', r'^carbon\.', [self.Archive.fromString('60:90d')])
    default = self.DefaultSchema('default', [self.Archive.fromString('60s:1d')])
    schemas = [carbon_schema, default]
    cache = self.SchemaMatchCache()

    self.assertIs(cache.match(schemas, 'carbon.agents.foo'), carbon_schema)

  def test_match_falls_back_to_default(self):
    carbon_schema = self.PatternSchema('carbon', r'^carbon\.', [self.Archive.fromString('60:90d')])
    default = self.DefaultSchema('default', [self.Archive.fromString('60s:1d')])
    schemas = [carbon_schema, default]
    cache = self.SchemaMatchCache()

    # Does not match the pattern schema, so the appended default wins.
    self.assertIs(cache.match(schemas, 'some.other.metric'), default)

  def test_match_honors_first_match_order(self):
    first = self.PatternSchema('first', r'foo', [self.Archive.fromString('60s:1d')])
    second = self.PatternSchema('second', r'foo', [self.Archive.fromString('60s:2d')])
    cache = self.SchemaMatchCache()

    self.assertIs(cache.match([first, second], 'a.foo.b'), first)

  def test_match_returns_none_when_nothing_matches(self):
    # No default in the list: mirrors the writer's "no storage schema matched" guard.
    schemas = [self.PatternSchema('carbon', r'^carbon\.', [self.Archive.fromString('60s:1d')])]
    cache = self.SchemaMatchCache()

    self.assertIsNone(cache.match(schemas, 'not.carbon.metric'))

  def test_match_memoizes_positive_result(self):
    schema = _fake_schema('x', matches=True)
    schemas = [schema]
    cache = self.SchemaMatchCache()

    self.assertIs(cache.match(schemas, 'metric'), schema)
    self.assertIs(cache.match(schemas, 'metric'), schema)
    # Second lookup served from cache: matches() ran only once.
    self.assertEqual(schema.matches.call_count, 1)

  def test_match_memoizes_negative_result(self):
    schema = _fake_schema('x', matches=False)
    schemas = [schema]
    cache = self.SchemaMatchCache()

    self.assertIsNone(cache.match(schemas, 'metric'))
    self.assertIsNone(cache.match(schemas, 'metric'))
    # A cached "no match" is honored without rescanning.
    self.assertEqual(schema.matches.call_count, 1)

  def test_match_rebuilds_when_schema_list_identity_changes(self):
    schema_a = _fake_schema('a', matches=True)
    schema_b = _fake_schema('b', matches=True)
    list_a = [schema_a]
    list_b = [schema_b]
    cache = self.SchemaMatchCache()

    self.assertIs(cache.match(list_a, 'metric'), schema_a)
    # A different list object (as a reload produces) invalidates and recomputes.
    self.assertIs(cache.match(list_b, 'metric'), schema_b)
    self.assertEqual(schema_a.matches.call_count, 1)
    self.assertEqual(schema_b.matches.call_count, 1)

  def test_invalidate_forces_recompute(self):
    schema = _fake_schema('x', matches=True)
    schemas = [schema]
    cache = self.SchemaMatchCache()

    cache.match(schemas, 'metric')
    cache.invalidate()
    cache.match(schemas, 'metric')
    # After invalidation the same list is rescanned.
    self.assertEqual(schema.matches.call_count, 2)


class ReloadInvalidationTest(_WriterTestCase):

  def tearDown(self):
    # Leave the shared module-level caches clean for any later importer.
    self.writer.STORAGE_SCHEMA_CACHE.invalidate()
    self.writer.AGGREGATION_SCHEMA_CACHE.invalidate()
    super(ReloadInvalidationTest, self).tearDown()

  def _storage_schema(self, name, days):
    return self.PatternSchema(name, r'.*', [self.Archive.fromString('60s:%dd' % days)])

  # --- storage ---

  def test_reloadStorageSchemas_success_swaps_and_serves_new_result(self):
    writer = self.writer
    schema_a = self._storage_schema('a', 1)
    list_a = [schema_a]
    list_b = [self._storage_schema('b', 2)]

    with mock.patch('carbon.writer.loadStorageSchemas', return_value=list_a):
      writer.reloadStorageSchemas()
    self.assertIs(writer.SCHEMAS, list_a)
    self.assertIs(writer.STORAGE_SCHEMA_CACHE.match(writer.SCHEMAS, 'metric'), schema_a)

    with mock.patch('carbon.writer.loadStorageSchemas', return_value=list_b):
      writer.reloadStorageSchemas()
    self.assertIs(writer.SCHEMAS, list_b)
    # The previously cached schema_a must not be served against the new list.
    self.assertIs(writer.STORAGE_SCHEMA_CACHE.match(writer.SCHEMAS, 'metric'), list_b[0])

  def test_reloadStorageSchemas_success_eagerly_invalidates(self):
    writer = self.writer
    with mock.patch('carbon.writer.loadStorageSchemas', return_value=[self._storage_schema('a', 1)]):
      writer.reloadStorageSchemas()
    writer.STORAGE_SCHEMA_CACHE.match(writer.SCHEMAS, 'metric')  # populate
    self.assertNotEqual(writer.STORAGE_SCHEMA_CACHE._matches, {})

    with mock.patch('carbon.writer.loadStorageSchemas', return_value=[self._storage_schema('a', 1)]):
      writer.reloadStorageSchemas()
    # Cleared immediately on success, before any further match() call.
    self.assertEqual(writer.STORAGE_SCHEMA_CACHE._matches, {})
    self.assertIsNone(writer.STORAGE_SCHEMA_CACHE._schemas)

  def test_reloadStorageSchemas_failure_preserves_cache_and_global(self):
    writer = self.writer
    list_a = [self._storage_schema('a', 1)]
    with mock.patch('carbon.writer.loadStorageSchemas', return_value=list_a):
      writer.reloadStorageSchemas()
    writer.STORAGE_SCHEMA_CACHE.match(writer.SCHEMAS, 'metric')

    with mock.patch('carbon.writer.loadStorageSchemas', side_effect=Exception('boom')):
      writer.reloadStorageSchemas()  # swallowed; must not invalidate

    self.assertIs(writer.SCHEMAS, list_a)                          # global unchanged
    self.assertIs(writer.STORAGE_SCHEMA_CACHE._schemas, list_a)     # cache still bound
    self.assertIn('metric', writer.STORAGE_SCHEMA_CACHE._matches)   # entry survived

  # --- aggregation ---

  def test_reloadAggregationSchemas_success_swaps_and_serves_new_result(self):
    writer = self.writer
    schema_a = self.PatternSchema('a', r'.*', (0.5, 'average'))
    schema_b = self.PatternSchema('b', r'.*', (0.1, 'min'))
    list_a = [schema_a]
    list_b = [schema_b]

    with mock.patch('carbon.writer.loadAggregationSchemas', return_value=list_a):
      writer.reloadAggregationSchemas()
    self.assertIs(writer.AGGREGATION_SCHEMAS, list_a)
    self.assertIs(writer.AGGREGATION_SCHEMA_CACHE.match(writer.AGGREGATION_SCHEMAS, 'metric'),
                  schema_a)

    with mock.patch('carbon.writer.loadAggregationSchemas', return_value=list_b):
      writer.reloadAggregationSchemas()
    self.assertIs(writer.AGGREGATION_SCHEMAS, list_b)
    self.assertIs(writer.AGGREGATION_SCHEMA_CACHE.match(writer.AGGREGATION_SCHEMAS, 'metric'),
                  schema_b)

  def test_reloadAggregationSchemas_failure_preserves_cache_and_global(self):
    writer = self.writer
    list_a = [self.PatternSchema('a', r'.*', (0.5, 'average'))]
    with mock.patch('carbon.writer.loadAggregationSchemas', return_value=list_a):
      writer.reloadAggregationSchemas()
    writer.AGGREGATION_SCHEMA_CACHE.match(writer.AGGREGATION_SCHEMAS, 'metric')

    with mock.patch('carbon.writer.loadAggregationSchemas', side_effect=Exception('boom')):
      writer.reloadAggregationSchemas()

    self.assertIs(writer.AGGREGATION_SCHEMAS, list_a)
    self.assertIs(writer.AGGREGATION_SCHEMA_CACHE._schemas, list_a)
    self.assertIn('metric', writer.AGGREGATION_SCHEMA_CACHE._matches)
