import os
from unittest import TestCase
from mock import patch


class SchemaCacheUnitTest(TestCase):
    """Unit tests for _SchemaCache that don't require config files."""

    def setUp(self):
        # carbon.writer imports carbon.storage which reads settings.CONF_DIR
        # at module level, so we need to ensure it is set before first import.
        test_directory = os.path.dirname(os.path.realpath(__file__))
        from carbon.tests.util import TestSettings
        from carbon.database import WhisperDatabase
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

    def _get_SchemaCache(self):
        from carbon.writer import _SchemaCache
        return _SchemaCache

    def _make_schemas(self):
        from carbon.storage import PatternSchema, DefaultSchema, Archive
        s1 = PatternSchema('specific', r'^carbon\.', [Archive(60, 1440)])
        s2 = PatternSchema('broad', r'.*', [Archive(60, 10080)])
        default = DefaultSchema('default', [Archive(60, 10080)])
        return [s1, s2, default]

    def test_match_returns_first_matching_schema(self):
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)
        # 'carbon.foo' matches s1 first (not s2 or default)
        result = cache.match('carbon.foo')
        self.assertIs(result, schemas[0])

    def test_match_falls_through_to_broader_schema(self):
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)
        # 'servers.bar' doesn't match s1, matches s2
        result = cache.match('servers.bar')
        self.assertIs(result, schemas[1])

    def test_match_caches_result(self):
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)

        metric = 'carbon.test.metric'
        result1 = cache.match(metric)
        result2 = cache.match(metric)
        self.assertIs(result1, result2)
        self.assertIn(metric, cache._cache)
        self.assertIs(cache._cache[metric], result1)

    def test_match_preserves_first_match_semantics(self):
        """A metric matching multiple schemas always returns the first."""
        SchemaCache = self._get_SchemaCache()
        from carbon.storage import PatternSchema, Archive
        s1 = PatternSchema('first', r'.*', [Archive(60, 60)])
        s2 = PatternSchema('second', r'.*', [Archive(120, 60)])
        cache = SchemaCache([s1, s2])

        self.assertIs(cache.match('anything'), s1)
        self.assertIs(cache.match('anything'), s1)  # cached

    def test_default_schema_always_matches(self):
        SchemaCache = self._get_SchemaCache()
        from carbon.storage import DefaultSchema, Archive
        default = DefaultSchema('default', [Archive(60, 10080)])
        cache = SchemaCache([default])
        result = cache.match('completely.arbitrary.metric.name')
        self.assertIs(result, default)

    def test_invalidate_clears_all_cached_entries(self):
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)

        cache.match('carbon.foo')
        cache.match('servers.bar')
        cache.match('other.baz')
        self.assertEqual(len(cache._cache), 3)

        cache.invalidate()
        self.assertEqual(len(cache._cache), 0)

    def test_invalidate_causes_rescan(self):
        """After invalidate, schemas are re-evaluated (same list)."""
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)

        cache.match('carbon.foo')
        cache.invalidate()
        result = cache.match('carbon.foo')
        # Same schema list => same result, but cache was repopulated
        self.assertIs(result, schemas[0])
        self.assertIn('carbon.foo', cache._cache)

    def test_empty_schema_list_returns_none(self):
        SchemaCache = self._get_SchemaCache()
        cache = SchemaCache([])
        self.assertIsNone(cache.match('anything'))

    def test_schema_list_is_copied(self):
        """Mutating the original list must not affect the cache."""
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)
        from carbon.storage import PatternSchema, Archive
        schemas.append(PatternSchema('extra', r'^extra\.', [Archive(60, 60)]))
        # The cache should not see the extra schema
        self.assertEqual(len(cache.schemas), 3)

    def test_different_metrics_cached_independently(self):
        SchemaCache = self._get_SchemaCache()
        schemas = self._make_schemas()
        cache = SchemaCache(schemas)

        r1 = cache.match('carbon.a')
        r2 = cache.match('servers.b')
        r3 = cache.match('carbon.c')

        self.assertIs(r1, schemas[0])  # matches 'specific'
        self.assertIs(r2, schemas[1])  # matches 'broad'
        self.assertIs(r3, schemas[0])  # matches 'specific'
        self.assertEqual(len(cache._cache), 3)


class SchemaCacheIntegrationTest(TestCase):
    """Integration tests that exercise writer.py reload functions."""

    def setUp(self):
        test_directory = os.path.dirname(os.path.realpath(__file__))
        from carbon.tests.util import TestSettings
        from carbon.database import WhisperDatabase
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

    def test_SCHEMAS_is_schema_cache_instance(self):
        from carbon.writer import SCHEMAS, _SchemaCache
        self.assertIsInstance(SCHEMAS, _SchemaCache)
        self.assertGreater(len(SCHEMAS.schemas), 0)

    def test_AGGREGATION_SCHEMAS_is_schema_cache_instance(self):
        from carbon.writer import AGGREGATION_SCHEMAS, _SchemaCache
        self.assertIsInstance(AGGREGATION_SCHEMAS, _SchemaCache)
        self.assertGreater(len(AGGREGATION_SCHEMAS.schemas), 0)

    def test_SCHEMAS_match_uses_loaded_schemas(self):
        from carbon.writer import SCHEMAS
        result = SCHEMAS.match('carbon.agents.hostname.metricsReceived')
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'carbon')

    def test_SCHEMAS_match_default_fallback(self):
        from carbon.writer import SCHEMAS
        result = SCHEMAS.match('completely.unknown.metric')
        # The catch-all pattern (.*) in the test config always matches,
        # so a schema is always returned (either a config rule or the
        # built-in DefaultSchema appended last by loadStorageSchemas).
        self.assertIsNotNone(result)

    def test_reloadStorageSchemas_creates_fresh_cache(self):
        from carbon import writer
        old_schemas = writer.SCHEMAS
        # Populate cache with a match
        old_schemas.match('carbon.test')
        self.assertGreater(len(old_schemas._cache), 0)

        writer.reloadStorageSchemas()

        # After reload, SCHEMAS is a new instance with an empty cache
        self.assertIsNot(writer.SCHEMAS, old_schemas)
        self.assertEqual(len(writer.SCHEMAS._cache), 0)
        # Schemas themselves should still be loaded
        self.assertGreater(len(writer.SCHEMAS.schemas), 0)

    def test_reloadAggregationSchemas_creates_fresh_cache(self):
        from carbon import writer
        old_agg = writer.AGGREGATION_SCHEMAS
        old_agg.match('some.metric')
        self.assertGreater(len(old_agg._cache), 0)

        writer.reloadAggregationSchemas()

        self.assertIsNot(writer.AGGREGATION_SCHEMAS, old_agg)
        self.assertEqual(len(writer.AGGREGATION_SCHEMAS._cache), 0)
        self.assertGreater(len(writer.AGGREGATION_SCHEMAS.schemas), 0)

    def test_reloadStorageSchemas_failure_preserves_old_cache(self):
        """If load fails, the old SCHEMAS (and its cache) stays in place."""
        from carbon import writer
        old_schemas = writer.SCHEMAS
        old_schemas.match('carbon.test')
        cache_before = old_schemas._cache.copy()

        with patch('carbon.writer.loadStorageSchemas', side_effect=Exception("bad config")):
            writer.reloadStorageSchemas()

        # SCHEMAS must not have changed
        self.assertIs(writer.SCHEMAS, old_schemas)
        self.assertEqual(writer.SCHEMAS._cache, cache_before)

    def test_reloadAggregationSchemas_failure_preserves_old_cache(self):
        from carbon import writer
        old_agg = writer.AGGREGATION_SCHEMAS
        old_agg.match('test.metric')
        cache_before = old_agg._cache.copy()

        with patch('carbon.writer.loadAggregationSchemas', side_effect=Exception("bad config")):
            writer.reloadAggregationSchemas()

        self.assertIs(writer.AGGREGATION_SCHEMAS, old_agg)
        self.assertEqual(writer.AGGREGATION_SCHEMAS._cache, cache_before)

    def test_reload_then_match_uses_new_schemas(self):
        """After reload, matching uses the freshly loaded schemas."""
        from carbon import writer

        writer.reloadStorageSchemas()
        result = writer.SCHEMAS.match('carbon.agents.host.memUsage')
        self.assertIsNotNone(result)
        # Should match the 'carbon' pattern from the test config
        self.assertEqual(result.name, 'carbon')

    def test_storage_and_aggregation_reload_are_independent(self):
        """Reloading one must not touch the other."""
        from carbon import writer
        old_schemas = writer.SCHEMAS
        old_agg = writer.AGGREGATION_SCHEMAS

        writer.reloadStorageSchemas()

        # Storage schemas replaced
        self.assertIsNot(writer.SCHEMAS, old_schemas)
        # Aggregation schemas untouched
        self.assertIs(writer.AGGREGATION_SCHEMAS, old_agg)
