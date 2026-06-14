"""Pytest conftest for carbon tests.

Ensures settings are initialized before module-level imports that depend on them.
"""
import os
import sys

# Ensure lib is in path
lib_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
if lib_path not in sys.path:
    sys.path.insert(0, lib_path)

# Pre-seal settings keys that module-level code expects at import time.
# carbon.storage reads settings.CONF_DIR at module scope; set it to the
# test data directory so storage-schemas.conf can be loaded.
_conf_dir = os.path.join(os.path.dirname(__file__), 'data', 'conf-directory')
from carbon.conf import settings
settings.setdefault('CONF_DIR', _conf_dir)
