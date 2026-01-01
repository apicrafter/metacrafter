"""Core module for Metacrafter."""

# Import CrafterCmd from the parent core.py module file
# Since Python treats metacrafter.core as a package (due to this directory),
# we need to import from the core.py file directly
import os
import importlib.util

# Get the path to the parent core.py file
_parent_dir = os.path.dirname(os.path.dirname(__file__))
_core_py_path = os.path.join(_parent_dir, 'core.py')

if os.path.exists(_core_py_path):
    # Load core.py as a module
    spec = importlib.util.spec_from_file_location("metacrafter_core_module", _core_py_path)
    if spec and spec.loader:
        _core_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(_core_module)
        # Export CrafterCmd and other constants
        CrafterCmd = getattr(_core_module, 'CrafterCmd', None)
        if CrafterCmd is None:
            raise ImportError("CrafterCmd not found in core.py")
        # Export app (typer.Typer instance)
        app = getattr(_core_module, 'app', None)
        if app is None:
            raise ImportError("app not found in core.py")
        # Export constants that may be imported by other modules
        SUPPORTED_FILE_TYPES = getattr(_core_module, 'SUPPORTED_FILE_TYPES', None)
        CODECS = getattr(_core_module, 'CODECS', None)
        BINARY_DATA_FORMATS = getattr(_core_module, 'BINARY_DATA_FORMATS', None)
        DEFAULT_BATCH_SIZE = getattr(_core_module, 'DEFAULT_BATCH_SIZE', None)
        DEFAULT_JSON_INDENT = getattr(_core_module, 'DEFAULT_JSON_INDENT', None)
        DEFAULT_RETRY_DELAY = getattr(_core_module, 'DEFAULT_RETRY_DELAY', None)
        DEFAULT_TABLE_FORMAT = getattr(_core_module, 'DEFAULT_TABLE_FORMAT', None)
        # Export helper functions
        _split_option_list = getattr(_core_module, '_split_option_list', None)
        _resolve_output_target = getattr(_core_module, '_resolve_output_target', None)
        # Export Typer app instances
        rules_app = getattr(_core_module, 'rules_app', None)
        scan_app = getattr(_core_module, 'scan_app', None)
        server_app = getattr(_core_module, 'server_app', None)
        export_app = getattr(_core_module, 'export_app', None)
        __all__ = [
            'CrafterCmd', 'app', 'SUPPORTED_FILE_TYPES', 'CODECS', 'BINARY_DATA_FORMATS',
            'DEFAULT_BATCH_SIZE', 'DEFAULT_JSON_INDENT', 'DEFAULT_RETRY_DELAY', 'DEFAULT_TABLE_FORMAT',
            '_split_option_list', '_resolve_output_target',
            'rules_app', 'scan_app', 'server_app', 'export_app'
        ]
    else:
        raise ImportError("Could not load core.py module")
else:
    raise ImportError(f"core.py not found at {_core_py_path}")
