import builtins
import importlib.util
import sys
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


class LinkMLModelLoader:
    """Helper class for loading and using LinkML-generated SQLAlchemy models with DBT tables."""

    def __init__(
        self, model_file_path, database_url, table_prefix="tgt_", schema_name=None
    ):
        """
        Initialize the loader.

        Args:
            model_file_path: Path to Python file with SQLAlchemy models
            database_url: SQLAlchemy database URL
            table_prefix: DBT table prefix (default: 'tgt_')
            schema_name: Database schema (default: None)
        """
        self.model_file_path = Path(model_file_path)
        self.database_url = database_url
        self.table_prefix = table_prefix
        self.schema_name = schema_name
        self.module = None
        self.engine = None
        self.Session = None

    def load(self):
        """Load the models and set up database connection."""
        # Create engine
        self.engine = create_engine(self.database_url)

        # Patch and load module
        self.module = self._load_and_patch_module()

        # Create session factory
        self.Session = sessionmaker(bind=self.engine)

        return self

    def _load_and_patch_module(self):
        """Load the module and patch table names."""
        # Load module from file WITHOUT executing it yet
        module_name = self.model_file_path.stem
        spec = importlib.util.spec_from_file_location(module_name, self.model_file_path)
        module = importlib.util.module_from_spec(spec)

        # Add to sys.modules BEFORE executing
        sys.modules[module_name] = module

        # Set up import hook to catch ANY imports during module execution
        old_import = builtins.__import__
        patched_modules = set()

        def patching_import(name, *args, **kwargs):
            imported_module = old_import(name, *args, **kwargs)

            # Check if this module has SQLAlchemy Base and hasn't been patched yet
            if (
                hasattr(imported_module, "Base")
                and hasattr(imported_module.Base, "metadata")
                and id(imported_module) not in patched_modules
            ):
                print(f"Patching module: {name}")

                if self.schema_name:
                    imported_module.Base.metadata.schema = self.schema_name

                for table in list(imported_module.Base.metadata.tables.values()):
                    original_name = table.name
                    new_name = f"{self.table_prefix}{original_name.lower()}"
                    print(f"  Patching table: {original_name} -> {new_name}")
                    table.name = new_name
                    if self.schema_name:
                        table.schema = self.schema_name

                patched_modules.add(id(imported_module))

            return imported_module

        # Replace import temporarily
        builtins.__import__ = patching_import

        try:
            # NOW execute the module - this triggers all imports
            spec.loader.exec_module(module)

            # Also patch the main module itself if it has Base
            if hasattr(module, "Base") and hasattr(module.Base, "metadata"):
                print(f"Patching main module: {module_name}")

                if self.schema_name:
                    module.Base.metadata.schema = self.schema_name

                for table in list(module.Base.metadata.tables.values()):
                    original_name = table.name
                    new_name = f"{self.table_prefix}{original_name.lower()}"
                    print(f"  Patching table: {original_name} -> {new_name}")
                    table.name = new_name
                    if self.schema_name:
                        table.schema = self.schema_name
        finally:
            # Restore original import
            builtins.__import__ = old_import

        return module

    def get_model(self, class_name):
        """Get a specific model class by name."""
        if not self.module:
            raise RuntimeError("Models not loaded. Call load() first.")
        return getattr(self.module, class_name)

    def create_session(self):
        """Create a new database session."""
        if not self.Session:
            raise RuntimeError("Models not loaded. Call load() first.")
        return self.Session()
