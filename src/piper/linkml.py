import builtins
import hashlib
import importlib.util
import logging
import os
import sys
from pathlib import Path

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_local_git_sha(file_path):
    """Calculates the Git-style SHA1 hash of a local file."""
    if not os.path.exists(file_path):
        return None
    with open(file_path, "rb") as f:
        data = f.read()
    # GitHub's blob SHA is sha1("blob " + length + "\0" + content)
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()


def sync_github_file(owner, repo, path, ref="main", save_as=None):
    """
    Downloads a file from GitHub only if it's missing or out of date.
    'ref' can be a branch name, tag, or specific commit SHA.
    """
    save_as = save_as or os.path.basename(path)
    api_url = f"https://api.github.com{owner}/{repo}/contents/{path}?ref={ref}"

    # 1. Fetch metadata from GitHub API
    response = requests.get(api_url)
    if response.status_code != 200:
        logging.error(f"Error fetching metadata: {response.json().get('message')}")
        return

    file_metadata = response.json()
    remote_sha = file_metadata["sha"]
    download_url = file_metadata["download_url"]

    # 2. Compare local SHA with remote SHA
    local_sha = get_local_git_sha(save_as)

    if local_sha == remote_sha:
        logging.info(f"File '{save_as}' is already up to date (SHA: {remote_sha[:7]}).")
        return

    # 3. Download if different or missing
    logging.warn(f"Update required. Downloading '{path}' from {ref}...")
    file_data = requests.get(download_url)
    with open(save_as, "wb") as f:
        f.write(file_data.content)
    logging.warn("Download complete.")


# --- Example Usage ---
# Downloads 'README.md' from a specific tag only if it has changed
# sync_github_file("psf", "requests", "README.md", ref="v2.31.0")


class LinkMLModelLoader:
    """Helper class for loading and using LinkML-generated SQLAlchemy models with DBT tables."""

    staging_dir = "staging"

    def __init__(
        self,
        model_source,
        model_filename,
        database_url,
        table_prefix="tgt_",
        schema_name=None,
        source_ref="main",
    ):
        """
        Initialize the loader.

        Args:
            model_file_path: Path to Python file with SQLAlchemy models
            database_url: SQLAlchemy database URL
            table_prefix: DBT table prefix (default: 'tgt_')
            schema_name: Database schema (default: None)
        """
        self.github_repository = None
        if model_source is None:
            self.model_file_path = model_source
            assert Path(model_filename).exists(), (
                f"File not found: '{model_filename}' does not exist."
            )
        else:
            self.github_repository = model_source
            self.model_file_path = Path(LinkMLModelLoader.staging_dir) / model_filename

            gh_owner, gh_repo = model_source.split("/")
            sync_github_file(
                gh_owner,
                gh_repo,
                f"project/sqlalchemy/{model_filename}",
                ref=source_ref,
                save_as=self.model_file_path,
            )

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
