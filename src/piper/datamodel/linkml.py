import hashlib
import importlib.util
import logging
import sys
from pathlib import Path

import requests
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


def get_local_git_sha(file_path: Path):
    """Calculates the Git-style SHA1 hash of a local file."""
    if not file_path.exists():
        return None

    with open(file_path, "rb") as f:
        data = f.read()
    # GitHub's blob SHA is sha1("blob " + length + "\0" + content)
    header = f"blob {len(data)}\0".encode("utf-8")
    return hashlib.sha1(header + data).hexdigest()


def sync_github_file(
    owner: str,
    repo: str,
    path: str,
    ref: str = "main",
    local_filepath: Path | None = None,
):
    """
    Downloads a file from GitHub only if it's missing or out of date.
    'ref' can be a branch name, tag, or specific commit SHA.
    """
    save_as = local_filepath if local_filepath else Path(path).stem
    api_url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}?ref={ref}"

    # 1. Fetch metadata from GitHub API
    response = requests.get(api_url)
    if response.status_code != 200:
        logging.error(f"Error fetching metadata: {response.json().get('message')}")
        return

    file_metadata = response.json()
    remote_sha = file_metadata["sha"]
    download_url = file_metadata["download_url"]

    # 2. Compare local SHA with remote SHA
    local_sha = get_local_git_sha(Path(save_as))

    if local_sha == remote_sha:
        logging.info(f"File '{save_as}' is already up to date (SHA: {remote_sha[:7]}).")
        return

    # 3. Download if different or missing
    logging.warn(f"Update required. Downloading '{path}' from {ref}...")
    file_data = requests.get(download_url)
    with open(save_as, "wb") as f:
        f.write(file_data.content)
    logging.warn("Download complete.")
    return save_as


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

        LinkMLModelLoader.staging_dir = Path(LinkMLModelLoader.staging_dir)
        if not LinkMLModelLoader.staging_dir.exists():
            logging.info(
                f"Creating model directory, '{LinkMLModelLoader.staging_dir.absolute()}'"
            )
            LinkMLModelLoader.staging_dir.mkdir(exist_ok=True, parents=True)

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
                local_filepath=self.model_file_path,
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
        # Load module from file
        module_name = self.model_file_path.stem
        spec = importlib.util.spec_from_file_location(module_name, self.model_file_path)
        module = importlib.util.module_from_spec(spec)

        # Add to sys.modules and execute
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        # Patch the module's tables
        if hasattr(module, "Base") and hasattr(module.Base, "metadata"):
            logging.info(f"Patching module: '{module_name}'")

            if self.schema_name:
                module.Base.metadata.schema = self.schema_name

            for table in list(module.Base.metadata.tables.values()):
                original_name = table.name
                new_name = self.table_prefix.format(original_name.lower())
                logging.info(f"  Patching table: '{original_name}' -> '{new_name}'")
                table.name = new_name
                if self.schema_name:
                    table.schema = self.schema_name
        else:
            logging.warning(f"Module '{module_name}' does not have Base.metadata")

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

    def stream(self, class_name, chunksize=1000):
        model = self.get_model(class_name)

        with self.create_session() as session:
            try:
                stmt = select(model).execution_options(stream_results=True)
                result = session.execute(stmt).yield_per(chunksize)

                for partition in result.partitions():
                    for row in partition:
                        yield row[0]
            except Exception as e:
                logging.error(f"Error streaming {class_name.__name__}: {e}.")
