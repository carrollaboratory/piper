import duckdb
from pathlib import Path
from yaml import safe_load
import tempfile
from sqlalchemy import create_engine, select
from sqlalchemy.pool import StaticPool
import logging

class TestFixture:
    def __init__(self, datadir="tests/data", filename=None):
        """Sets up the configuration, but defers connection if needed,
        or prepares the object."""
        self.datadir = datadir
        self.conn = None
        self.tmpfile = False
        self.engine = None
        self.filename = filename

    def __enter__(self):
        """Opens the connection and initializes data when entering the 'with' block."""

        filename = self.filename
        if filename is None:
            import os
            filename = os.path.join(tempfile.gettempdir(), f"test_{os.urandom(8).hex()}.duckdb")
            self._tmp_db_path = filename  # track for cleanup
            self.tmpfile = True

        logging.debug(f"Creating database, '{filename}'")
        self.conn = duckdb.connect(filename)

        # Populate the database
        logging.debug(f"Loading fixtures from '{self.datadir}'")
        for file in Path(self.datadir).glob("*.yaml"):
            tablename = file.stem

            with file.open('rt') as f:
                fixtures = safe_load(f)
            logging.debug(f"Creating table, '{tablename}' from '{file}'")
            self.conn.execute(f"CREATE TABLE {tablename} AS SELECT d.* FROM (SELECT unnest($data) AS d) t", {"data": fixtures})

        self.conn.commit()
        self.dburi = f"duckdb:///{filename}"
        # self.engine = create_engine(self.dburi, poolclass=StaticPool)

        self.conn.close()

        return self


    def close(self):
        if self.engine:
            self.engine.dispose()
            self.engine = None

        # remove tmp file if exists
        if hasattr(self, '_tmp_db_path') and self._tmp_db_path:
            try:
                import os
                os.unlink(self._tmp_db_path)
            except FileNotFoundError:
                pass  # already gone, no problem
            self._tmp_db_path = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Safely closes the connection automatically when exiting the block."""
        self.close()

    def query(self, sql, *args):
        """Example query method."""
        return self.conn.execute(sql, *args).fetchall()
