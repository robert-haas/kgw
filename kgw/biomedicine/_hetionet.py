import csv
import os
import sqlite3

import luigi
import orjson

from .. import _shared
from .._shared.extract import is_informative_value


class MetadataFetcher:
    _cached_values = {}

    @classmethod
    def get_versions(cls):
        # Fixed list instead of dynamically fetching it because there is only one version
        return ["1.0"]

    @classmethod
    def get_metadata(cls, version):
        # Check
        versions = cls.get_versions()
        if version not in versions:
            raise ValueError(
                f'Version "{version}" is not valid.\nAvailable options: {versions}'
            )

        # Memorize
        key = f"metadata_{version}"
        if key not in cls._cached_values:
            cls._cached_values[key] = _shared.extract.get_metadata_from_hetionet()

        # Lookup
        metadata = cls._cached_values[key]
        return metadata


class FetchKnowledgeGraphFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")
        self.metadata = MetadataFetcher.get_metadata(self.version)

    def requires(self):
        filename = "hetionet-v1.0.json.bz2"
        url = self.metadata[filename]["url"]
        md5 = self.metadata[filename]["md5"]
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url, md5=md5)

    def output(self):
        return self.input()


class ExtractKnowledgeGraph(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")

    def requires(self):
        return FetchKnowledgeGraphFile(self.dirpath, self.version)

    def output(self):
        return luigi.LocalTarget(self.input().path[:-4])

    def run(self):
        _shared.extract.extract_bz2(self.input().path)


class CreateSqliteFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    batch_size = luigi.IntParameter(default=10_000, significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg.sqlite")

    def requires(self):
        tasks = {
            "subdirpath": _shared.tasks.CreateDirectory(self.subdirpath),
            "kg_file": ExtractKnowledgeGraph(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        with _shared.tasks.temp_output_path(self.filepath) as sqlite_filepath:
            with sqlite3.connect(sqlite_filepath) as conn:
                cursor = conn.cursor()
                _shared.transform.create_sql_schema(cursor)
                self._insert_nodes(conn, cursor)
                self._insert_edges(conn, cursor)
                conn.commit()
        self._remove_input_file()

    def _insert_nodes(self, conn, cursor):
        # Special aspects in handling the raw data
        # 1) Node property "id" is renamed to "identifier" to prevent an issue with GraphML.
        kg_filepath = self.input()["kg_file"].path
        data_kg = _shared.transform.read_json_file(kg_filepath)
        data_nodes = data_kg["nodes"]
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        batch = []
        for item in data_nodes:
            node_id = str(item["identifier"])
            node_type = str(item["kind"])
            node_properties = {
                "name": item["name"],
            }
            node_properties.update(item["data"])
            node_properties_str = orjson.dumps(node_properties).decode("utf-8")
            node = (node_id, node_type, node_properties_str)
            batch.append(node)
            if len(batch) >= self.batch_size:
                cursor.executemany(sql_cmd, batch)
                batch = []
        if batch:
            cursor.executemany(sql_cmd, batch)
        conn.commit()

    def _insert_edges(self, conn, cursor):
        kg_filepath = self.input()["kg_file"].path
        data_kg = _shared.transform.read_json_file(kg_filepath)
        data_edges = data_kg["edges"]
        sql_cmd = """
            INSERT INTO edges (source_id, target_id, type, properties)
            VALUES (?, ?, ?, ?)
        """
        batch = []
        for item in data_edges:
            source_id = str(item["source_id"][1])
            target_id = str(item["target_id"][1])
            edge_type = item["kind"]
            edge_properties = {
                "direction": item["direction"]
            }
            edge_properties.update(item["data"])
            edge_properties_str = orjson.dumps(edge_properties).decode("utf-8")
            edge = (source_id, target_id, edge_type, edge_properties_str)
            batch.append(edge)
            if len(batch) >= self.batch_size:
                cursor.executemany(sql_cmd, batch)
                batch = []
        if batch:
            cursor.executemany(sql_cmd, batch)
        conn.commit()

    def _remove_input_file(self):
        kg_filepath = self.input()["kg_file"].path
        try:
            os.remove(kg_filepath)
        except Exception:
            pass


class Hetionet(_shared.base.Project):
    """Hetionet.

    References
    ----------
    - Publication: https://doi.org/10.7554/elife.26726
    - Website: https://het.io
    - Code: https://github.com/hetio/hetionet
    - Data: https://het.io/explore/#download-dataset

    """

    _label = "hetionet"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "Compound": "green",
        "Gene": "blue",
        "Disease": "red",
    }
