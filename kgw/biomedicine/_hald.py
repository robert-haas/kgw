import os
import sqlite3

import luigi
import orjson

from .. import _shared
from .._shared.extract import is_informative_value


class MetadataFetcher:
    dataset_id = 22828196
    _cached_values = {}

    @classmethod
    def get_versions(cls):
        # Memorize
        key = "versions"
        if key not in cls._cached_values:
            cls._cached_values[key] = _shared.extract.get_versions_from_figshare(
                cls.dataset_id
            )

        # Lookup
        versions = cls._cached_values[key]
        return versions

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
            cls._cached_values[key] = _shared.extract.get_metadata_from_figshare(
                cls.dataset_id, version
            )

        # Lookup
        metadata = cls._cached_values[key]
        return metadata


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
            "nodes_file": FetchNodesFile(self.dirpath, self.version),
            "edges_file": FetchEdgesFile(self.dirpath, self.version),
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

    def _insert_nodes(self, conn, cursor):
        # Special aspects in handling the raw data
        # 1) The dict keys in the raw data can be ignored, because source ids, edge types,
        #    and target ids are encoded in a cleaner way in the dict values.
        nodes_filepath = self.input()["nodes_file"].path
        data_nodes = _shared.transform.read_json_file(nodes_filepath)
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        skipped_keys = ["entity", "type"]
        batch = []
        for entry in data_nodes.values():
            entry = entry[0]
            node_id = entry["entity"]
            node_type = entry["type"]
            node_properties = {
                key: val
                for key, val in entry.items()
                if key not in skipped_keys and is_informative_value(val)
            }
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
        # Special aspects in handling the raw data
        # 1) The dict keys in the raw data can be ignored, because source ids, edge types,
        #    and target ids are encoded in a cleaner way in the dict values.
        # 2) Some dict values are mostly redundant and are therefore skipped,
        #    e.g. source and source type are part of the node data.
        edges_filepath = self.input()["edges_file"].path
        data_edges = _shared.transform.read_json_file(edges_filepath)
        sql_cmd = """
            INSERT INTO edges (source_id, target_id, type, properties)
            VALUES (?, ?, ?, ?)
        """
        skipped_keys = [
            "source entity",
            "target entity",
            "relationship",
            "source",
            "target",
            "source type",
            "target type",
        ]
        batch = []
        for entry in data_edges.values():
            source_id = entry["source entity"]
            target_id = entry["target entity"]
            edge_type = entry["relationship"]
            edge_properties = {
                key: val
                for key, val in entry.items()
                if key not in skipped_keys and is_informative_value(val)
            }
            edge_properties_str = orjson.dumps(edge_properties).decode("utf-8")
            edge = (source_id, target_id, edge_type, edge_properties_str)
            batch.append(edge)
            if len(batch) >= self.batch_size:
                cursor.executemany(sql_cmd, batch)
                batch = []
        if batch:
            cursor.executemany(sql_cmd, batch)
        conn.commit()


class FetchNodesFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")
        self.metadata = MetadataFetcher.get_metadata(self.version)

    def requires(self):
        filename = "Entity_Info.json"
        url = self.metadata[filename]["url"]
        md5 = self.metadata[filename]["md5"]
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url, md5=md5)

    def output(self):
        return self.input()


class FetchEdgesFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")
        self.metadata = MetadataFetcher.get_metadata(self.version)

    def requires(self):
        filename = "Relation_Info.json"
        url = self.metadata[filename]["url"]
        md5 = self.metadata[filename]["md5"]
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url, md5=md5)

    def output(self):
        return self.input()


class Hald(_shared.base.Project):
    """Human Aging and Longevity Dataset (HALD).

    References
    ----------
    - Publication: https://doi.org/10.1038/s41597-023-02781-0
    - Website: https://bis.zju.edu.cn/hald
    - Code: https://github.com/zexuwu/hald
    - Data: https://doi.org/10.6084/m9.figshare.22828196

    """

    _label = "hald"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "Pharmaceutical Preparations": "green",
        "Toxin": "green",
        "Gene": "blue",
        "Peptide": "blue",
        "Protein": "blue",
        "RNA": "blue",
        "Disease": "red",
    }

    def to_schema(self):
        tasks = [
            _shared.tasks.CreateCompactSchemaFile(
                self.dirpath,
                self.version,
                self._node_type_to_color,
                self._CreateSqliteFile,
            ),
        ]
        self._tasks.extend(tasks)
