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
        # Memorize
        key = "versions"
        if key not in cls._cached_values:
            cls._cached_values[key] = _shared.extract.get_versions_from_monarch()

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
            cls._cached_values[key] = _shared.extract.get_metadata_from_monarch(version)

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
        filename = "monarch-kg.tar.gz"
        url = self.metadata[filename]["url"]
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url)

    def output(self):
        return self.input()


class DecompressKnowledgeGraphFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")

    def requires(self):
        return FetchKnowledgeGraphFile(self.dirpath, self.version)

    def output(self):
        return {
            "nodes_file": luigi.LocalTarget(
                os.path.join(self.subdirpath, "monarch-kg_nodes.tsv")
            ),
            "edges_file": luigi.LocalTarget(
                os.path.join(self.subdirpath, "monarch-kg_edges.tsv")
            ),
        }

    def run(self):
        _shared.extract.extract_tar_gz(self.input().path)


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
            "csv_files": DecompressKnowledgeGraphFile(self.dirpath, self.version),
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
        self._remove_decompressed_input_files()

    def _insert_nodes(self, conn, cursor):
        nodes_filepath = self.input()["csv_files"]["nodes_file"].path
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        skipped_keys = ["id", "category"]
        with open(nodes_filepath, "r", newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            columns = next(reader)
            id_idx = columns.index("id")
            type_idx = columns.index("category")
            properties_name_idx = [
                (col, columns.index(col)) for col in columns if col not in skipped_keys
            ]
            batch = []
            for row in reader:
                node_id = row[id_idx]
                node_type = row[type_idx]
                node_properties = {
                    name: row[idx]
                    for name, idx in properties_name_idx
                    if is_informative_value(row[idx])
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
        edges_filepath = self.input()["csv_files"]["edges_file"].path
        sql_cmd = """
            INSERT INTO edges (source_id, target_id, type, properties)
            VALUES (?, ?, ?, ?)
        """
        skipped_keys = ["subject", "object", "predicate"]
        with open(edges_filepath, "r", newline="") as f:
            reader = csv.reader(f, delimiter="\t")
            columns = next(reader)
            source_id_idx = columns.index("subject")
            target_id_idx = columns.index("object")
            type_idx = columns.index("predicate")
            properties_name_idx = [
                (col, columns.index(col)) for col in columns if col not in skipped_keys
            ]
            batch = []
            for row in reader:
                source_id = row[source_id_idx]
                target_id = row[target_id_idx]
                edge_type = row[type_idx]
                edge_properties = {
                    name: row[idx]
                    for name, idx in properties_name_idx
                    if is_informative_value(row[idx])
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

    def _remove_decompressed_input_files(self):
        nodes_filepath = self.input()["csv_files"]["nodes_file"].path
        edges_filepath = self.input()["csv_files"]["edges_file"].path
        for filepath in (nodes_filepath, edges_filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass


class MonarchKg(_shared.base.Project):
    """Monarch Knowledge Graph (MonarchKG).

    References
    ----------
    - Publication: https://doi.org/10.1093/nar/gkad1082
    - Website: https://monarchinitiative.org
    - Code: https://github.com/monarch-initiative/monarch-ingest
    - Data: https://data.monarchinitiative.org/monarch-kg/index.html

    """

    _label = "monarchkg"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "biolink:Drug": "green",
        "biolink:ChemicalEntity": "green",
        "biolink:MolecularEntity": "green",
        "biolink:SmallMolecule": "green",
        "biolink:Gene": "blue",
        "biolink:Protein": "blue",
        "biolink:Disease": "red",
        "biolink:Pathway": "red",
        "biolink:BiologicalProcessOrActivity": "red",
    }
