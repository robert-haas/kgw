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
        # Fixed list instead of dynamically fetching it because:
        # 1) scraping Harvard Dataverse is prevented with a JavaScript challenge
        # 2) files did not change between versions 1.0, 2.0 and 2.1
        return ["2.1"]

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
            cls._cached_values[key] = _shared.extract.get_metadata_from_primekg()

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
        filename = "kg.csv.zip"
        url = self.metadata[filename]["url"]
        md5 = self.metadata[filename]["md5"]
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url, md5=md5)

    def output(self):
        return self.input()


class FetchAnnotationFiles(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")
        self.metadata = MetadataFetcher.get_metadata(self.version)
        self.filenames = [
            "disease_features.csv.zip",
            "drug_features.csv.zip",
        ]

    def requires(self):
        tasks = []
        for filename in self.filenames:
            url = self.metadata[filename]["url"]
            md5 = self.metadata[filename]["md5"]
            task = _shared.tasks.DownloadFile(self.subdirpath, filename, url, md5=md5)
            tasks.append(task)
        return tasks

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
        _shared.extract.extract_zip(self.input().path)


class ExtractAnnotations(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")

    def requires(self):
        return FetchAnnotationFiles(self.dirpath, self.version)

    def output(self):
        return [luigi.LocalTarget(target.path[:-4]) for target in self.input()]

    def run(self):
        for target in self.input():
            _shared.extract.extract_zip(target.path)


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
            "annotation_files": ExtractAnnotations(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        with _shared.tasks.temp_output_path(self.filepath) as sqlite_filepath:
            with sqlite3.connect(sqlite_filepath) as conn:
                cursor = conn.cursor()
                self._prepare_annotations()
                _shared.transform.create_sql_schema(cursor)
                self._insert_nodes(conn, cursor)
                self._insert_edges(conn, cursor)
                conn.commit()
        self._remove_input_csv_files()

    def _prepare_annotations(self):
        node_id_to_annotation_map = {}
        for target in self.input()["annotation_files"]:
            filepath = target.path

            with open(filepath, newline="") as f:
                reader = csv.reader(f, delimiter=",")
                header = next(reader)
                data_columns = header[1:]
                for row in reader:
                    node_id = row[0]
                    annotation = {}
                    for i, col in enumerate(data_columns, 1):
                        value = row[i]
                        if is_informative_value(value):
                            annotation[col] = value
                    node_id_to_annotation_map[node_id] = annotation
        self.node_id_to_annotation_map = node_id_to_annotation_map

    def _insert_nodes(self, conn, cursor):
        # Special aspects in handling the raw data
        # 1) Node property "id" is renamed to "identifier" to prevent an issue with GraphML.
        kg_filepath = self.input()["kg_file"].path
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        seen_node_ids = set()
        with open(kg_filepath, "r", newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            columns = next(reader)
            x_index_idx = columns.index("x_index")
            x_id_idx = columns.index("x_id")
            x_type_idx = columns.index("x_type")
            x_name_idx = columns.index("x_name")
            x_source_idx = columns.index("x_source")
            y_index_idx = columns.index("y_index")
            y_id_idx = columns.index("y_id")
            y_type_idx = columns.index("y_type")
            y_name_idx = columns.index("y_name")
            y_source_idx = columns.index("y_source")
            batch = []
            for row in reader:
                x_index = row[x_index_idx]
                x_id = row[x_id_idx]
                x_type = row[x_type_idx]
                x_name = row[x_name_idx]
                x_source = row[x_source_idx]
                y_index = row[y_index_idx]
                y_id = row[y_id_idx]
                y_type = row[y_type_idx]
                y_name = row[y_name_idx]
                y_source = row[y_source_idx]
                # x
                node_id = x_index
                if node_id not in seen_node_ids:
                    seen_node_ids.add(node_id)
                    node_type = x_type
                    node_properties = {
                        "identifier": x_id,
                        "name": x_name,
                        "source": x_source,
                    }
                    if node_id in self.node_id_to_annotation_map:
                        node_properties.update(self.node_id_to_annotation_map[node_id])
                    node_properties_str = orjson.dumps(node_properties).decode("utf-8")
                    node = (node_id, node_type, node_properties_str)
                    batch.append(node)
                # y
                node_id = y_index
                if node_id not in seen_node_ids:
                    seen_node_ids.add(node_id)
                    node_type = y_type
                    node_properties = {
                        "identifier": y_id,
                        "name": y_name,
                        "source": y_source,
                    }
                    node_properties = {
                        key: val
                        for key, val in node_properties.items()
                        if is_informative_value(val)
                    }
                    if node_id in self.node_id_to_annotation_map:
                        node_properties.update(self.node_id_to_annotation_map[node_id])
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
        sql_cmd = """
            INSERT INTO edges (source_id, target_id, type, properties)
            VALUES (?, ?, ?, ?)
        """
        with open(kg_filepath, "r", newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter=",")
            columns = next(reader)
            relation_idx = columns.index("relation")
            display_relation_idx = columns.index("display_relation")
            x_index_idx = columns.index("x_index")
            y_index_idx = columns.index("y_index")
            batch = []
            for row in reader:
                source_id = row[x_index_idx]
                target_id = row[y_index_idx]
                edge_type = row[relation_idx]
                edge_properties = {"display_relation": row[display_relation_idx]}
                edge_properties = {
                    key: val
                    for key, val in edge_properties.items()
                    if is_informative_value(val)
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

    def _remove_input_csv_files(self):
        kg_filepath = self.input()["kg_file"].path
        annotation_filepaths = [
            target.path for target in self.input()["annotation_files"]
        ]
        all_filepaths = [kg_filepath] + annotation_filepaths
        for filepath in all_filepaths:
            try:
                os.remove(filepath)
            except Exception:
                pass


class PrimeKg(_shared.base.Project):
    """Precision Medicine Knowledge Graph (PrimeKG).

    References
    ----------
    - Publication: https://doi.org/10.1038/s41597-023-01960-3
    - Website: https://zitniklab.hms.harvard.edu/projects/PrimeKG
    - Code: https://github.com/mims-harvard/PrimeKG
    - Data: https://doi.org/10.7910/DVN/IXA7BM

    """

    _label = "primekg"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "drug": "green",
        "gene/protein": "blue",
        "disease": "red",
        "pathway": "red",
        "biological_process": "red",
    }
