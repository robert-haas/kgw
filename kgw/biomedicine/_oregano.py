import csv
import os
import sqlite3

import luigi
import orjson

from .. import _shared
from .._shared.extract import is_informative_value


class MetadataFetcher:
    dataset_id = 23553114
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


class FetchKnowledgeGraphFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")
        self.metadata = MetadataFetcher.get_metadata(self.version)

    def _detect_filename(self):
        # The filename is slightly different for different versions, hence it has to be detected
        for candidate in self.metadata:
            if candidate.startswith("OREGANO") and candidate.endswith(".tsv"):
                return candidate
        raise ValueError(
            "Could not identify the TSV file that contains Oregano's knowledge graph."
        )

    def requires(self):
        filename = self._detect_filename()
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
            "ACTIVITY.tsv",
            "COMPOUND.tsv",
            "DISEASES.tsv",
            "EFFECT.tsv",
            "GENES.tsv",
            "INDICATION.tsv",
            "PATHWAYS.tsv",
            "PHENOTYPES.tsv",
            "SIDE_EFFECT.tsv",
            "TARGET.tsv",
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
            "kg_file": FetchKnowledgeGraphFile(self.dirpath, self.version),
            "annotation_files": FetchAnnotationFiles(self.dirpath, self.version),
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

    @staticmethod
    def node_name_to_type(node_name):
        try:
            # Most node names contain both the type and id of a node separated
            # by a ":" character.
            node_type, _ = node_name.split(":", 1)
            node_type = node_type.lower()
        except Exception:
            # Node names that occur in "has_code" relations as target do not
            # have a ":" and explicit type.
            # Here the type "code" is assigned to such nodes to be consistent.
            node_type = "code"
        return node_type

    def _prepare_annotations(self):
        targets = self.input()["annotation_files"]

        def strip_if_str(val):
            if isinstance(val, str):
                val = val.strip()
            return val

        node_name_to_annotation_map = {}
        for target in targets:
            filepath = target.path
            with open(filepath, newline="") as f:
                reader = csv.reader(f, delimiter="\t")
                columns = next(reader)
                data_columns = columns[1:]
                for row in reader:
                    node_name = row[0]
                    annotation = {}
                    for i, col in enumerate(data_columns, 1):
                        value = strip_if_str(row[i])
                        if is_informative_value(value):
                            annotation[strip_if_str(col)] = value
                    node_name_to_annotation_map[node_name] = annotation
        self.node_name_to_annotation_map = node_name_to_annotation_map

    def _insert_nodes(self, conn, cursor):
        kg_filepath = self.input()["kg_file"].path
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        with open(kg_filepath, "r", newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter="\t")
            seen_nodes = set()
            batch = []
            for row in reader:
                subject, _, object = row
                for node_name in (subject, object):
                    if node_name not in seen_nodes:  # skip duplicates
                        seen_nodes.add(node_name)
                        node_id = node_name
                        node_type = self.node_name_to_type(node_name)
                        node_properties = self.node_name_to_annotation_map.get(
                            node_id, {}
                        )
                        node_properties_str = orjson.dumps(node_properties).decode(
                            "utf-8"
                        )
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
        # 1) This code skips redundant edges, i.e. repeated (subject, predicte, object) triples,
        #    because they do not differ in any other way as well.
        # 2) There are no edge properties in this knowledge graph
        kg_filepath = self.input()["kg_file"].path
        sql_cmd = """
            INSERT INTO edges (source_id, target_id, type, properties)
            VALUES (?, ?, ?, ?)
        """
        with open(kg_filepath, "r", newline="") as csvfile:
            reader = csv.reader(csvfile, delimiter="\t")
            seen_triples = set()
            batch = []
            for row in reader:
                subject, predicate, object = row
                source_id = subject
                target_id = object
                edge_type = predicate
                edge_properties = {}  # no edge properties in raw data
                edge_properties_str = orjson.dumps(edge_properties).decode("utf-8")
                triple = (source_id, edge_type, target_id)
                if triple not in seen_triples:  # skip duplicates
                    seen_triples.add(triple)
                    edge = (source_id, target_id, edge_type, edge_properties_str)
                    batch.append(edge)
                    if len(batch) >= self.batch_size:
                        cursor.executemany(sql_cmd, batch)
                        batch = []
            if batch:
                cursor.executemany(sql_cmd, batch)
        conn.commit()


class Oregano(_shared.base.Project):
    """Oregano Knowledge Graph.

    References
    ----------
    - Publication: https://doi.org/10.1038/s41597-023-02757-0
    - Code: https://gitub.u-bordeaux.fr/erias/oregano
    - Data: https://doi.org/10.6084/m9.figshare.23553114

    """

    _label = "oregano"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "compound": "green",
        "molecule": "green",
        "gene": "blue",
        "protein": "blue",
        "disease": "red",
        "pathway": "red",
    }
