import csv
import os
import sqlite3
import sys

import luigi
import orjson

from .. import _shared
from .._shared.extract import is_informative_value


class MetadataFetcher:
    _cached_values = {}

    @classmethod
    def get_versions(cls):
        # Returns a fixed list instead of dynamically fetching the information because
        # - there are only two versions
        # - if a new version is published, there may again be a format change
        return ["1.0", "2.0"]

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
            cls._cached_values[key] = _shared.extract.get_metadata_from_pharmebinet(
                version
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
        # The filename varies between different versions, therefore it has to be detected
        return list(self.metadata.keys())[0]

    def requires(self):
        filename = self._detect_filename()
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
            "nodes_file": luigi.LocalTarget(os.path.join(self.subdirpath, "nodes.tsv")),
            "edges_file": luigi.LocalTarget(os.path.join(self.subdirpath, "edges.tsv")),
        }

    def run(self):
        filepath = self.input().path

        # The archive format varies between different versions, therefore conditional processing
        if filepath.endswith("tar.gz"):
            _shared.extract.extract_tar_gz(filepath)
        elif filepath.endswith("zip"):
            # Extract
            _shared.extract.extract_zip(filepath)

            # Move files from resulting directory into the base directory
            base_dirpath = os.path.dirname(filepath)
            new_dirname = "tsv_files"
            new_dirpath = os.path.join(base_dirpath, new_dirname)
            for filename in ("nodes.tsv", "edges.tsv"):
                source = os.path.join(new_dirpath, filename)
                target = os.path.join(base_dirpath, filename)
                _shared.extract.move_file(source, target)

            # Delete the empty directory
            _shared.extract.delete_empty_directory(new_dirpath)
        else:
            msg = (
                "Unexpected file format when trying to decompress "
                f"the knowledge graph: {filepath}"
            )
            raise ValueError(msg)


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
            "tsv_files": DecompressKnowledgeGraphFile(self.dirpath, self.version),
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
        nodes_filepath = self.input()["tsv_files"]["nodes_file"].path
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        with open(nodes_filepath, "r", newline="") as f:
            csv.field_size_limit(sys.maxsize)
            reader = csv.reader(f, delimiter="\t")
            columns = next(reader)
            id_idx = columns.index("node_id")
            type_idx = columns.index("labels")
            properties_str_idx = columns.index("properties")
            other_properties_name_idx = [
                (col, columns.index(col))
                for col in (
                    "name",
                    "identifier",
                    "resource",
                    "license",
                    "source",
                    "url",
                )
            ]
            batch = []
            for row in reader:
                node_id = row[id_idx]
                node_type = row[type_idx]

                node_properties_str = row[properties_str_idx]
                node_properties = orjson.loads(node_properties_str)
                for name, idx in other_properties_name_idx:
                    node_properties[name] = row[idx]
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
        edges_filepath = self.input()["tsv_files"]["edges_file"].path
        sql_cmd = """
            INSERT INTO edges (source_id, target_id, type, properties)
            VALUES (?, ?, ?, ?)
        """
        with open(edges_filepath, "r", newline="") as f:
            csv.field_size_limit(sys.maxsize)
            reader = csv.reader(f, delimiter="\t")
            columns = next(reader)
            source_id_idx = columns.index("start_id")
            target_id_idx = columns.index("end_id")
            type_idx = columns.index("type")
            properties_str_idx = columns.index("properties")
            other_properties_name_idx = [
                (col, columns.index(col))
                for col in ("relationship_id", "resource", "license", "source", "url")
            ]
            batch = []
            for row in reader:
                source_id = row[source_id_idx]
                target_id = row[target_id_idx]
                edge_type = row[type_idx]

                edge_properties_str = row[properties_str_idx]
                edge_properties = orjson.loads(edge_properties_str)
                for name, idx in other_properties_name_idx:
                    val = row[idx]
                    if is_informative_value(val):
                        edge_properties[name] = val
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
        nodes_filepath = self.input()["tsv_files"]["nodes_file"].path
        edges_filepath = self.input()["tsv_files"]["edges_file"].path
        _shared.extract.delete_file(nodes_filepath)
        _shared.extract.delete_file(edges_filepath)


class PharMeBINet(_shared.base.Project):
    """Pharmacological medical biochemical network (PharMeBINet).

    References
    ----------
    - Publication: https://doi.org/10.1038/s41597-022-01510-3
    - Website: https://pharmebi.net
    - Code: https://github.com/ckoenigs/PharMeBINet
    - Data: https://pharmebi.net/#/download

    """

    _label = "pharmebinet"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "Chemical": "green",
        "Chemical|Compound": "green",
        "Chemical|Compound|Salt": "green",
        "Chemical|Compound|Target": "green",
        "Chemical|Target": "green",
        "Metabolite": "green",
        "Gene": "blue",
        "GeneVariant|Indel|Variant": "blue",
        "GeneVariant|Variant": "blue",
        "GeneVariant|Variant|Complex": "blue",
        "GeneVariant|Variant|CopyNumberGain": "blue",
        "GeneVariant|Variant|CopyNumberLoss": "blue",
        "GeneVariant|Variant|Deletion": "blue",
        "GeneVariant|Variant|Duplication": "blue",
        "GeneVariant|Variant|Fusion": "blue",
        "GeneVariant|Variant|Insertion": "blue",
        "GeneVariant|Variant|Inversion": "blue",
        "GeneVariant|Variant|Microsatellite": "blue",
        "GeneVariant|Variant|MultipleNucleotideVariation": "blue",
        "GeneVariant|Variant|ProteinOnly": "blue",
        "GeneVariant|Variant|SingleNucleotideVariant": "blue",
        "GeneVariant|Variant|TandemDuplication": "blue",
        "GeneVariant|Variant|Translocation": "blue",
        "GeneVariant|Variant|Variation": "blue",
        "Protein": "blue",
        "Protein|Carrier": "blue",
        "Protein|Carrier|Enzyme": "blue",
        "Protein|Carrier|Transporter": "blue",
        "Protein|Enzyme": "blue",
        "Protein|Target": "blue",
        "Protein|Target|Carrier": "blue",
        "Protein|Target|Carrier|Enzyme": "blue",
        "Protein|Target|Carrier|Transporter": "blue",
        "Protein|Target|Carrier|Transporter|Enzyme": "blue",
        "Protein|Target|Enzyme": "blue",
        "Protein|Target|Transporter": "blue",
        "Protein|Target|Transporter|Enzyme": "blue",
        "Protein|Transporter": "blue",
        "Protein|Transporter|Enzyme": "blue",
        "Variant|DistinctChromosomes|Genotype": "blue",
        "Variant|Genotype|CompoundHeterozygote": "blue",
        "Variant|Genotype|Diplotype": "blue",
        "Variant|Haplotype": "blue",
        "Variant|Haplotype|HaplotypeSingleVariant": "blue",
        "Variant|Haplotype|PhaseUnknown": "blue",
        "BiologicalProcess": "red",
        "Disease|Phenotype": "red",
        "Pathway": "red",
        "Phenotype": "red",
        "Phenotype|SideEffect": "red",
        "Phenotype|Symptom": "red",
        "Regulation": "red",
    }

    def to_schema(self):
        # PharMeBINet has an unusually large number of different edge types,
        # therefore a more compact schema representation is used for it.
        tasks = [
            _shared.tasks.CreateCompactSchemaFile(
                self.dirpath,
                self.version,
                self._node_type_to_color,
                self._CreateSqliteFile,
            ),
        ]
        self._tasks.extend(tasks)
