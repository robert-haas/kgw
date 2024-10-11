import csv
import os
import sqlite3

import luigi
import orjson

from .. import _shared
from .._shared.extract import is_informative_value


class MetadataFetcher:
    dataset_id = "mrcf7f4tc2"
    _cached_values = {}

    @classmethod
    def get_versions(cls):
        # Memorize
        key = "versions"
        if key not in cls._cached_values:
            cls._cached_values[key] = _shared.extract.get_versions_from_mendeley(
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
            cls._cached_values[key] = _shared.extract.get_metadata_from_mendeley(
                cls.dataset_id, version
            )

        # Lookup
        metadata = cls._cached_values[key]
        return metadata


class FetchNeo4jDumpFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")
        self.metadata = MetadataFetcher.get_metadata(self.version)

    def _detect_filename(self):
        # The filename is slightly different for different versions, hence it has to be detected
        for candidate in self.metadata:
            if candidate.endswith(".dump"):
                return candidate
        raise ValueError(
            "Could not identify the Neo4j dump file that contains the knowledge graph."
        )

    def requires(self):
        filename = self._detect_filename()
        url = self.metadata[filename]["url"]
        sha256 = self.metadata[filename]["sha256"]
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url, sha256=sha256)

    def output(self):
        return self.input()


class FetchNeo4jApocFile(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "downloads")

    def requires(self):
        filename = "apoc-4.4.0.24-all.jar"
        url = (
            "https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/"
            "download/4.4.0.24/apoc-4.4.0.24-all.jar"
        )
        md5 = "7c6a702322b0aaf663c25f378cd3494d"
        return _shared.tasks.DownloadFile(self.subdirpath, filename, url, md5=md5)

    def output(self):
        return self.input()


class ExportNeo4jToCsvFiles(_shared.tasks.ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")

    def requires(self):
        return {
            "neo4j_database_dump": FetchNeo4jDumpFile(self.dirpath, self.version),
            "neo4j_apoc_plugin": FetchNeo4jApocFile(self.dirpath, self.version),
        }

    def output(self):
        return {
            "nodes_file": luigi.LocalTarget(
                os.path.join(self.subdirpath, "neo4j_nodes.csv")
            ),
            "edges_file": luigi.LocalTarget(
                os.path.join(self.subdirpath, "neo4j_edges.csv")
            ),
        }

    def run(self):
        dw = DockerWrapper()

        # Check docker installation
        if not dw.test_installation():
            msg = (
                "Testing the Docker installation on your system indicated that "
                "it does not work as expected on a minimal example. A potential "
                "issue can be a missing internet connection for fetching remote "
                "Docker images."
            )
            raise ValueError(msg)

        # Convert .dump to .csv
        filepath_db = self.input()["neo4j_database_dump"].path
        filepath_apoc = self.input()["neo4j_apoc_plugin"].path
        dirpath_source = os.path.dirname(filepath_db)
        dirpath_target = self.subdirpath
        filename_db = os.path.basename(filepath_db)
        filename_apoc = os.path.basename(filepath_apoc)
        dw.convert_neo4j_dump_to_csv_files(
            dirpath_source, dirpath_target, filename_db, filename_apoc
        )


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
            "csv_files": ExportNeo4jToCsvFiles(self.dirpath, self.version),
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
        self._remove_input_csv_files()

    def _insert_nodes(self, conn, cursor):
        nodes_filepath = self.input()["csv_files"]["nodes_file"].path
        sql_cmd = """
            INSERT INTO nodes (id, type, properties)
            VALUES (?, ?, ?)
        """
        with open(nodes_filepath, "r", newline="") as f:
            reader = csv.reader(f, delimiter=",")
            columns = next(reader)
            batch = []
            for row in reader:
                node_id = str(row[0])
                node_type = str(row[1])
                node_properties = {
                    str(key): val
                    for key, val in orjson.loads(row[2]).items()
                    if is_informative_value(val)
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
        with open(edges_filepath, "r", newline="") as f:
            reader = csv.reader(f, delimiter=",")
            columns = next(reader)
            batch = []
            for row in reader:
                source_id = str(row[0])
                target_id = str(row[1])
                edge_type = str(row[2])
                edge_properties = {
                    str(key): val
                    for key, val in orjson.loads(row[3]).items()
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
        nodes_filepath = self.input()["csv_files"]["nodes_file"].path
        edges_filepath = self.input()["csv_files"]["edges_file"].path
        for filepath in (nodes_filepath, edges_filepath):
            try:
                os.remove(filepath)
            except Exception:
                pass


class DockerWrapper:
    def __init__(self):
        try:
            import docker

            self._docker_module = docker
        except ImportError:
            msg = (
                "This knowledge graph project requires 1) a Docker installation on "
                'your operating system and 2) the Python package "docker" for the '
                "language bindings, which can be installed with `pip install docker`."
            )
            raise ImportError(msg)

    def test_installation(self):
        try:
            # Definition
            container_name = "hello-world"
            expected_message = b"Hello from Docker!"
            # Run
            client = self._docker_module.from_env()
            client.images.pull(container_name)
            container = client.containers.run(container_name, detach=True)
            works = expected_message in container.logs()
            # Cleanup
            container.remove(force=True)
            client.images.remove(container_name, force=True)
            client.containers.prune()
            client.images.prune()
            client.volumes.prune()
        except Exception:
            works = False
        return works

    def run_command(self, container, command):
        print(command)
        exit_code, output = container.exec_run(command)
        result = output.decode().strip()
        print(result)
        print()
        return result

    def stop_and_remove_container(self, name):
        try:
            client = self._docker_module.from_env()
            container = client.containers.get(name)
            if container.status == "running":
                container.stop()
            container.remove(force=True)
            client.containers.prune()
            client.images.prune()
            client.volumes.prune()
        except Exception:
            pass

    def convert_neo4j_dump_to_csv_files(
        self, dirpath_source, dirpath_target, filename_db, filename_apoc
    ):
        # APOC library provides CSV export: https://neo4j.com/labs/apoc/4.4/overview/apoc.export

        # Temporary directory for output to prevent side effects such as changed permissions
        dirpath_target_temp = os.path.join(dirpath_target, "tempdir_for_docker")
        os.makedirs(dirpath_target_temp, exist_ok=True)

        # Fetch and run the Neo4j v4.4 Docker container
        unique_container_name = "kgw_neo4j_198bc9da7d"
        self.stop_and_remove_container(unique_container_name)
        client = self._docker_module.from_env()
        container = client.containers.run(
            image="neo4j:4.4",
            name=unique_container_name,
            detach=True,
            volumes={
                os.path.abspath(dirpath_source): {"bind": "/source", "mode": "rw"},
                os.path.abspath(dirpath_target_temp): {
                    "bind": "/target",
                    "mode": "rw",
                },
            },
            working_dir="/target",
            entrypoint="tail -f /dev/null",
        )

        # Convert the .dump file to two .csv files
        commands = [
            # Configure Neo4j for the import
            '''sh -c "echo 'dbms.allow_upgrade=true' >> /var/lib/neo4j/conf/neo4j.conf"''',
            '''sh -c "echo 'dbms.security.procedures.allowlist=apoc.*' >> /var/lib/neo4j/conf/neo4j.conf"''',
            '''sh -c "echo 'dbms.security.procedures.unrestricted=apoc.*' >> /var/lib/neo4j/conf/neo4j.conf"''',
            '''sh -c "echo 'apoc.export.file.enabled=true' >> /var/lib/neo4j/conf/neo4j.conf"''',
            '''sh -c "neo4j-admin set-initial-password correcthorsebatterystaple"''',
            # Make APOC extension available to Neo4j
            f'''sh -c "cp /source/{filename_apoc} /var/lib/neo4j/plugins"''',
            # Import the .dump file
            f'''sh -c "neo4j-admin load --from=/source/{filename_db} --database=neo4j --force"''',
            # Start the database
            '''sh -c "rm -rf /var/lib/neo4j/logs && neo4j start"''',
            # Wait so it is ready
            '''sh -c "sleep 360"''',
            # Export a .csv file for nodes
            '''cypher-shell -u neo4j -p correcthorsebatterystaple -d neo4j "CALL apoc.export.csv.query('MATCH (n) RETURN id(n) as id, labels(n)[0] as type, properties(n) as properties', 'nodes.csv', {})"''',
            # Export a .csv file for edges
            '''cypher-shell -u neo4j -p correcthorsebatterystaple -d neo4j "CALL apoc.export.csv.query('MATCH ()-[r]->() RETURN id(startNode(r)) as source_id, id(endNode(r)) as target_id, type(r) as type, properties(r) as properties', 'edges.csv', {})"''',
            # Move the .csv files to the mounted directory to make them available outside the container
            '''sh -c "mv /var/lib/neo4j/import/nodes.csv /target"''',
            '''sh -c "mv /var/lib/neo4j/import/edges.csv /target"''',
        ]
        for cmd in commands:
            self.run_command(container, cmd)

        # Remove the container
        self.stop_and_remove_container(unique_container_name)

        # Move CSV files from temporary to target directory
        os.rename(
            os.path.join(dirpath_target_temp, "nodes.csv"),
            os.path.join(dirpath_target, "neo4j_nodes.csv"),
        )
        os.rename(
            os.path.join(dirpath_target_temp, "edges.csv"),
            os.path.join(dirpath_target, "neo4j_edges.csv"),
        )
        os.rmdir(dirpath_target_temp)
        return container


class Ckg(_shared.base.Project):
    """Clinical Knowledge Graph (CKG).

    References
    ----------
    - Publication: https://doi.org/10.1038/s41587-021-01145-6
    - Website: https://ckg.readthedocs.io
    - Code: https://github.com/MannLabs/CKG
    - Data: https://doi.org/10.17632/mrcf7f4tc2

    """

    _label = "ckg"
    _MetadataFetcher = MetadataFetcher
    _CreateSqliteFile = CreateSqliteFile
    _node_type_to_color = {
        "Metabolite": "green",
        "Known_variant": "blue",
        "Gene": "blue",
        "Transcript": "blue",
        "Protein": "blue",
        "Modified_protein": "blue",
        "Peptide": "blue",
        "Disease": "red",
        "Pathway": "red",
        "Biological_process": "red",
    }
