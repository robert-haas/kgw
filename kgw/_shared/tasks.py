import logging
import os
from contextlib import contextmanager

import luigi

from . import extract, load


# Contextmanager for temporary files


@contextmanager
def temp_output_path(filepath, suffix=".partial", reuse_partial_file=False):
    temp_filepath = filepath + suffix
    if os.path.exists(temp_filepath):
        if not reuse_partial_file:
            os.remove(temp_filepath)
    try:
        # Yield the temp file path for use in the block
        yield temp_filepath
        # Once the block completes, rename temp file to final file path
        os.rename(temp_filepath, filepath)
    except Exception as excp:
        if not reuse_partial_file:
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)
        raise excp


# Reporting behavior for all tasks


class ReportingTask(luigi.Task):
    logger = logging.getLogger("kgw")
    RED = "\033[91m"
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    RESET = "\033[0m"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Prevent occasionally duplicated log messages
        self._start_happened = False
        self._success_happened = False
        self._failure_happened = False

    @luigi.Task.event_handler(luigi.Event.START)
    def on_start(self):
        if not self._start_happened:
            self.logger.info(f"{self.BLUE}Started{self.RESET}   {self}")
        self._start_happened = True

    @luigi.Task.event_handler(luigi.Event.SUCCESS)
    def on_success(self):
        if not self._success_happened:
            self.logger.info(f"{self.GREEN}Finished{self.RESET}  {self}")
        self._success_happened = True

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def on_failure(self, exception):
        excp_name = type(exception).__name__
        excp_text = str(exception)
        if not self._failure_happened:
            self.logger.info(
                f"{self.RED}Failed{self.RESET}    {self}\n"
                f"                               {excp_name}: {excp_text}"
            )
        self._failure_happened = True


# Extract


class DirectoryTarget(luigi.Target):
    def __init__(self, dirpath):
        self.dirpath = dirpath

    def exists(self):
        return os.path.isdir(self.dirpath)

    def touch(self):
        os.makedirs(self.dirpath, exist_ok=True)


class CreateDirectory(ReportingTask):
    dirpath = luigi.Parameter()

    def output(self):
        return DirectoryTarget(self.dirpath)

    def run(self):
        self.output().touch()


class DownloadFile(ReportingTask):
    dirpath = luigi.Parameter()
    filename = luigi.Parameter()
    url = luigi.Parameter(significant=False)
    md5 = luigi.Parameter(default="", significant=False)
    sha256 = luigi.Parameter(default="", significant=False)

    # https://luigi.readthedocs.io/en/stable/configuration.html#per-task-retry-policy
    retry_count = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.filepath = os.path.join(self.dirpath, self.filename)

    def requires(self):
        return CreateDirectory(self.dirpath)

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        with temp_output_path(self.filepath, reuse_partial_file=True) as filepath_tmp:
            extract.fetch_file(self.url, filepath_tmp)
            if self.md5 != "":
                if not extract.is_valid_file_by_md5(filepath_tmp, self.md5):
                    message = f"Invalid file (MD5 hash does not match): {self.filepath}"
                    raise Exception(message)
            if self.sha256 != "":
                if not extract.is_valid_file_by_sha256(filepath_tmp, self.sha256):
                    message = (
                        f"Invalid file (SHA256 hash does not match): {self.filepath}"
                    )
                    raise Exception(message)


# Load


class CreateStatisticsFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "statistics.json")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as statistics_filepath:
            load.sqlite_to_statistics(db_filepath, statistics_filepath)


class CreateSchemaFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    node_type_to_color = luigi.DictParameter(significant=False)
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "schema.html")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as schema_filepath:
            load.sqlite_to_schema(db_filepath, schema_filepath, self.node_type_to_color)


class CreateCompactSchemaFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    node_type_to_color = luigi.DictParameter(significant=False)
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "schema.html")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as schema_filepath:
            load.sqlite_to_schema_compact(
                db_filepath, schema_filepath, self.node_type_to_color
            )


class CreateSqlFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg.sql")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as sql_filepath:
            load.sqlite_to_sql(db_filepath, sql_filepath)


class CreateCsvNodesFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg_nodes.csv")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as csv_filepath:
            load.sqlite_to_csv(db_filepath, csv_filepath, "nodes")


class CreateCsvEdgesFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg_edges.csv")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as csv_filepath:
            load.sqlite_to_csv(db_filepath, csv_filepath, "edges")


class CreateJsonlNodesFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg_nodes.jsonl")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as jsonl_filepath:
            load.sqlite_to_jsonl(db_filepath, jsonl_filepath, "nodes")


class CreateJsonlEdgesFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg_edges.jsonl")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as jsonl_filepath:
            load.sqlite_to_jsonl(db_filepath, jsonl_filepath, "edges")


class CreateMettaFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    representation = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)
    batch_size = luigi.IntParameter(default=10_000, significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, f"kg_{self.representation}.metta")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as metta_filepath:
            if self.representation == "spo":
                load.sqlite_to_metta_repr1(db_filepath, metta_filepath, self.batch_size)
            elif self.representation == "properties_aggregated":
                load.sqlite_to_metta_repr2(db_filepath, metta_filepath, self.batch_size)
            elif self.representation == "properties_expanded":
                load.sqlite_to_metta_repr3(db_filepath, metta_filepath, self.batch_size)
            else:
                message = (
                    f"Invalid representation: {self.representation}\n"
                    'Available options: "rpo", "properties_aggregated", "properties_expanded"'
                )
                raise ValueError(message)


class CreateGraphMlFile(ReportingTask):
    dirpath = luigi.Parameter()
    version = luigi.Parameter()
    transform_task = luigi.TaskParameter(significant=False)
    batch_size = luigi.IntParameter(default=10_000, significant=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.subdirpath = os.path.join(self.dirpath, "results")
        self.filepath = os.path.join(self.subdirpath, "kg.graphml")

    def requires(self):
        tasks = {
            "subdirpath": CreateDirectory(self.subdirpath),
            "sqlite_file": self.transform_task(self.dirpath, self.version),
        }
        return tasks

    def output(self):
        return luigi.LocalTarget(self.filepath)

    def run(self):
        db_filepath = self.input()["sqlite_file"].path
        with temp_output_path(self.filepath) as graphml_filepath:
            load.sqlite_to_graphml(db_filepath, graphml_filepath, self.batch_size)
