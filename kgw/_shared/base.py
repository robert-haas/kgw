import logging
import multiprocessing
import os
import sys
import warnings

import luigi

from . import tasks


class Project:
    def __init__(self, version, workdir):
        """Initialize a project instance so that tasks can be defined on it.

        Parameters
        ----------
        version : `str`
            Version of the dataset that will be downloaded and processed.
            The method :meth:`get_versions` returns all currently available
            versions.
        workdir : `str`
            Path of the working directory in which a unique subdirectory will
            be created to hold all downloaded and generated files for
            this project and version.

        Raises
        ------
        ValueError
            Raised if `version` is invalid or unavailable.
        TypeError
            Raised if `workdir` is not a string.

        Notes
        -----
        This class does not automatically download or process any data.
        Such tasks first need to be specified by calling the relevant methods
        on the project object and then passing it to the function :func:`~kgw.run`
        that builds and executes a corresponding workflow.

        """
        # Argument processing
        version = str(version)
        _available_versions = self.get_versions()
        if version.lower() == "latest":
            version = _available_versions[-1]
        if version not in _available_versions:
            msg = f'Version "{version}" is not valid.\nAvailable options: {_available_versions}'
            raise ValueError(msg)

        if not isinstance(workdir, str):
            msg = 'Argument "workdir" needs to be a string.'
            raise TypeError(msg)

        # Set instance variables
        self.version = version
        self.dirpath = os.path.join(workdir, f"{self._label}_v{version}")
        self._tasks = []

    @classmethod
    def get_versions(cls):
        """Fetch all currently available versions from the data repository of the project."""
        return cls._MetadataFetcher.get_versions()

    def to_sqlite(self):
        """Convert the knowledge graph to a file-based SQLite database.

        Generates the output file `kg.sqlite`. This database contains
        a unified representation for each knowledge graph, with the
        same schema being used for each projects. From this intermediate
        format it is possible to generate all other files, using just
        one method per output format rather than writing a custom
        converter for each project.

        References
        ----------
        - `SQLite <https://www.sqlite.org>`__
        - `Wikipedia: SQLite <https://en.wikipedia.org/wiki/SQLite>`__

        """
        tasks = [self._CreateSqliteFile(self.dirpath, self.version)]
        self._tasks.extend(tasks)

    def to_statistics(self):
        """Determine some statistical properties of the knowledge graph.

        Generates the output file `statistics.json`. This is a JSON
        file that contains data about basic statistics of the elements
        in the knowledge graph, such as node, edge and type counts.

        """
        required_tasks = [
            tasks.CreateStatisticsFile(
                self.dirpath, self.version, self._CreateSqliteFile
            ),
        ]
        self._tasks.extend(required_tasks)

    def to_schema(self):
        """Determine the schema of the knowledge graph.

        Generates the output file `schema.html`. This is a standalone
        HTML file with an interactive graph visualization of all
        entity types in the knowledge graph and the relationship types
        by which they are connected.

        References
        ----------
        - `Neo4j: Graph modeling guidelines
          <https://neo4j.com/docs/getting-started/data-modeling/guide-data-modeling/>`__
        - `Memgraph: Graph modeling
          <https://memgraph.com/docs/fundamentals/graph-modeling>`__

        """
        required_tasks = [
            tasks.CreateSchemaFile(
                self.dirpath,
                self.version,
                self._node_type_to_color,
                self._CreateSqliteFile,
            ),
        ]
        self._tasks.extend(required_tasks)

    def to_sql(self):
        """Convert the knowledge graph to a SQL text file.

        Generates the output file `kg.sql`. This is a text file with
        SQL commands that can be used to import the structure and content
        of the knowledge graph into relational database systems such as
        MySQL or PostgreSQL.

        References
        ----------
        - `StackOverflow: How to import an SQL file using the command line in MySQL?
          <https://stackoverflow.com/questions/17666249/how-to-import-an-sql-file-using-the-command-line-in-mysql>`__
        - `StackOverflow: Import SQL dump into PostgreSQL database
          <https://stackoverflow.com/questions/6842393/import-sql-dump-into-postgresql-database>`__

        """
        required_tasks = [
            tasks.CreateSqlFile(self.dirpath, self.version, self._CreateSqliteFile),
        ]
        self._tasks.extend(required_tasks)

    def to_csv(self):
        """Convert the knowledge graph to two CSV text files.

        Generates the output files `kg_nodes.csv` and `kg_edges.csv`.

        References
        ----------
        - `Wikipedia: CSV <https://en.wikipedia.org/wiki/Comma-separated_values>`__

        """
        required_tasks = [
            tasks.CreateCsvNodesFile(
                self.dirpath, self.version, self._CreateSqliteFile
            ),
            tasks.CreateCsvEdgesFile(
                self.dirpath, self.version, self._CreateSqliteFile
            ),
        ]
        self._tasks.extend(required_tasks)

    def to_jsonl(self):
        """Convert the knowledge graph to two JSON Lines text files.

        Generates the output files `kg_nodes.jsonl` and `kg_edges.jsonl`.

        References
        ----------
        - `JSONL <https://jsonlines.org>`__

        """
        required_tasks = [
            tasks.CreateJsonlNodesFile(
                self.dirpath, self.version, self._CreateSqliteFile
            ),
            tasks.CreateJsonlEdgesFile(
                self.dirpath, self.version, self._CreateSqliteFile
            ),
        ]
        self._tasks.extend(required_tasks)

    def to_metta(self, representation="spo"):
        """Convert the knowledge graph to a MeTTa text file.

        Generates the output file `kg_spo.metta`, `kg_properties_aggregated.metta` or
        `kg_properties_expanded.metta`, depending on the chosen representation.

        Caution: These representations are still subject to experimentation
        and testing. They might change in future versions of this package.

        Parameters
        ----------
        representation : str
            The format used to represent the knowledge graph in the MeTTa language.

            Available options:

            - `"spo"`: Semantic triples of the form `("subject", "predicate", "object")`.
              If properties are present in the original knowledge graph, they are
              ignored in this representation.
            - `"properties_aggregated"`: Properties (=key-value pairs) are represented
              by putting each key on a separate line, but each value is ensured to be a
              single number or string. This means values that hold a compound data type
              like a list or dict are aggregated into one string in JSON string format.
              Text identifiers of nodes are reused to create the association with their
              properties, while text identifiers of the form "e{cnt}" are introduced for
              edges to serve the same purpose.
            - `"properties_expanded"`: Properties (=key-value pairs) are represented
              by fully expanding their keys and values onto as many lines as required.
              Numerical identifiers for nodes and edges are introduced to create the
              association between these elements and their properties.

        References
        ----------
        - `MeTTa language <https://metta-lang.dev>`__
        - `OpenCog Hyperon framework <https://hyperon.opencog.org>`__

        """
        required_tasks = [
            tasks.CreateMettaFile(
                self.dirpath, self.version, representation, self._CreateSqliteFile
            ),
        ]
        self._tasks.extend(required_tasks)

    def to_graphml(self):
        """Convert the knowledge graph to a GraphML text file.

        Generates the output file `kg.graphml`.

        References
        ----------
        - `GraphML <http://graphml.graphdrawing.org>`__
        - `Wikipedia: GraphML <https://en.wikipedia.org/wiki/GraphML>`__

        """
        required_tasks = [
            tasks.CreateGraphMlFile(self.dirpath, self.version, self._CreateSqliteFile),
        ]
        self._tasks.extend(required_tasks)


def set_logging_behavior(verbose):
    # Turn off luigi's logger
    for name in ("luigi", "luigi-interface"):
        logger = logging.getLogger(name)
        if logger.handlers:
            logger.handlers.clear()
        logger.disabled = True
        warnings.filterwarnings("ignore", module=name)

    # Set up logger for this package
    logger = logging.getLogger(__name__)
    if logger.handlers:
        logger.handlers.clear()
    if verbose:
        logger.disabled = False
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[logging.StreamHandler(sys.stdout)],
            force=True,
        )
    else:
        logger.disabled = True


def run(workflow, num_workers=None, verbose=True):
    """Execute all tasks in the provided workflow according to their dependencies.

    This function uses the package Luigi [1]_ to build
    a dependency graph of all tasks defined in the workflow and
    execute them in parallel with multiple worker processes.

    Parameters
    ----------
    workflow : `~kgw._shared.base.Project`, or list of `~kgw._shared.base.Project`
        Specification of a workflow in form of a single or multiple
        project objects. A project object provides several methods that can
        be called in order to add specific tasks.
        For example, calling the method `to_csv()` will store a task in the
        object that represents the conversion of the project's knowledge graph
        into CSV format. The workflow engine automatically detects these tasks,
        inspects their dependencies, and schedules all necessary steps in the
        correct order.
    num_workers : `int`, optional, default=4*cpu_count
        The number of worker processes to run tasks in parallel. If not
        specified, it defaults to 4 times the number of CPU cores available
        on the machine.
    verbose : `bool`, optional, default=True
        If `True`, a log of tasks and a summary of results is written to stdout.
        If `False`, no text is printed.

    Returns
    -------
    success : `bool`
        Returns `True` if all tasks were successfully scheduled and executed,
        otherwise `False`. A failed run can be resumed from an
        intermediate state without re-running previously completed tasks.

    Raises
    ------
    TypeError
        Raised if `workflow` is not a project object or list of such objects,
        or if `num_workers` is not an integer, or if `verbose` is not a boolean.
    ValueError
        Raised if `workflow` is an empty list.

    References
    ----------
    .. [1] Luigi Documentation, https://luigi.readthedocs.io

    """
    # Argument processing
    if isinstance(workflow, Project):
        workflows = [workflow]
    elif isinstance(workflow, (list, tuple, set)) and all(
        isinstance(w, Project) for w in workflow
    ):
        workflows = list(workflow)
    else:
        msg = 'Argument "workflow" needs to be a project or a list of projects.'
        raise TypeError(msg)
    if len(workflows) == 0:
        msg = "Got an empty list of workflows."
        raise ValueError(msg)

    if num_workers is None:
        num_workers = multiprocessing.cpu_count() * 4
    elif not isinstance(num_workers, int):
        msg = 'Argument "num_workers" needs to be an integer or None.'
        raise TypeError(msg)

    if not isinstance(verbose, bool):
        msg = 'Argument "verbose" needs to be True or False.'
        raise TypeError(msg)

    # Set logging behavior
    set_logging_behavior(verbose)

    # Collect tasks
    tasks = []
    for workflow in workflows:
        tasks.extend(workflow._tasks)

    # Run tasks
    if verbose:
        msg = "Log of performed tasks"
        print(msg)
        print("=" * len(msg))
        print()
    result = luigi.build(
        tasks,
        local_scheduler=True,
        detailed_summary=True,
        workers=num_workers,
    )

    # Report results
    def crop_results(text):
        prefix = "\n===== Luigi Execution Summary =====\n\n"
        postfix = "\n\n===== Luigi Execution Summary =====\n"
        if text.startswith(prefix):
            text = text[len(prefix) :]
        if text.endswith(postfix):
            text = text[: -len(postfix)]
        return text

    if verbose:
        print()
        print()
        msg = "Summary of workflow results"
        print(msg)
        print("=" * len(msg))
        print()
        print(crop_results(result.summary_text))
    return result.scheduling_succeeded
