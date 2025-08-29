import json
import os

import pytest

import kgw

from . import utils


def test_package_api(workdir):
    # Package version
    pkg_version = kgw.__version__
    assert isinstance(pkg_version, str)
    assert "." in pkg_version

    # Project classes
    all_project_classes = [
        kgw.biomedicine.Ckg,
        kgw.biomedicine.Hald,
        kgw.biomedicine.Hetionet,
        kgw.biomedicine.MonarchKg,
        kgw.biomedicine.Oregano,
        kgw.biomedicine.PharMeBINet,
        kgw.biomedicine.PrimeKg,
    ]

    # Project instances
    version = "latest"
    all_project_instances = [
        proj_cls(version, workdir) for proj_cls in all_project_classes
    ]

    # Test argument parsing of the project class constructor
    for proj_cls in all_project_classes:
        with pytest.raises(ValueError):
            proj_cls("nonsense", workdir)
        with pytest.raises(TypeError):
            proj_cls(version, 123)

    # Test fetching project versions
    for proj in all_project_instances:
        versions = proj.get_versions()
        assert isinstance(versions, list)
        for v in versions:
            assert isinstance(v, str)
            assert len(v) > 0

    # Test the run function with various argument combinations
    hald = kgw.biomedicine.Hald(version, workdir)
    oregano = kgw.biomedicine.Oregano(version, workdir)
    for workflow in [hald, [hald, oregano], (hald, oregano), set([hald, oregano])]:
        kgw.run(workflow)
        kgw.run(workflow, 3)
        kgw.run(workflow, 3, True)
        kgw.run(workflow=workflow, num_workers=42, verbose=False)

    # Test argument parsing of the run function
    for val in [3.14, "hi"]:
        with pytest.raises(TypeError):
            kgw.run(val)
        with pytest.raises(TypeError):
            kgw.run(hald, val)
        with pytest.raises(TypeError):
            kgw.run(hald, 2, val)
    with pytest.raises(ValueError):
        kgw.run([])


def test_entire_etl_pipeline_on_latest_project_versions(workdir):
    specification = [
        # (kgw.biomedicine.Ckg, True),
        (kgw.biomedicine.Hald, False),
        (kgw.biomedicine.Hetionet, False),
        # (kgw.biomedicine.MonarchKg, False),
        # (kgw.biomedicine.Oregano, False),
        (kgw.biomedicine.PharMeBINet, True),
        # (kgw.biomedicine.PrimeKg, False),
    ]
    tasks = []
    for project_class, too_large_kg in specification:
        nonsense_version = -123
        with pytest.raises(ValueError) as excinfo:
            proj = project_class(nonsense_version, workdir)
        msg = f'Version "{nonsense_version}" is not valid.\nAvailable options: '
        assert str(excinfo.value).startswith(msg)

        proj = project_class("latest", workdir)

        # Output formats tested on all KGs
        proj.to_statistics()
        proj.to_schema()
        proj.to_sqlite()
        proj.to_metta(representation="spo")
        proj.to_metta(representation="properties_aggregated")
        proj.to_metta(representation="properties_expanded")

        # Output formats tested only if a KG is not too large,
        # since it would take excessive time and storage otherwise
        if not too_large_kg:
            proj.to_sql()
            proj.to_csv()
            proj.to_jsonl()
            proj.to_graphml()
        tasks.append(proj)

    success = kgw.run(tasks)
    assert success, "A part of the workflow failed"


def test_file_statistics(workdir, project_params):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_statistics()
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath = os.path.join(workdir, project_dir, "results", "statistics.json")
    utils.check_file(filepath)

    # Content checks
    data = utils.load_json_file(filepath)
    assert isinstance(data, dict)
    for key in ("num_nodes", "num_edges", "num_node_types", "num_edge_types"):
        val = data[key]
        assert isinstance(val, int)
        assert val > 0

    for key in ("node_types", "edge_types"):
        val = data[key]
        assert isinstance(val, dict)
        assert len(val) > 0
        for typ, cnt in val.items():
            assert isinstance(typ, str)
            assert isinstance(cnt, int)
            assert len(typ) > 0
            assert cnt > 0

    # Comparisons
    assert num_nodes == data["num_nodes"]
    assert num_edges == data["num_edges"]


def test_file_schema(workdir, project_params):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_schema()
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath = os.path.join(workdir, project_dir, "results", "schema.html")
    utils.check_file(filepath)


def test_file_sql(workdir, project_params):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_sql()
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath = os.path.join(workdir, project_dir, "results", "kg.sql")
    utils.check_file(filepath)

    # Content checks
    text = utils.load_text_file(filepath)
    assert text.startswith("BEGIN TRANSACTION;")
    assert "CREATE TABLE nodes" in text
    assert "CREATE TABLE edges" in text
    assert 'INSERT INTO "nodes"' in text
    assert 'INSERT INTO "edges"' in text
    assert "CREATE INDEX" in text
    assert text.endswith("COMMIT;\n")


def test_file_csv(workdir, project_params):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_csv()
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath_nodes = os.path.join(workdir, project_dir, "results", "kg_nodes.csv")
    filepath_edges = os.path.join(workdir, project_dir, "results", "kg_edges.csv")
    utils.check_file(filepath_nodes)
    utils.check_file(filepath_edges)

    # Content checks
    node_data, node_columns = utils.load_csv_file(filepath_nodes)
    assert node_columns == ["id", "type", "properties"]
    for row in node_data:
        nid, ntype, nprop_str = row
        assert isinstance(nid, str)
        assert isinstance(ntype, str)
        assert isinstance(nprop_str, str)
        assert len(nid) > 0
        assert len(ntype) > 0
        assert len(nprop_str) > 0
        nprop = json.loads(nprop_str)
        assert isinstance(nprop, dict)

    edge_data, edge_columns = utils.load_csv_file(filepath_edges)
    assert edge_columns == ["source_id", "target_id", "type", "properties"]
    for row in edge_data:
        source_id, target_id, etype, eprop_str = row
        assert isinstance(source_id, str)
        assert isinstance(target_id, str)
        assert isinstance(etype, str)
        assert isinstance(eprop_str, str)
        assert len(source_id) > 0
        assert len(target_id) > 0
        assert len(etype) > 0
        assert len(eprop_str) > 0
        eprop = json.loads(eprop_str)
        assert isinstance(eprop, dict)

    # Comparisons
    assert num_nodes == len(node_data)
    assert num_edges == len(edge_data)


def test_file_jsonl(workdir, project_params):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_jsonl()
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath_nodes = os.path.join(workdir, project_dir, "results", "kg_nodes.jsonl")
    filepath_edges = os.path.join(workdir, project_dir, "results", "kg_edges.jsonl")
    utils.check_file(filepath_nodes)
    utils.check_file(filepath_edges)

    # Content checks
    node_data = utils.load_jsonl_file(filepath_nodes)
    for row in node_data:
        nid = row["id"]
        ntype = row["type"]
        nprop = row["properties"]
        assert isinstance(nid, str)
        assert isinstance(ntype, str)
        assert isinstance(nprop, dict)
        assert len(nid) > 0
        assert len(ntype) > 0
        assert len(nprop) >= 0  # may be an empty dict

    edge_data = utils.load_jsonl_file(filepath_edges)
    for row in edge_data:
        source_id = row["source_id"]
        target_id = row["target_id"]
        etype = row["type"]
        eprop = row["properties"]
        assert isinstance(source_id, str)
        assert isinstance(target_id, str)
        assert isinstance(etype, str)
        assert isinstance(eprop, dict)
        assert len(source_id) > 0
        assert len(target_id) > 0
        assert len(etype) > 0
        assert len(eprop) >= 0  # may be an empty dict

    # Comparisons
    assert num_nodes == len(node_data)
    assert num_edges == len(edge_data)


def test_file_graphml(workdir, project_params):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_graphml()
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath = os.path.join(workdir, project_dir, "results", "kg.graphml")
    utils.check_file(filepath)

    # Content checks
    num_nodes_ig, num_edges_ig, parsed_nodes_ig = utils.load_graphml_igraph(filepath)
    num_nodes_nx, num_edges_nx, parsed_nodes_nx = utils.load_graphml_networkx(filepath)

    # Comparisons
    assert num_nodes == num_nodes_ig == num_nodes_nx
    assert num_edges == num_edges_ig == num_edges_nx
    assert parsed_nodes_ig == parsed_nodes_nx


@pytest.mark.parametrize(
    "representation", ["spo", "properties_aggregated", "properties_expanded"]
)
def test_file_metta(workdir, project_params, representation):
    project_name, project_class, project_version, num_nodes, num_edges = project_params
    project = project_class(project_version, workdir)
    project.to_metta(representation=representation)
    kgw.run([project])

    # File checks
    project_dir = utils.get_project_dir(project_name, project_version)
    filepath = os.path.join(
        workdir, project_dir, "results", f"kg_{representation}.metta"
    )
    utils.check_file(filepath)

    # Content checks
    with open(filepath) as f:
        previous_number = 0
        for cnt_line, line in enumerate(f):
            # Skip comment lines
            if line.startswith(";"):
                continue

            # Ensure line starts and ends with a parenthesis
            assert line.startswith("(")
            assert line.endswith(")\n")

            # Ensure number stays same or is incremented by 1 from line to line
            if representation == "properties_expanded":
                number = int(line.split(" ", 1)[0][1:])
                assert number == previous_number or number == (previous_number + 1)
                previous_number = number

            # Ensure last parenthesis closes the first one, i.e. not depth 0 in between
            depth = 0
            idx_first_parenthesis = 0
            idx_last_parenthesis = (
                len(line) - 2
            )  # -2 instead of -1 due to \n being last character
            pause_parenthesis_tracking = False
            cnt_previous_backslashes = 0
            for i, c in enumerate(line):
                if depth == 0:
                    assert (
                        i <= idx_first_parenthesis or i >= idx_last_parenthesis
                    ), f"Invalid parentheses in {filepath} on line {cnt_line} at character {i}"
                if c == '"' and cnt_previous_backslashes % 2 == 0:
                    # ignore parenthesis inside string literals, which start and end
                    # with an unescaped " character
                    pause_parenthesis_tracking = not pause_parenthesis_tracking
                if not pause_parenthesis_tracking and c == "(":
                    depth += 1
                if not pause_parenthesis_tracking and c == ")":
                    depth -= 1
                if c == "\\":
                    cnt_previous_backslashes += 1
                else:
                    cnt_previous_backslashes = 0
            assert (
                depth == 0
            ), f"Imbalanced parentheses in {filepath} on line {cnt_line} at character {i}"
