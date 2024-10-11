import csv
import json
import os
import sys

import igraph as ig
import networkx as nx


def get_project_dir(name, version):
    return f"{name}_v{version}"


def check_file(filepath):
    assert os.path.isfile(filepath), f'File "{filepath}" does not exist'
    assert os.path.getsize(filepath) > 0, f'File "{filepath}" is empty'


def load_text_file(filepath):
    with open(filepath) as f:
        data = f.read()
    return data


def load_json_file(filepath):
    with open(filepath) as f:
        data = json.load(f)
    assert data
    return data


def load_jsonl_file(filepath):
    data = []
    with open(filepath) as file:
        for line in file:
            assert len(line) > 1, "Blank line"
            item = json.loads(line)
            data.append(item)
    return data


def load_csv_file(filepath):
    csv.field_size_limit(sys.maxsize)
    with open(filepath, "r", newline="") as csvfile:
        reader = csv.reader(csvfile)
        columns = next(reader)
        data = []
        for row in reader:
            assert len(row) > 0, "Blank line"
            data.append(row)

    assert isinstance(data, list)
    assert isinstance(columns, list)
    assert len(data) > 0
    assert len(columns) > 0
    assert len(data[0]) == len(columns)
    return data, columns


def generate_sample_indices(num_total, num_samples=42):
    return [int(num_total / num_samples * i) for i in range(num_samples)]


def load_graphml_igraph(filepath):
    # Parsing GraphML
    graph = ig.Graph.Read_GraphML(filepath)
    # Counts
    num_nodes = graph.vcount()
    num_edges = graph.ecount()
    # Sample nodes
    sample_nodes_idx = generate_sample_indices(num_nodes)
    sample_nodes = [graph.vs[idx] for idx in sample_nodes_idx]
    skip_attrs = ["id", "node_type"]
    parsed_nodes = []
    for chosen_node in sample_nodes:
        node_id = chosen_node["id"]
        node_type = chosen_node["node_type"]
        node_attributes = graph.vertex_attributes()
        node_properties = {
            attr: chosen_node[attr]
            for attr in node_attributes
            if attr not in skip_attrs
        }
        node_properties = {
            k: v
            for k, v in sorted(node_properties.items())
            if v is not None and v != ""
        }
        parsed_nodes.append((node_id, node_type, node_properties))
    return num_nodes, num_edges, parsed_nodes


def load_graphml_networkx(filepath):
    # Parsing GraphML
    graph = nx.read_graphml(filepath)
    # Counts
    num_nodes = graph.number_of_nodes()
    num_edges = graph.number_of_edges()
    # Sample nodes
    sample_nodes_idx = generate_sample_indices(num_nodes)
    sample_nodes = [
        node
        for idx, node in enumerate(graph.nodes(data=True))
        if idx in sample_nodes_idx
    ]
    skip_attrs = ["id", "node_type"]
    parsed_nodes = []
    for chosen_node in sample_nodes:
        node_id, node_data = chosen_node
        node_type = node_data["node_type"]
        node_properties = {
            k: v for k, v in sorted(node_data.items()) if k not in skip_attrs
        }
        parsed_nodes.append((node_id, node_type, node_properties))
    return num_nodes, num_edges, parsed_nodes
