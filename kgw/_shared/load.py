import csv
import sqlite3
import xml.sax.saxutils as saxutils

import gravis as gv
import networkx as nx
import orjson


def sqlite_to_statistics(db_filepath, statistics_filepath):
    with sqlite3.connect(db_filepath) as conn:
        cursor = conn.cursor()

        # Node statistics
        query = """
            SELECT
                (SELECT COUNT(*) FROM nodes) AS num_nodes,
                (SELECT COUNT(DISTINCT type) FROM nodes) AS num_node_types,
                json_group_object(type, count) AS node_types
            FROM (
                SELECT
                    type,
                    COUNT(*) AS count
                FROM
                    nodes
                GROUP BY
                    type
                ORDER BY
                    count DESC,
                    type ASC
            )
        """
        cursor.execute(query)
        row = cursor.fetchone()
        node_data = {
            "num_nodes": row[0],
            "num_node_types": row[1],
            "node_types": orjson.loads(row[2]),
        }

        # Edge statistics
        query = """
            SELECT
                (SELECT COUNT(*) FROM edges) AS num_edges,
                (SELECT COUNT(DISTINCT type) FROM edges) AS num_edge_types,
                json_group_object(type, count) AS edge_types
            FROM (
                SELECT
                    type,
                    COUNT(*) AS count
                FROM
                    edges
                GROUP BY
                    type
                ORDER BY
                    count DESC,
                    type ASC
            )
        """
        cursor.execute(query)
        row = cursor.fetchone()
        edge_data = {
            "num_edges": row[0],
            "num_edge_types": row[1],
            "edge_types": orjson.loads(row[2]),
        }

        # Combination and reordering
        data = {
            "num_nodes": node_data["num_nodes"],
            "num_edges": edge_data["num_edges"],
            "num_node_types": node_data["num_node_types"],
            "num_edge_types": edge_data["num_edge_types"],
            "node_types": node_data["node_types"],
            "edge_types": edge_data["edge_types"],
        }

        # Writing to file
        with open(statistics_filepath, "wb") as f:
            f.write(orjson.dumps(data, option=orjson.OPT_INDENT_2))
    return data


def sqlite_to_schema(db_filepath, schema_filepath, node_type_to_color=None):
    if node_type_to_color is None:
        node_type_to_color = {}

    with sqlite3.connect(db_filepath) as conn:
        cursor = conn.cursor()

        # Nodes
        query = """
            SELECT
                type,
                COUNT(*) as count
            FROM
                nodes
            GROUP BY
                type
            ORDER BY
                count DESC,
                type ASC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        node_data = list(rows)

        # Edges
        query = """
            SELECT
                source_node.type AS source_type,
                edges.type AS edge_type,
                target_node.type AS target_type,
                COUNT(*) AS triple_count
            FROM
                edges
            JOIN
                nodes AS source_node
            ON
                edges.source_id = source_node.id
            JOIN
                nodes AS target_node
            ON
                edges.target_id = target_node.id
            GROUP BY
                source_type, edge_type, target_type
            ORDER BY
                triple_count DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        edge_data = list(rows)

        # Graph
        g = nx.MultiDiGraph()
        for node_type, cnt_instances in node_data:
            size = cnt_instances
            color = node_type_to_color.get(node_type, None)
            hover = (
                f"Node type: {node_type}\n\n" f"Number of instances: {cnt_instances}"
            )
            g.add_node(
                node_type, size=size, color=color, label_color=color, hover=hover
            )

        for source_type, edge_type, target_type, cnt_instances in edge_data:
            size = cnt_instances
            color = node_type_to_color.get(source_type, None)
            hover = (
                f"Edge type: {edge_type}\n"
                f"Source: {source_type}\n"
                f"Target: {target_type}\n\n"
                f"Number of instances: {cnt_instances}"
            )
            label = edge_type
            g.add_edge(source_type, target_type, size=size, color=color, hover=hover, label=label, label_color="gray")

        # Plot
        fig = gv.d3(
            g,
            graph_height=800,
            show_node_label=True,
            show_edge_label=False,
            edge_curvature=0.1,
            use_node_size_normalization=True,
            node_size_normalization_min=10,
            node_size_normalization_max=50,
            node_drag_fix=True,
            node_hover_neighborhood=True,
            use_edge_size_normalization=True,
            edge_size_normalization_max=4,
            edge_label_data_source="label",
            many_body_force_strength=-3000,
            zoom_factor=1.0,
        )
        fig.export_html(schema_filepath)


def sqlite_to_schema_compact(db_filepath, schema_filepath, node_type_to_color=None):
    if node_type_to_color is None:
        node_type_to_color = {}

    with sqlite3.connect(db_filepath) as conn:
        cursor = conn.cursor()

        # Nodes
        query = """
            SELECT
                type, COUNT(*) as count
            FROM
                nodes
            GROUP BY
                type
            ORDER BY
                count DESC,
                type ASC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        node_data = list(rows)

        # Edges
        query = """
            SELECT
                source_node.type AS source_type,
                target_node.type AS target_type,
                COUNT(*) AS duple_count,
                COUNT(DISTINCT edges.type) AS distinct_edge_types_count
            FROM
                edges
            JOIN
                nodes AS source_node
            ON
                edges.source_id = source_node.id
            JOIN
                nodes AS target_node
            ON
                edges.target_id = target_node.id
            GROUP BY
                source_type, target_type
            ORDER BY
                duple_count DESC
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        edge_data = list(rows)

        # Graph
        g = nx.DiGraph()
        for node_type, cnt_instances in node_data:
            size = cnt_instances
            color = node_type_to_color.get(node_type, None)
            hover = (
                f"Node type: {node_type}\n\n" f"Number of instances: {cnt_instances}"
            )
            g.add_node(
                node_type, size=size, color=color, label_color=color, hover=hover
            )

        for source_type, target_type, cnt_instances, cnt_types in edge_data:
            size = cnt_instances
            color = node_type_to_color.get(source_type, None)
            hover = (
                f"Source: {source_type}\n"
                f"Target: {target_type}\n\n"
                f"Number of instances: {cnt_instances}\n"
                f"Number of represented edge types: {cnt_types}"
            )
            g.add_edge(source_type, target_type, size=size, color=color, hover=hover)

        # Plot
        fig = gv.d3(
            g,
            graph_height=800,
            show_node_label=True,
            show_edge_label=False,
            edge_curvature=0.1,
            use_node_size_normalization=True,
            node_size_normalization_min=10,
            node_size_normalization_max=50,
            node_drag_fix=True,
            node_hover_neighborhood=True,
            use_edge_size_normalization=True,
            edge_size_normalization_max=4,
            many_body_force_strength=-3000,
            zoom_factor=1.0,
        )
        fig.export_html(schema_filepath)


def sqlite_to_sql(db_filepath, sql_filepath, batch_size=10_000):
    # https://docs.python.org/3/library/sqlite3.html#sqlite3.Connection.iterdump
    with sqlite3.connect(db_filepath) as conn:
        with open(sql_filepath, "w") as f:
            for line in conn.iterdump():
                f.write(f"{line}\n")


def sqlite_to_csv(db_filepath, csv_filepath, table_name, batch_size=10_000):
    if table_name not in ("nodes", "edges"):
        message = f"Unknown table name: {table_name}"
        raise ValueError(message)

    with sqlite3.connect(db_filepath) as conn:
        with open(csv_filepath, "w", newline="") as f:
            cursor = conn.cursor()
            writer = csv.writer(f, dialect="excel", quoting=csv.QUOTE_ALL)
            # Header
            query = f"SELECT * FROM {table_name} LIMIT 0"
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            writer.writerow(columns)
            # Data
            query = f"SELECT * FROM {table_name}"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                writer.writerows(rows)


def sqlite_to_jsonl(db_filepath, jsonl_filepath, table_name, batch_size=10_000):
    if table_name == "nodes":
        query = """
            SELECT
                json_object(
                    'id', id,
                    'type', type,
                    'properties', json(properties)
                )
            FROM
                nodes
        """
        with sqlite3.connect(db_filepath) as conn:
            with open(jsonl_filepath, "w") as f:
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    f.write("\n".join(r[0] for r in rows))
                    f.write("\n")
    elif table_name == "edges":
        query = """
            SELECT
                json_object(
                    'source_id', source_id,
                    'target_id', target_id,
                    'type', type,
                    'properties', json(properties)
                )
            FROM
                edges
        """
        with sqlite3.connect(db_filepath) as conn:
            with open(jsonl_filepath, "w") as f:
                cursor = conn.cursor()
                cursor.execute(query)
                while True:
                    rows = cursor.fetchmany(batch_size)
                    if not rows:
                        break
                    f.write("\n".join(r[0] for r in rows))
                    f.write("\n")
    else:
        message = f"Unknown table name: {table_name}"
        raise ValueError(message)


def sqlite_to_graphml(db_filepath, graphml_filepath, batch_size=10_000):
    # http://graphml.graphdrawing.org
    def clean_id(item):
        return item.replace("\\", '\\\\"').replace('"', '\\"')

    def clean_val(item):
        if isinstance(item, (bool, int, float)):
            return item
        else:
            item_str = orjson.dumps(item).decode("utf-8")
            item_xml = saxutils.escape(item_str)
            return item_xml

    with sqlite3.connect(db_filepath) as conn:
        with open(graphml_filepath, "w") as f:
            cursor = conn.cursor()

            # Head
            head = """<?xml version="1.0" encoding="UTF-8"?>
<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
                             http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
"""
            f.write(head)

            # Attributes
            def attr_val_to_type(val):
                attr_type = "string"
                if isinstance(val, bool):
                    attr_type = "boolean"
                elif isinstance(val, int):
                    attr_type = "long"  # alternative: "int"
                elif isinstance(val, float):
                    attr_type = "double"  # alternative: "float"
                return attr_type

            attributes = {}
            attr_cnt = 0
            # - Collect node attributes
            attributes["_node_type"] = {
                "id": f"d{attr_cnt}",
                "for": "node",
                "name": "node_type",
                "type": "string",
            }
            attr_cnt += 1
            query = "SELECT properties FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for (properties_str,) in rows:
                    properties = orjson.loads(properties_str)
                    for name, val in properties.items():
                        key = f"node_{name}"
                        if key not in attributes:
                            attributes[key] = {
                                "id": f"d{attr_cnt}",
                                "for": "node",
                                "name": name,
                                "type": attr_val_to_type(val),
                            }
                            attr_cnt += 1
            # - Collect edge attributes
            attributes["_edge_type"] = {
                "id": f"d{attr_cnt}",
                "for": "edge",
                "name": "edge_type",
                "type": "string",
            }
            attr_cnt += 1
            query = "SELECT properties FROM edges"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for (properties_str,) in rows:
                    properties = orjson.loads(properties_str)
                    for name, val in properties.items():
                        key = f"edge_{name}"
                        if key not in attributes:
                            attributes[key] = {
                                "id": f"d{attr_cnt}",
                                "for": "edge",
                                "name": name,
                                "type": attr_val_to_type(val),
                            }
                            attr_cnt += 1
            # - Write both
            lines = []
            for _, attr in attributes.items():
                lines.append(
                    "<"
                    f'key id="{attr["id"]}" '
                    f'for="{attr["for"]}" '
                    f'attr.name="{attr["name"]}" '
                    f'attr.type="{attr["type"]}"'
                    "/>"
                )
            f.write("\n".join(lines))

            # Graph
            f.write('\n<graph id="knowledge_graph" edgedefault="directed">\n')

            # Nodes
            query = "SELECT * FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for node_id, node_type, node_properties in rows:
                    lines = []
                    lines.append(f'<node id="{clean_id(node_id)}">')
                    attr = {"_node_type": node_type}
                    for k, v in orjson.loads(node_properties).items():
                        key = f"node_{k}"
                        attr[key] = v
                    for key, val in attr.items():
                        lines.append(
                            f' <data key="{attributes[key]["id"]}">{clean_val(val)}</data>'
                        )
                    lines.append(" </node>")
                    result.extend(lines)
                f.write("\n".join(result))
                f.write("\n")

            # Edges
            query = "SELECT * FROM edges"
            cursor.execute(query)
            edge_id = 0
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for source_id, target_id, edge_type, edge_properties in rows:
                    lines = []
                    lines.append(
                        f'<edge id="{edge_id}" '
                        f'source="{clean_id(source_id)}" '
                        f'target="{clean_id(target_id)}">'
                    )
                    attr = {"_edge_type": edge_type}
                    for k, v in orjson.loads(edge_properties).items():
                        key = f"edge_{k}"
                        attr[key] = v
                    for key, val in attr.items():
                        lines.append(
                            f' <data key="{attributes[key]["id"]}">{clean_val(val)}</data>'
                        )
                    lines.append("</edge>")
                    result.extend(lines)
                    edge_id += 1
                f.write("\n".join(result))
                f.write("\n")

            # Tail
            tail = """
</graph>
</graphml>
"""
            f.write(tail)


def clean(item):
    if isinstance(item, (int, float)):
        s = str(item)
    else:
        s = orjson.dumps(item).decode("utf-8")
        if not s.startswith('"'):
            s = orjson.dumps(s).decode("utf-8")
    return s


def sqlite_to_metta_repr1(db_filepath, metta_filepath, batch_size=10_000):
    with sqlite3.connect(db_filepath) as conn:
        with open(metta_filepath, "w") as f:
            cursor = conn.cursor()

            # Types
            f.write("; Types\n")
            # Node types
            f.write("(: NodeType Type)\n")
            query = "SELECT DISTINCT type FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for (node_type,) in rows:
                    # (: node_type NodeType)
                    node_type = clean(node_type)
                    line = f"(: {node_type} NodeType)"
                    result.append(line)
                f.write("\n".join(result))
                f.write("\n")

            # Nodes
            f.write("; Nodes\n")
            query = "SELECT * FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for node_id, node_type, _ in rows:
                    node_id = clean(node_id)
                    node_type = clean(node_type)
                    # node type definitions:
                    #   (: node_id node_type)
                    line = f"(: {node_id} {node_type})"
                    result.append(line)
                f.write("\n".join(result))
                f.write("\n")

            # Edges
            f.write("; Edges\n")
            query = "SELECT * FROM edges"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for source_id, target_id, edge_type, _ in rows:
                    source_id = clean(source_id)
                    target_id = clean(target_id)
                    edge_type = clean(edge_type)
                    line = f"({source_id} {edge_type} {target_id})"
                    result.append(line)
                f.write("\n".join(result))
                f.write("\n")


def sqlite_to_metta_repr2(db_filepath, metta_filepath, batch_size=10_000):
    property_relation = '"has_property"'

    with sqlite3.connect(db_filepath) as conn:
        with open(metta_filepath, "w") as f:
            cursor = conn.cursor()

            # Types
            f.write("; Types\n")
            f.write("(: NodeType Type)\n")
            f.write("(: EdgeType Type)\n")
            # 1. Node types
            query = "SELECT DISTINCT type FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for (node_type,) in rows:
                    # (: node_type NodeType)
                    node_type = clean(node_type)
                    line = f"(: {node_type} NodeType)"
                    result.append(line)
                f.write("\n".join(result))
                f.write("\n")
            # 2. Edge types
            query = "SELECT DISTINCT type FROM edges"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for (edge_type,) in rows:
                    # (: edge_type Edge)
                    edge_type = clean(edge_type)
                    line = f"(: {edge_type} EdgeType)"
                    result.append(line)
                f.write("\n".join(result))
                f.write("\n")

            # Nodes, their types, and their properties
            f.write("; Nodes\n")
            query = "SELECT * FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for node_id, node_type, node_properties_str in rows:
                    node_id = clean(node_id)
                    node_type = clean(node_type)
                    node_properties = orjson.loads(node_properties_str)
                    # node type definitions:
                    #   (: node_id node_type)
                    line = f"(: {node_id} {node_type})"
                    result.append(line)
                    # node properties:
                    #   (has_property node_id (node_property_key node_property_value))
                    for key, val in node_properties.items():
                        key = clean(key)
                        val = clean(val)
                        line = f"({property_relation} {node_id} ({key} {val}))"
                        result.append(line)
                f.write("\n".join(result))
                f.write("\n")

            # Edges, their types, and their properties
            f.write("; Edges\n")
            query = "SELECT * FROM edges"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for i, (
                    source_id,
                    target_id,
                    edge_type,
                    edge_properties_str,
                ) in enumerate(rows):
                    edge_id = f'"e{i}"'
                    source_id = clean(source_id)
                    target_id = clean(target_id)
                    edge_type = clean(edge_type)
                    edge_properties = orjson.loads(edge_properties_str)
                    # edge type definitions:
                    #   (: edge_id edge_type)
                    line = f"(: {edge_id} {edge_type})"
                    result.append(line)
                    # edges in prefix notation:
                    #   (edge_id (predicate subject object))
                    line = f"({edge_id} ({edge_type} {source_id} {target_id}))"
                    result.append(line)

                    # edge properties:
                    #   (has_property edge_id (edge_property_key edge_property_value))
                    for key, val in edge_properties.items():
                        key = clean(key)
                        val = clean(val)
                        line = f"({property_relation} {edge_id} ({key} {val}))"
                        result.append(line)
                f.write("\n".join(result))
                f.write("\n")


def sqlite_to_metta_repr3(db_filepath, metta_filepath, batch_size=10_000):
    def dict_generator(d, pre=None):
        pre = pre[:] if pre else []
        if isinstance(d, dict):
            for key, value in d.items():
                if isinstance(value, dict):
                    yield from dict_generator(value, pre + [key])
                elif isinstance(value, list) or isinstance(value, tuple):
                    for k0, v0 in enumerate(value):
                        if isinstance(v0, list) or isinstance(v0, tuple):
                            for k1, v1 in enumerate(v0):
                                if isinstance(v1, list) or isinstance(v1, tuple):
                                    for k2, v2 in enumerate(v1):
                                        if isinstance(v2, list) or isinstance(
                                            v2, tuple
                                        ):
                                            for k3, v3 in enumerate(v2):
                                                if isinstance(v3, list) or isinstance(
                                                    v3, tuple
                                                ):
                                                    for k4, v4 in enumerate(v3):
                                                        if isinstance(
                                                            v4, list
                                                        ) or isinstance(v4, tuple):
                                                            raise NotImplementedError()
                                                        else:
                                                            yield from dict_generator(
                                                                v4,
                                                                pre
                                                                + [
                                                                    (
                                                                        key,
                                                                        k0,
                                                                        k1,
                                                                        k2,
                                                                        k3,
                                                                        k4,
                                                                    )
                                                                ],
                                                            )
                                                else:
                                                    yield from dict_generator(
                                                        v3,
                                                        pre + [(key, k0, k1, k2, k3)],
                                                    )
                                        else:
                                            yield from dict_generator(
                                                v2, pre + [(key, k0, k1, k2)]
                                            )
                                else:
                                    yield from dict_generator(v1, pre + [(key, k0, k1)])
                        else:
                            yield from dict_generator(v0, pre + [(key, k0)])
                else:
                    yield pre + [key, value]
        else:
            yield pre + [d]

    def dict_to_metta(data):
        output = []
        for path in dict_generator(data):
            s = path[-1]
            if isinstance(s, str):
                s = clean(s)
            for item in reversed(path[:-1]):
                if isinstance(item, tuple):
                    item_str = " ".join(clean(x) for x in item)
                    s = f"({item_str} {s})"
                else:
                    s = f"({clean(item)} {s})"
            output.append(s)
        return output

    with sqlite3.connect(db_filepath) as conn:
        with open(metta_filepath, "w") as f:
            cursor = conn.cursor()
            cnt = 0

            # Nodes
            query = "SELECT * FROM nodes"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for node_id, node_type, node_properties_str in rows:
                    node_properties = orjson.loads(node_properties_str)
                    item = {
                        cnt: {
                            "id": node_id,
                            "type": node_type,
                            "properties": node_properties,
                        }
                    }
                    metta_lines = dict_to_metta(item)
                    result.extend(metta_lines)
                    cnt += 1
                f.write("\n".join(result))
                f.write("\n")

            # Edges
            query = "SELECT * FROM edges"
            cursor.execute(query)
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                result = []
                for source_id, target_id, edge_type, edge_properties_str in rows:
                    edge_properties = orjson.loads(edge_properties_str)
                    item = {
                        cnt: {
                            "source_id": source_id,
                            "target_id": target_id,
                            "type": edge_type,
                            "properties": edge_properties,
                        }
                    }
                    metta_lines = dict_to_metta(item)
                    result.extend(metta_lines)
                    cnt += 1
                f.write("\n".join(result))
                f.write("\n")
