import orjson


def read_json_file(filepath):
    with open(filepath, "rb") as f:
        data = orjson.loads(f.read())
    return data


def create_sql_schema(cursor):
    query = """
    CREATE TABLE IF NOT EXISTS nodes (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL,
        properties TEXT
    );

    CREATE TABLE IF NOT EXISTS edges (
        source_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        type TEXT NOT NULL,
        properties TEXT,
        FOREIGN KEY (source_id) REFERENCES nodes (id),
        FOREIGN KEY (target_id) REFERENCES nodes (id)
    );

    CREATE INDEX IF NOT EXISTS idx_edges_source ON edges (source_id);
    CREATE INDEX IF NOT EXISTS idx_edges_target ON edges (target_id);
"""
    cursor.executescript(query)
