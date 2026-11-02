#!/usr/bin/env python3
"""Write the TiDB schema facts the capture converter needs: table IDs, handle kind, index IDs and column types."""
import argparse
import json

import pymysql

INT = {"tinyint", "smallint", "mediumint", "int", "bigint"}
TEXT = {"char", "varchar", "text"}


def kind(data_type):
    return "int" if data_type in INT else "text" if data_type in TEXT else "unsupported:" + data_type


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True, help="host:port of a TiDB instance")
    parser.add_argument("--db", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    host, port = args.host.split(":")
    conn = pymysql.connect(host=host, port=int(port), user="root", autocommit=True)
    with conn.cursor() as c:
        c.execute("SELECT TABLE_NAME, TIDB_TABLE_ID, TIDB_PK_TYPE FROM information_schema.tables "
                  "WHERE TABLE_SCHEMA = %s ORDER BY TIDB_TABLE_ID", (args.db,))
        tables = c.fetchall()
        c.execute("SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA = %s",
                  (args.db,))
        types = {(t, col.lower()): kind(dt) for t, col, dt in c.fetchall()}
        c.execute("SELECT TABLE_NAME, KEY_NAME, INDEX_ID, NON_UNIQUE, COLUMN_NAME, SEQ_IN_INDEX, CLUSTERED "
                  "FROM information_schema.tidb_indexes WHERE TABLE_SCHEMA = %s ORDER BY TABLE_NAME, INDEX_ID, SEQ_IN_INDEX",
                  (args.db,))
        indexes = c.fetchall()
    result = []
    for name, table_id, pk_type in tables:
        clustered = pk_type == "CLUSTERED"
        primary = [col for t, key, _, _, col, _, _ in indexes if t == name and key == "PRIMARY"]
        if not clustered:
            raise SystemExit("%s: non-clustered primary key (hidden _tidb_rowid handle) is not supported" % name)
        entry = {"id": table_id, "name": name, "pk_types": [types[(name, c.lower())] for c in primary],
                 "int_handle": len(primary) == 1 and types[(name, primary[0].lower())] == "int", "indexes": []}
        by_index = {}
        for t, key, index_id, non_unique, col, _, _ in indexes:
            if t == name and key != "PRIMARY":
                by_index.setdefault((index_id, key, non_unique), []).append(types[(name, col.lower())])
        for (index_id, key, non_unique), cols in sorted(by_index.items()):
            entry["indexes"].append({"id": index_id, "name": key, "unique": not non_unique, "types": cols})
        result.append(entry)
    with open(args.out, "x") as out:
        json.dump({"database": args.db, "tables": result}, out, indent=2)


if __name__ == "__main__":
    main()
