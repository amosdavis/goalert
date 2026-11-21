"""Read the table written by TestWriteInteropTable with delta-rs, an
independent Delta Lake implementation, and check that it sees exactly what
GoAlert wrote (docs/filestore-failure-modes.md, F35).

Usage: python check_interop.py DIR
"""

import datetime
import json
import os
import sys

from deltalake import DeltaTable


def plain(value):
    """Convert delta-rs values to the JSON forms expected.json uses."""
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            raise AssertionError(f"timestamp {value} has no time zone")
        return value.astimezone(datetime.timezone.utc).isoformat(timespec="microseconds")
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain(v) for v in value]
    return value


def main(directory):
    with open(f"{directory}/expected.json") as f:
        expected = json.load(f)

    table_dir = os.path.join(directory, "table")
    table = DeltaTable(table_dir)
    assert table.version() == expected["version"], (table.version(), expected["version"])

    # file_uris gives local files as plain absolute paths.
    root = os.path.realpath(table_dir)
    files = sorted(
        os.path.relpath(os.path.realpath(uri), root).replace(os.sep, "/")
        for uri in table.file_uris()
    )
    assert files == sorted(expected["files"]), (files, expected["files"])

    rows = table.to_pyarrow_table().to_pylist()
    for name in ("alerts", "users"):
        got = sorted(
            (plain(r[name]) for r in rows if r["tbl"] == name),
            key=lambda r: r["id"],
        )
        if got != expected[name]:
            print(f"{name} rows differ:\n got:  {json.dumps(got, indent=2)}\n want: {json.dumps(expected[name], indent=2)}")
            return 1
        for r in rows:
            if r["tbl"] != name:
                assert r[name] is None, f"a {r['tbl']} row has a {name} value"

    print(f"delta-rs read version {table.version()}: {len(rows)} rows, all as GoAlert wrote them")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1]))
