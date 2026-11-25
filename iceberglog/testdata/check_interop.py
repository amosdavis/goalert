"""Read a table GoAlert wrote through an Iceberg catalog
(TestWriteIcebergInteropTable) with pyiceberg, an independent implementation,
and check that it sees exactly what GoAlert wrote
(docs/filestore-failure-modes.md, F35, F47).

DIR holds the table, iceberg.metadata.json and expected.json: the version, the
active files, and the rows of each GoAlert table.

Usage: python check_interop.py DIR
"""

import datetime
import json
import sys

from pyiceberg.table import StaticTable


def plain(value):
    """Convert values to the JSON forms expected.json uses."""
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
    table = StaticTable.from_metadata(f"{directory}/iceberg.metadata.json")

    summary = table.current_snapshot().summary
    assert summary["goalert.version"] == str(expected["version"]), summary

    prefix = table.location().rstrip("/") + "/"
    files = sorted(task.file.file_path for task in table.scan().plan_files())
    assert files == sorted(prefix + f for f in expected["files"]), files

    rows = {name: [] for name in expected["tables"]}
    for row in table.scan().to_arrow().to_pylist():
        present = {k: v for k, v in row.items() if v is not None}
        assert len(present) == 1, f"a row of one GoAlert table each: {row}"
        ((name, value),) = present.items()
        # GoAlert keeps its own sequences in tables of its own.
        if name in rows:
            rows[name].append(plain(value))
    for name, want in expected["tables"].items():
        got = sorted(rows[name], key=lambda r: r["id"])
        assert got == want, f"{name}:\n{got}\nwant:\n{want}"
    print(f"pyiceberg read version {expected['version']}: {len(files)} files, " + ", ".join(f"{len(r)} {n}" for n, r in rows.items()))


if __name__ == "__main__":
    main(sys.argv[1])
