"""Read supplier CSV files and attach source lineage."""

import csv
from pathlib import Path

REQUIRED_COLUMNS = (
    "source_system",
    "source_id",
    "name",
    "email",
    "phone",
    "address",
    "age",
)


def read_source_file(path):
    """Read one CSV file after checking that its required headers exist."""
    path = Path(path)
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        headers = reader.fieldnames or []
        missing = [column for column in REQUIRED_COLUMNS if column not in headers]
        if missing:
            columns = ", ".join(missing)
            raise ValueError(f"{path.name}: missing required columns: {columns}")

        rows = []
        for row_number, record in enumerate(reader, start=2):
            cleaned = {
                key: value.strip() if isinstance(value, str) else value
                for key, value in record.items()
                if key is not None
            }
            cleaned["source_file"] = path.name
            cleaned["source_row"] = row_number
            rows.append(cleaned)

    return rows


def ingest_all_sources(directory):
    """Read every ``source_*.csv`` file in a directory."""
    directory = Path(directory)
    if not directory.exists():
        raise FileNotFoundError(f"Input directory does not exist: {directory}")

    paths = sorted(directory.glob("source_*.csv"))
    rows = []
    for path in paths:
        source_rows = read_source_file(path)
        rows.extend(source_rows)

    return rows
