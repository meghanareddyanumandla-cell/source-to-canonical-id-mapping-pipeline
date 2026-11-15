"""Run the identity-mapping batch and write its outputs."""

import argparse
import csv
import json
import logging
import sqlite3
import uuid
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from .canonical import generate_canonical_id
from .database import (
    create_database,
    fail_run,
    load_entities,
    publish_success,
    start_run,
)
from .ingestion import ingest_all_sources
from .matching import build_index, find_match, index_entity
from .validation import validate_records

logger = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _write_csv(path, rows, columns):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            serialized = {
                key: json.dumps(value, ensure_ascii=False)
                if isinstance(value, (list, dict))
                else value
                for key, value in row.items()
            }
            writer.writerow(serialized)


def _write_outputs(output_dir, run, entities, mappings, rejects, conflicts, unresolved):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_csv(
        output_dir / "canonical_entities.csv",
        entities,
        ("canonical_id", "name", "email", "phone", "created_at", "updated_at"),
    )
    _write_csv(
        output_dir / "id_mappings.csv",
        mappings,
        (
            "source_system",
            "source_id",
            "canonical_id",
            "source_file",
            "source_row",
            "last_seen_run_id",
        ),
    )

    run_id = run["run_id"]
    _write_csv(
        output_dir / f"rejects_{run_id}.csv",
        rejects,
        ("source_system", "source_id", "source_file", "source_row", "issues", "raw_record_json"),
    )
    review_columns = ("source_system", "source_id", "name", "reason", "source_file", "source_row")
    _write_csv(output_dir / f"conflicts_{run_id}.csv", conflicts, review_columns)
    _write_csv(output_dir / f"unresolved_{run_id}.csv", unresolved, review_columns)

    run_path = output_dir / f"run_{run_id}.json"
    run_path.write_text(json.dumps(run, indent=2), encoding="utf-8")

    report_lines = [
        "CUSTOMER IDENTITY MAPPING PIPELINE",
        "====================================",
        f"Run ID: {run_id}",
        f"Status: {run['status']}",
        f"Input rows: {run['input_rows']}",
        f"Accepted rows: {run['accepted_rows']}",
        f"Mapped rows: {run['mapped_rows']}",
        f"Rejected rows: {run['rejected_rows']}",
        f"Conflict rows: {run['conflict_rows']}",
        f"Unresolved rows: {run['unresolved_rows']}",
        f"Started at: {run['started_at']}",
        f"Ended at: {run['ended_at']}",
        "",
    ]
    (output_dir / "pipeline_report.txt").write_text(
        "\n".join(report_lines), encoding="utf-8"
    )


def _new_entity(record, canonical_id, timestamp):
    return {
        "canonical_id": canonical_id,
        "name": record["name"],
        "email": record.get("email") or None,
        "phone": record.get("phone") or None,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def _lineage(record):
    return {
        "source_system": record["source_system"],
        "source_id": record["source_id"],
        "name": record["name"],
        "source_file": record["source_file"],
        "source_row": record["source_row"],
    }


def run_pipeline(
    data_directory="data",
    database_path="mapping_pipeline.db",
    output_directory="reports",
    run_id=None,
):
    """Process one delivery; a reused run ID replaces that run's audit rows."""
    run_id = run_id or str(uuid.uuid4())
    started_at = utc_now()
    counts = [0, 0, 0, 0, 0]

    create_database(database_path)
    start_run(database_path, run_id, started_at)

    try:
        input_rows = ingest_all_sources(data_directory)
        counts[0] = len(input_rows)
        valid_rows, rejects = validate_records(input_rows)
        counts[2] = len(rejects)

        persisted_entities = load_entities(database_path)
        entities = [
            {
                **entity,
                "created_at": entity.get("created_at") or started_at,
                "updated_at": entity.get("updated_at") or started_at,
            }
            for entity in persisted_entities
        ]
        entities_by_id = {entity["canonical_id"]: entity for entity in entities}
        entity_index = build_index(entities)

        mappings = []
        conflicts = []
        unresolved = []

        for record in valid_rows:
            status, canonical_id, reason = find_match(record, entities, entity_index)
            source_details = _lineage(record)

            if status == "conflict":
                conflicts.append({**source_details, "reason": reason})
                continue

            if status == "unresolved" and reason == "no_match_identifiers":
                unresolved.append({**source_details, "reason": reason})
                continue

            if status == "unresolved":
                canonical_id = generate_canonical_id(
                    record["source_system"], record["source_id"]
                )
                entity = entities_by_id.get(canonical_id)
                if entity is None:
                    entity = _new_entity(record, canonical_id, started_at)
                    entities.append(entity)
                    entities_by_id[canonical_id] = entity
                    index_entity(entity_index, entity)
            else:
                entity = entities_by_id[canonical_id]
                if not entity.get("email") and record.get("email"):
                    entity["email"] = record["email"]
                if not entity.get("phone") and record.get("phone"):
                    entity["phone"] = record["phone"]
                entity["updated_at"] = started_at
                index_entity(entity_index, entity)

            mappings.append(
                {
                    "source_system": record["source_system"],
                    "source_id": record["source_id"],
                    "canonical_id": canonical_id,
                    "source_file": record["source_file"],
                    "source_row": record["source_row"],
                    "last_seen_run_id": run_id,
                }
            )

        counts[1] = len(mappings)
        counts[3] = len(conflicts)
        counts[4] = len(unresolved)

        for reject in rejects:
            raw_fields = {
                key: value
                for key, value in reject.items()
                if key not in {"issues", "raw_record_json"}
            }
            reject["raw_record_json"] = json.dumps(raw_fields, ensure_ascii=False)

        run = {
            "run_id": run_id,
            "started_at": started_at,
            "ended_at": utc_now(),
            "status": "succeeded",
            "input_rows": counts[0],
            "accepted_rows": len(valid_rows),
            "mapped_rows": counts[1],
            "rejected_rows": counts[2],
            "conflict_rows": counts[3],
            "unresolved_rows": counts[4],
            "error": None,
        }

        observed_keys = {
            (row["source_system"], row["source_id"])
            for row in input_rows
            if (row.get("source_system") or "").strip()
            and (row.get("source_id") or "").strip()
        }
        publish_success(
            database_path,
            run,
            entities,
            mappings,
            rejects,
            conflicts,
            unresolved,
            observed_keys,
        )

        with closing(sqlite3.connect(database_path)) as connection:
            connection.row_factory = sqlite3.Row
            query = """
                SELECT source_system, source_id, canonical_id,
                       source_file, source_row, last_seen_run_id
                FROM id_mappings
                ORDER BY source_system, source_id
            """
            current_mappings = [dict(row) for row in connection.execute(query)]

        _write_outputs(
            output_directory,
            run,
            entities,
            current_mappings,
            rejects,
            conflicts,
            unresolved,
        )
        logger.info(
            "Run %s succeeded: %s input, %s mapped, %s rejected, "
            "%s conflicts, %s unresolved",
            run_id,
            counts[0],
            counts[1],
            counts[2],
            counts[3],
            counts[4],
        )
        return run

    except Exception as error:
        logger.exception("Run %s failed", run_id)
        try:
            fail_run(
                database_path,
                run_id,
                utc_now(),
                counts,
                f"{type(error).__name__}: {error}",
            )
        except Exception:
            logger.exception("Could not record failure for run %s", run_id)
        raise


def main():
    parser = argparse.ArgumentParser(description="Map source keys to canonical IDs")
    parser.add_argument("--data", default="data", help="Folder containing source CSV files")
    parser.add_argument("--database", default="mapping_pipeline.db", help="SQLite database path")
    parser.add_argument("--output", default="reports", help="Folder for CSV and JSON outputs")
    parser.add_argument("--run-id", help="Stable ID to use when retrying a delivery")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
    )
    args = parser.parse_args()
    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    result = run_pipeline(args.data, args.database, args.output, args.run_id)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
