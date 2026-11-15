"""SQLite schema, migrations, and transactional writes."""

import sqlite3
from contextlib import closing
from pathlib import Path


def _table_columns(connection, table_name):
    rows = connection.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in rows}


def _add_column_if_missing(connection, table_name, column_name, definition):
    if column_name not in _table_columns(connection, table_name):
        connection.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
        )


def create_database(database_path="mapping_pipeline.db"):
    """Create current tables and add columns missing from the earlier prototype."""
    Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database_path)) as connection, connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS canonical_entities (
                canonical_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                email TEXT,
                phone TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS id_mappings (
                source_system TEXT NOT NULL,
                source_id TEXT NOT NULL,
                canonical_id TEXT NOT NULL REFERENCES canonical_entities(canonical_id),
                source_file TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                last_seen_run_id TEXT NOT NULL,
                PRIMARY KEY (source_system, source_id)
            );
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                started_at TEXT NOT NULL,
                ended_at TEXT,
                status TEXT NOT NULL,
                input_rows INTEGER NOT NULL DEFAULT 0,
                accepted_rows INTEGER NOT NULL DEFAULT 0,
                mapped_rows INTEGER NOT NULL DEFAULT 0,
                rejected_rows INTEGER NOT NULL DEFAULT 0,
                conflict_rows INTEGER NOT NULL DEFAULT 0,
                unresolved_rows INTEGER NOT NULL DEFAULT 0,
                error TEXT
            );
            CREATE TABLE IF NOT EXISTS mapping_history (
                history_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source_system TEXT NOT NULL,
                source_id TEXT NOT NULL,
                canonical_id TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                mapped_at TEXT NOT NULL,
                UNIQUE (run_id, source_system, source_id)
            );
            CREATE TABLE IF NOT EXISTS rejects (
                reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source_system TEXT,
                source_id TEXT,
                source_file TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                issues TEXT NOT NULL,
                raw_record TEXT NOT NULL,
                UNIQUE (run_id, source_file, source_row)
            );
            CREATE TABLE IF NOT EXISTS unresolved_mappings (
                unresolved_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source_system TEXT,
                source_id TEXT,
                name TEXT,
                reason TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                UNIQUE (run_id, source_file, source_row)
            );
            CREATE TABLE IF NOT EXISTS mapping_conflicts (
                conflict_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                source_system TEXT,
                source_id TEXT,
                name TEXT,
                reason TEXT NOT NULL,
                source_file TEXT NOT NULL,
                source_row INTEGER NOT NULL,
                UNIQUE (run_id, source_file, source_row)
            );
            """
        )

        # Additive migration keeps the earlier prototype's current mappings and history.
        migrations = {
            "canonical_entities": (
                ("created_at", "TEXT NOT NULL DEFAULT ''"),
                ("updated_at", "TEXT NOT NULL DEFAULT ''"),
            ),
            "id_mappings": (
                ("source_file", "TEXT NOT NULL DEFAULT ''"),
                ("source_row", "INTEGER NOT NULL DEFAULT 0"),
                ("last_seen_run_id", "TEXT NOT NULL DEFAULT ''"),
            ),
            "pipeline_runs": (
                ("ended_at", "TEXT"),
                ("input_rows", "INTEGER NOT NULL DEFAULT 0"),
                ("accepted_rows", "INTEGER NOT NULL DEFAULT 0"),
                ("mapped_rows", "INTEGER NOT NULL DEFAULT 0"),
                ("rejected_rows", "INTEGER NOT NULL DEFAULT 0"),
                ("conflict_rows", "INTEGER NOT NULL DEFAULT 0"),
                ("unresolved_rows", "INTEGER NOT NULL DEFAULT 0"),
                ("error", "TEXT"),
            ),
            "mapping_history": (
                ("source_file", "TEXT NOT NULL DEFAULT ''"),
                ("source_row", "INTEGER NOT NULL DEFAULT 0"),
            ),
            "unresolved_mappings": (
                ("run_id", "TEXT NOT NULL DEFAULT 'legacy'"),
                ("source_file", "TEXT NOT NULL DEFAULT ''"),
                ("source_row", "INTEGER NOT NULL DEFAULT 0"),
            ),
            "mapping_conflicts": (
                ("run_id", "TEXT NOT NULL DEFAULT 'legacy'"),
                ("source_file", "TEXT NOT NULL DEFAULT ''"),
                ("source_row", "INTEGER NOT NULL DEFAULT 0"),
                ("reason", "TEXT NOT NULL DEFAULT 'legacy_conflict'"),
            ),
        }
        for table_name, columns in migrations.items():
            for column_name, definition in columns:
                _add_column_if_missing(
                    connection, table_name, column_name, definition
                )


def start_run(database_path, run_id, started_at):
    """Mark a run as active before reading inputs, so failures can be recorded."""
    with closing(sqlite3.connect(database_path)) as connection, connection:
        connection.execute(
            """
            INSERT INTO pipeline_runs (run_id, started_at, status)
            VALUES (?, ?, 'running')
            ON CONFLICT(run_id) DO UPDATE SET
                started_at = excluded.started_at,
                ended_at = NULL,
                status = 'running',
                input_rows = 0,
                accepted_rows = 0,
                mapped_rows = 0,
                rejected_rows = 0,
                conflict_rows = 0,
                unresolved_rows = 0,
                error = NULL
            """,
            (run_id, started_at),
        )


def load_entities(database_path):
    """Return the persisted canonical entities as dictionaries."""
    with closing(sqlite3.connect(database_path)) as connection:
        connection.row_factory = sqlite3.Row
        cursor = connection.execute(
            """
            SELECT canonical_id, name, email, phone, created_at, updated_at
            FROM canonical_entities
            """
        )
        return [dict(row) for row in cursor]


def publish_success(
    database_path,
    run,
    entities,
    mappings,
    rejects,
    conflicts,
    unresolved,
    observed_keys=(),
):
    """Commit current mappings and this run's audit data in one transaction."""
    connection = sqlite3.connect(database_path)
    try:
        with connection:
            audit_tables = (
                "mapping_history",
                "rejects",
                "mapping_conflicts",
                "unresolved_mappings",
            )
            for table_name in audit_tables:
                connection.execute(
                    f"DELETE FROM {table_name} WHERE run_id = ?", (run["run_id"],)
                )

            # A newly observed but unmatchable key must not keep an old current mapping.
            for source_system, source_id in observed_keys:
                connection.execute(
                    "DELETE FROM id_mappings WHERE source_system = ? AND source_id = ?",
                    (source_system, source_id),
                )

            for entity in entities:
                connection.execute(
                    """
                    INSERT INTO canonical_entities
                        (canonical_id, name, email, phone, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(canonical_id) DO UPDATE SET
                        name = excluded.name,
                        email = COALESCE(excluded.email, canonical_entities.email),
                        phone = COALESCE(excluded.phone, canonical_entities.phone),
                        updated_at = excluded.updated_at
                    """,
                    (
                        entity["canonical_id"],
                        entity["name"],
                        entity.get("email") or None,
                        entity.get("phone") or None,
                        entity["created_at"],
                        entity["updated_at"],
                    ),
                )

            for mapping in mappings:
                connection.execute(
                    """
                    INSERT INTO id_mappings
                        (source_system, source_id, canonical_id, source_file,
                         source_row, last_seen_run_id)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source_system, source_id) DO UPDATE SET
                        canonical_id = excluded.canonical_id,
                        source_file = excluded.source_file,
                        source_row = excluded.source_row,
                        last_seen_run_id = excluded.last_seen_run_id
                    """,
                    (
                        mapping["source_system"],
                        mapping["source_id"],
                        mapping["canonical_id"],
                        mapping["source_file"],
                        mapping["source_row"],
                        run["run_id"],
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO mapping_history
                        (run_id, source_system, source_id, canonical_id,
                         source_file, source_row, mapped_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run["run_id"],
                        mapping["source_system"],
                        mapping["source_id"],
                        mapping["canonical_id"],
                        mapping["source_file"],
                        mapping["source_row"],
                        run["ended_at"],
                    ),
                )

            for reject in rejects:
                connection.execute(
                    """
                    INSERT INTO rejects
                        (run_id, source_system, source_id, source_file,
                         source_row, issues, raw_record)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run["run_id"],
                        reject.get("source_system"),
                        reject.get("source_id"),
                        reject["source_file"],
                        reject["source_row"],
                        ";".join(reject["issues"]),
                        reject["raw_record_json"],
                    ),
                )

            for table_name, records in (
                ("mapping_conflicts", conflicts),
                ("unresolved_mappings", unresolved),
            ):
                for record in records:
                    connection.execute(
                        f"""
                        INSERT INTO {table_name}
                            (run_id, source_system, source_id, name, reason,
                             source_file, source_row)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            run["run_id"],
                            record.get("source_system"),
                            record.get("source_id"),
                            record.get("name"),
                            record["reason"],
                            record["source_file"],
                            record["source_row"],
                        ),
                    )

            connection.execute(
                """
                UPDATE pipeline_runs
                SET ended_at = ?, status = 'succeeded', input_rows = ?,
                    accepted_rows = ?, mapped_rows = ?, rejected_rows = ?,
                    conflict_rows = ?, unresolved_rows = ?, error = NULL
                WHERE run_id = ?
                """,
                (
                    run["ended_at"],
                    run["input_rows"],
                    run["accepted_rows"],
                    run["mapped_rows"],
                    run["rejected_rows"],
                    run["conflict_rows"],
                    run["unresolved_rows"],
                    run["run_id"],
                ),
            )
    finally:
        connection.close()


def fail_run(database_path, run_id, ended_at, counts, error):
    """Record a failed run after its processing transaction has rolled back."""
    with closing(sqlite3.connect(database_path)) as connection, connection:
        connection.execute(
            """
            UPDATE pipeline_runs
            SET ended_at = ?, status = 'failed', input_rows = ?,
                accepted_rows = ?, mapped_rows = 0, rejected_rows = ?,
                conflict_rows = ?, unresolved_rows = ?, error = ?
            WHERE run_id = ?
            """,
            (ended_at, *counts, error[:2000], run_id),
        )
