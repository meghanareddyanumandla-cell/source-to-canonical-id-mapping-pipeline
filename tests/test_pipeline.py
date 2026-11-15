"""End-to-end checks for matching, audit trails, and retry behavior."""

import csv
import json
import sqlite3

import pytest

from src.canonical import generate_canonical_id
from src.pipeline import run_pipeline
from src.sample_data import generate


CSV_COLUMNS = (
    "source_system",
    "source_id",
    "name",
    "email",
    "phone",
    "address",
    "age",
)


def write_csv(path, rows, columns=CSV_COLUMNS):
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def record(system, key, name, email, phone, age="30"):
    return {
        "source_system": system,
        "source_id": key,
        "name": name,
        "email": email,
        "phone": phone,
        "address": "synthetic",
        "age": age,
    }


def test_two_sources_map_to_the_same_stable_entities(tmp_path):
    input_dir = tmp_path / "input"
    generate(input_dir, rows=3, seed=9, include_edge_cases=False)

    result = run_pipeline(
        input_dir,
        tmp_path / "catalog.sqlite",
        tmp_path / "reports",
        run_id="happy",
    )

    assert result["input_rows"] == 6
    assert result["accepted_rows"] == 6
    assert result["mapped_rows"] == 6
    assert result["rejected_rows"] == 0

    with sqlite3.connect(tmp_path / "catalog.sqlite") as database:
        assert database.execute("SELECT COUNT(*) FROM canonical_entities").fetchone()[0] == 3
        assert database.execute("SELECT COUNT(*) FROM id_mappings").fetchone()[0] == 6
        distinct_ids = database.execute(
            "SELECT COUNT(DISTINCT canonical_id) FROM id_mappings"
        ).fetchone()[0]
        assert distinct_ids == 3
        event_count = database.execute(
            "SELECT COUNT(*) FROM mapping_history WHERE run_id = 'happy'"
        ).fetchone()[0]
        assert event_count == 6
        assert database.execute("PRAGMA foreign_key_check").fetchall() == []

    assert (tmp_path / "reports" / "id_mappings.csv").exists()


def test_reusing_a_run_id_is_idempotent(tmp_path):
    input_dir = tmp_path / "input"
    generate(input_dir, rows=2, include_edge_cases=False)
    database_path = tmp_path / "catalog.sqlite"
    output_dir = tmp_path / "reports"

    run_pipeline(input_dir, database_path, output_dir, run_id="retry")
    run_pipeline(input_dir, database_path, output_dir, run_id="retry")

    with sqlite3.connect(database_path) as database:
        run_count = database.execute(
            "SELECT COUNT(*) FROM pipeline_runs WHERE run_id = 'retry'"
        ).fetchone()[0]
        event_count = database.execute(
            "SELECT COUNT(*) FROM mapping_history WHERE run_id = 'retry'"
        ).fetchone()[0]
        current_count = database.execute("SELECT COUNT(*) FROM id_mappings").fetchone()[0]
        assert (run_count, event_count, current_count) == (1, 4, 4)

    run_pipeline(input_dir, database_path, output_dir, run_id="backfill")
    with sqlite3.connect(database_path) as database:
        assert database.execute("SELECT COUNT(*) FROM mapping_history").fetchone()[0] == 8


def test_empty_input_is_a_successful_run(tmp_path):
    input_dir = tmp_path / "empty"
    input_dir.mkdir()

    result = run_pipeline(
        input_dir,
        tmp_path / "catalog.sqlite",
        tmp_path / "reports",
        run_id="empty",
    )

    assert result["status"] == "succeeded"
    assert result["input_rows"] == 0
    assert result["accepted_rows"] == 0


def test_invalid_rows_are_quarantined_with_source_location(tmp_path):
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    write_csv(
        input_dir / "source_a.csv",
        [
            record("a", "bad", "", "not-email", "123", "abc"),
            record("a", "ok", "No Contact", "", "", ""),
        ],
    )

    result = run_pipeline(
        input_dir,
        tmp_path / "catalog.sqlite",
        tmp_path / "reports",
        run_id="quality",
    )

    assert result["rejected_rows"] == 1
    assert result["unresolved_rows"] == 1
    with (tmp_path / "reports" / "rejects_quality.csv").open(
        encoding="utf-8", newline=""
    ) as source:
        rejected = next(csv.DictReader(source))
    assert rejected["source_file"] == "source_a.csv"
    assert rejected["source_row"] == "2"
    issues = set(json.loads(rejected["issues"]))
    assert {"invalid_email", "invalid_phone", "invalid_age", "missing_required:name"} <= issues

    with sqlite3.connect(tmp_path / "catalog.sqlite") as database:
        assert database.execute("SELECT COUNT(*) FROM rejects").fetchone()[0] == 1


def test_every_occurrence_of_a_duplicate_source_key_is_rejected(tmp_path):
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    write_csv(
        input_dir / "source_a.csv",
        [
            record("a", "same", "First", "first@example.test", "2025550100"),
            record("a", "same", "Second", "second@example.test", "2025550101"),
        ],
    )

    result = run_pipeline(
        input_dir,
        tmp_path / "catalog.sqlite",
        tmp_path / "reports",
        run_id="duplicate",
    )

    assert result["rejected_rows"] == 2
    assert result["accepted_rows"] == 0
    with sqlite3.connect(tmp_path / "catalog.sqlite") as database:
        assert database.execute("SELECT COUNT(*) FROM rejects").fetchone()[0] == 2


def test_email_and_phone_resolving_to_different_people_is_a_conflict(tmp_path):
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    write_csv(
        input_dir / "source_a.csv",
        [
            record("a", "one", "One", "one@example.test", "2025550101"),
            record("a", "two", "Two", "two@example.test", "2025550102"),
        ],
    )
    database_path = tmp_path / "catalog.sqlite"
    output_dir = tmp_path / "reports"
    run_pipeline(input_dir, database_path, output_dir, run_id="seed")

    (input_dir / "source_a.csv").unlink()
    write_csv(
        input_dir / "source_b.csv",
        [record("b", "three", "Three", "one@example.test", "2025550102")],
    )
    result = run_pipeline(input_dir, database_path, output_dir, run_id="conflict")

    assert result["conflict_rows"] == 1
    assert result["accepted_rows"] == 1
    assert result["mapped_rows"] == 0
    with sqlite3.connect(database_path) as database:
        reason = database.execute(
            "SELECT reason FROM mapping_conflicts WHERE run_id = 'conflict'"
        ).fetchone()[0]
    assert reason == "email_and_phone_point_to_different_entities"


def test_phone_formatting_and_late_records_resolve_to_existing_entity(tmp_path):
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    write_csv(
        input_dir / "source_a.csv",
        [record("a", "1", "One", "one@example.test", "+1 (202) 555-0101")],
    )
    database_path = tmp_path / "catalog.sqlite"
    output_dir = tmp_path / "reports"
    run_pipeline(input_dir, database_path, output_dir, run_id="early")

    with sqlite3.connect(database_path) as database:
        first_id = database.execute(
            "SELECT canonical_id FROM id_mappings"
        ).fetchone()[0]

    (input_dir / "source_a.csv").unlink()
    write_csv(
        input_dir / "source_b.csv",
        [record("b", "late", "One", "", "12025550101")],
    )
    result = run_pipeline(input_dir, database_path, output_dir, run_id="late")

    assert result["mapped_rows"] == 1
    with sqlite3.connect(database_path) as database:
        late_id = database.execute(
            "SELECT canonical_id FROM id_mappings WHERE source_id = 'late'"
        ).fetchone()[0]
    assert late_id == first_id


def test_schema_change_is_recorded_as_a_failed_run(tmp_path):
    input_dir = tmp_path / "data"
    input_dir.mkdir()
    write_csv(input_dir / "source_a.csv", [{"wrong": "header"}], columns=("wrong",))
    database_path = tmp_path / "catalog.sqlite"

    with pytest.raises(ValueError):
        run_pipeline(input_dir, database_path, tmp_path / "reports", run_id="schema")

    with sqlite3.connect(database_path) as database:
        status, error = database.execute(
            "SELECT status, error FROM pipeline_runs WHERE run_id = 'schema'"
        ).fetchone()
    assert status == "failed"
    assert "missing required columns" in error


def test_missing_input_directory_is_recorded_as_a_failure(tmp_path):
    database_path = tmp_path / "catalog.sqlite"

    with pytest.raises(FileNotFoundError):
        run_pipeline(
            tmp_path / "absent",
            database_path,
            tmp_path / "reports",
            run_id="missing",
        )

    with sqlite3.connect(database_path) as database:
        status = database.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = 'missing'"
        ).fetchone()[0]
    assert status == "failed"


def test_canonical_ids_are_stable_without_case_differences():
    first = generate_canonical_id("System", "abc")
    second = generate_canonical_id("system", "ABC")
    assert first == second


def test_generator_is_repeatable_and_includes_labelled_edge_cases(tmp_path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    generate(first_dir, rows=2, seed=17)
    generate(second_dir, rows=2, seed=17)

    first_bytes = (first_dir / "source_a.csv").read_bytes()
    second_bytes = (second_dir / "source_a.csv").read_bytes()
    assert first_bytes == second_bytes

    with (first_dir / "source_a.csv").open(encoding="utf-8", newline="") as source:
        rows = list(csv.DictReader(source))
    assert any(row["source_id"] == "INVALID-MISSING-NAME" for row in rows)
    assert rows[0]["name"].replace(" ", "").isalpha()
    assert rows[0]["email"].endswith("@example.test")
    duplicate_count = sum(row["source_id"] == "A0000000" for row in rows)
    assert duplicate_count == 2


def test_failed_write_rolls_back_and_same_run_id_can_retry(tmp_path, monkeypatch):
    input_dir = tmp_path / "input"
    generate(input_dir, rows=1, include_edge_cases=False)
    database_path = tmp_path / "catalog.sqlite"
    output_dir = tmp_path / "reports"

    import src.pipeline as pipeline

    real_publish = pipeline.publish_success

    def fail_publish(*args, **kwargs):
        raise sqlite3.OperationalError("simulated write failure")

    monkeypatch.setattr(pipeline, "publish_success", fail_publish)
    with pytest.raises(sqlite3.OperationalError):
        pipeline.run_pipeline(input_dir, database_path, output_dir, run_id="partial")

    with sqlite3.connect(database_path) as database:
        status = database.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = 'partial'"
        ).fetchone()[0]
        mapping_count = database.execute("SELECT COUNT(*) FROM id_mappings").fetchone()[0]
    assert status == "failed"
    assert mapping_count == 0

    monkeypatch.setattr(pipeline, "publish_success", real_publish)
    result = pipeline.run_pipeline(
        input_dir, database_path, output_dir, run_id="partial"
    )
    assert result["status"] == "succeeded"

    with sqlite3.connect(database_path) as database:
        run_count = database.execute(
            "SELECT COUNT(*) FROM pipeline_runs WHERE run_id = 'partial'"
        ).fetchone()[0]
        event_count = database.execute(
            "SELECT COUNT(*) FROM mapping_history WHERE run_id = 'partial'"
        ).fetchone()[0]
    assert run_count == 1
    assert event_count == 2


def test_blank_source_system_is_rejected_instead_of_guessed_from_filename(tmp_path):
    input_dir = tmp_path / "input"
    input_dir.mkdir()
    write_csv(
        input_dir / "source_a.csv",
        [record("", "key-1", "Name", "name@example.test", "2025550100")],
    )

    result = run_pipeline(
        input_dir,
        tmp_path / "catalog.sqlite",
        tmp_path / "reports",
        run_id="missing-system",
    )

    assert result["rejected_rows"] == 1
    with sqlite3.connect(tmp_path / "catalog.sqlite") as database:
        issue_codes = database.execute(
            "SELECT issues FROM rejects WHERE run_id = 'missing-system'"
        ).fetchone()[0]
    assert "missing_required:source_system" in issue_codes
