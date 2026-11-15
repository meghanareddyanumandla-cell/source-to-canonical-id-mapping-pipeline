# Customer identity mapping pipeline

A local, standard-library Python project that maps source customer keys to deterministic canonical IDs. Sample names are familiar-looking but generated; all sample email addresses use the reserved `example.test` domain. Exact normalized email and phone rules link records; cases where the two identifiers lead to different entities are recorded as conflicts. Rows without a usable identifier are unresolved. Invalid rows are retained in a source-lineage quarantine. SQLite is the durable store.

## Run locally

Requires Python 3.10+. From the project root (`C:\CandidateWorkspace`):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m src.sample_data --output data --rows 12 --seed 17
python -m src.pipeline --data data --database mapping_pipeline.db --output reports --run-id sample-001
python -m pytest
```

The generator streams reproducible paired source feeds. `--rows 100000` creates 100,000 records in each file. I put a clean large sample in `data/large/source_a.csv` and `data/large/source_b.csv`; it has 100,000 data rows per file. The regular `data/` folder stays small for quick runs. Generate the large sample again with `python -m src.sample_data --output data/large --rows 100000 --seed 17 --no-edge-cases`. By default the generator adds deliberately labelled invalid, duplicate-key, and no-contact rows; use `--no-edge-cases` for clean volume runs. Use a new `--run-id` for a new delivery. Reuse the same ID to retry the same logical delivery safely. The database is the source of truth; rerunning recreates CSV outputs from committed state.

All paths are configurable. Example: `python -m src.pipeline --data data --database data/output/catalog.sqlite --output data/output`. The input directory may be empty; malformed files or schema changes fail the run and are recorded in `pipeline_runs`. An input CSV is named `source_<label>.csv`, and its `source_system` column is authoritative.

## Folder guide

- `data/`: input `source_*.csv` files and generated samples.
- `src/`: ingestion, validation, matching, stable ID generation, SQLite persistence, pipeline, sample-data generator, and performance script.
- `tests/`: end-to-end and rule tests.
- `reports/`: current full snapshots (`canonical_entities.csv`, `id_mappings.csv`), run-specific quarantine/review CSVs, and `run_<run_id>.json`.
- `mapping_pipeline.db`: local SQLite database (configurable); back it up to retain history.

## Schema and grain

Source CSV required headers: `source_system`, `source_id`, `name`, `email`, `phone`, `address`, `age`. `source_system`, `source_id`, and `name` must be nonblank; email, phone, address, and age may be blank. Supplied email must have a basic email shape. Supplied phone can contain common punctuation and must contain 7–15 digits. Age, when supplied, must be a whole number from 0 to 120. A `(source_system, source_id)` duplicate within one delivery quarantines **all** rows for that key.

| Table | Grain / key | Important columns and rules |
|---|---|---|
| `canonical_entities` | One current row per inferred entity; `canonical_id` PK | UUID5-derived `CAN-...`; name, email, phone; created/updated UTC timestamps. |
| `id_mappings` | One current row per source key; `(source_system, source_id)` PK | Canonical FK, source file and row, last run ID. Upserted when a source key is seen again. |
| `mapping_history` | One mapping event per run and source key | `run_id`, source key, canonical ID, source lineage, mapped timestamp; same run ID retry replaces its own events. |
| `pipeline_runs` | One operational record per run; `run_id` PK | Start/end, status, input/accepted/mapped/rejected/conflict/unresolved counts, failure reason. |
| `rejects` | One invalid row per run and source row | Issue codes, raw row JSON, source file/row. |
| `mapping_conflicts` | One ambiguous row per run and source row | Reason plus source lineage; the pipeline does not auto-resolve it. |
| `unresolved_mappings` | One unmatchable row per run and source row | Reason plus source lineage. |

`accepted_rows` means rows passing validation and duplicate checks; `mapped_rows` is the accepted subset actually assigned a canonical ID. Thus `input_rows = rejected_rows + accepted_rows`, and `accepted_rows = mapped_rows + conflict_rows + unresolved_rows`. Current mapping tables are upserted; run history and review records are append-only across distinct run IDs. Within a reused run ID, audit rows are replaced so retry is idempotent.

## Flow and matching rules

```text
source_*.csv -> schema check -> row validation / duplicate quarantine
             -> exact email + phone resolution -> SQLite transaction
             -> current snapshots + run-specific reject/conflict/unresolved files
```

Email is trimmed and case-folded; phone matching compares digits only. A unique exact identifier match is accepted unless another supplied identifier contradicts the matched entity. If email and phone point to different canonical IDs, the row is a conflict. No identifiers or no exact match creates a new entity only when the source row itself has a stable source key and at least one identifier; otherwise it is unresolved. IDs are UUID5 based on the first source key creating an entity. Name/address/age are not fuzzy matching features.

## Reconciliation and review queries

```sql
SELECT status, input_rows, accepted_rows, mapped_rows, rejected_rows, conflict_rows, unresolved_rows, error FROM pipeline_runs ORDER BY started_at;
SELECT COUNT(*) AS current_entities FROM canonical_entities;
SELECT COUNT(*) AS current_source_mappings FROM id_mappings;
SELECT run_id, COUNT(*) AS mapped FROM mapping_history GROUP BY run_id ORDER BY run_id;
SELECT run_id, source_file, source_row, issues FROM rejects ORDER BY run_id, source_file, source_row;
SELECT run_id, source_system, source_id, reason FROM mapping_conflicts;
SELECT run_id, source_system, source_id, reason FROM unresolved_mappings;
PRAGMA foreign_key_check;
```

## Tests and performance

`python -m pytest` covers deterministic IDs, cross-source matching, incremental history, same-run retry, empty input, nulls and invalid values, duplicate keys, unexpected schema, failed-run capture, formatting normalization, and one-to-many identifier conflicts. `python -m src.performance --rows 100000` measures a reproducible clean 200,000-row ingest locally. It reports throughput; results depend on disk and machine. The first scaling improvement would be batched/chunked CSV processing and indexed normalized identifier columns; the current implementation favors a clear small-project design and holds the batch in memory.

## Recovery and limitations

The SQLite transaction writes mappings, audit rows, and run success together. A parse/validation/database failure rolls back data writes and records a failed run. Retry with the same `--run-id` after fixing the cause; duplicate mapping events for that run are removed/replaced. A distinct delivery uses a new run ID. Snapshot export follows the database commit, so rerunning after an export failure republishes snapshots from SQLite.

Matching is exact and conservative. Shared/recycled contacts may produce review cases; no probabilistic matching, human-resolution UI, API ingestion, cloud deployment, or PII encryption is included. Use synthetic data for demonstrations and protect real customer data and database backups.
