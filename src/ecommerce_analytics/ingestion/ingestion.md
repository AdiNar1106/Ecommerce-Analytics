# Ingestion Layer: How to Reproduce

This layer turns a static Kaggle dataset (REES46 e-commerce clickstream) into a realistic
daily event feed, then loads it into a raw (bronze) layer in DuckDB using dlt.

```
Kaggle CSV ──► data/landing/ ──► daily exports (with chaos) ──► dlt ──► data/warehouse.duckdb (raw.events)
 download.py                     make_exports.py                 load_events.py
```

---

## Prerequisites

| Tool | Check |
|---|---|
| Python 3.12 + uv | `uv --version` |
| DuckDB CLI | `duckdb -c "select 42"` |
| Kaggle credentials at `~/.kaggle/kaggle.json` (permissions `600`) | `uv run kaggle datasets files mkechinov/ecommerce-behavior-data-from-multi-category-store` |
| Python packages | `uv add duckdb dbt-duckdb dlt kaggle pandas pyarrow` |
| Free disk space | ~25 GB (raw CSV + compressed daily exports + warehouse) |

Accept the dataset's terms on the Kaggle page once in your browser if prompted.

`.gitignore` must include:

```
data/
*.duckdb
.venv/
target/
logs/
.env
```

Data source credit (required by the dataset license): REES46 Marketing Platform,
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store

---

## Project layout

```
ecommerce-analytics/
├── ingestion/
│   ├── download.py        # 1. pull a month from Kaggle
│   ├── explore.sql        # 2. profile the raw file
│   ├── make_exports.py    # 3. split into a daily feed + inject chaos
│   ├── load_events.py     # 4. load into raw.events with dlt
│   └── verify_load.sql    # 5. prove completeness + idempotency
├── data/                  # git-ignored
│   ├── landing/           # raw monthly CSVs
│   ├── exports/events/    # simulated vendor bucket, one folder per day
│   └── warehouse.duckdb   # the warehouse
└── ingestion.md           # this file
```

Run every command from the project root.

---

## Step 1: Download

```bash
uv run python ingestion/download.py --month 2019-Oct
```

- Output: `data/landing/2019-Oct.csv` (~5 GB, ~42M rows).
- Skips the download if the file already exists, so it is safe to rerun.
- Later: `--month 2019-Nov` for November (~9 GB).

---

## Step 2: Explore

```bash
duckdb < ingestion/explore.sql
```

Profiles the file before any pipeline decisions are made. Record your findings:

- [ ] Row count, date range, distinct users / sessions / products
- [ ] Event mix (view / cart / purchase)
- [ ] Null rates (`category_code` and `brand` are sparse)
- [ ] Exact duplicate rows already in the source
- [ ] Zero or negative prices
- [ ] Daily volume shape
- [ ] Products whose price changes (motivates a dbt snapshot later)
- [ ] Sessions shared by multiple users
- [ ] Top `category_code` values (a dotted hierarchy to split later)

---

## Step 3: Build the daily feed

```bash
uv run python ingestion/make_exports.py --month 2019-Oct
```

Simulates how a tracking vendor (Segment, Snowplow) delivers data:

| Added / injected | What it is | Rate |
|---|---|---|
| `event_id` | MD5 hash of the row, like Segment's `messageId` | every row |
| `received_at` | when the tracking server got the event | every row |
| Late events | arrive 1–3 days after they happened | ~1.0% |
| Duplicate deliveries | same `event_id` delivered again 1 hour later | ~0.5% |

- Files are partitioned by the day they were **received**:
  `data/exports/events/export_date=2019-10-01/from_2019-Oct_0.csv.gz`
- Chaos is derived from a hash of each event, so reruns produce identical output.
- Late October events spill into Nov 1–3 folders; the month in the filename keeps them
  from clashing with November's files.
- Low on RAM? Add `--memory-limit 2GB`.

---

## Step 4: Load into raw

Close any DuckDB session that has `data/warehouse.duckdb` open first (one writer at a time).

```bash
# Replay the feed like a real nightly job
uv run python ingestion/load_events.py --through 2019-10-07   # backfill the first week
uv run python ingestion/load_events.py --date 2019-10-08      # "tonight's batch"
uv run python ingestion/load_events.py                        # everything not yet loaded

# Redo a day (backfill / fix) without creating duplicates
uv run python ingestion/load_events.py --date 2019-10-08 --reload
```

Output: table `raw.events` in `data/warehouse.duckdb`.

**What lands in raw**

- All source columns as text (typing happens in dbt staging).
- Duplicates and late events are kept on purpose.
- Lineage columns: `_source_file`, `_export_date`, `_extracted_at`.

**How idempotency works (two layers)**

1. **dlt state** remembers which files were loaded; reruns skip them.
2. **Delete-insert merge on `_source_file`**: reloading a file deletes its old rows before
   inserting, so a file's rows can never be doubled, even with `--reload` or lost state.

---

## Step 5: Verify

```bash
duckdb < ingestion/verify_load.sql
```

| Check | Expected result |
|---|---|
| 1. Rows in files vs rows in raw, per day | `diff = 0` for every loaded day |
| 2. Total raw rows | unchanged after `load_events.py --reload` |
| 3. Duplicate deliveries | ~0.5% (raw is not deduplicated yet) |
| 4. Days late | most at 0, ~1% at 1–3 days |
| 5. Lineage | one row per loaded file with its load time |

**Idempotency proof:** run check 2, run `uv run python ingestion/load_events.py --reload`,
run check 2 again. Same number = idempotent.

---

## Full reset

```bash
rm -rf data/exports data/warehouse.duckdb ~/.dlt/pipelines/clickstream
```

Keeps the downloaded CSVs in `data/landing/`. Then rerun from Step 3.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `401 Unauthorized` from Kaggle | Regenerate the key; confirm `~/.kaggle/kaggle.json` exists with `chmod 600` |
| `403` from Kaggle | Accept the dataset terms on its Kaggle page |
| `Could not set lock on file ... warehouse.duckdb` | Close other DuckDB sessions (CLI, VS Code, notebooks) |
| Out of memory in Step 3 | `--memory-limit 2GB`; close other apps |
| Loader says `skip` for every file | Already loaded; use `--reload` to force |
| `No export files matched` | Run Step 3 first, or check the `--date` value |

---

## Concepts demonstrated (for the blog post)

- **Bronze/raw layer:** store data as it arrived; clean later where it's tested.
- **Idempotency:** running a load twice gives the same result as running it once.
- **Incremental loading:** process only what's new; track what's done.
- **Late-arriving data and duplicates:** partition by receive time, handle in staging.
- **Lineage:** every row knows its source file and load time.

**Next layer:** dbt staging on `raw.events` (cast types, dedupe on `event_id`, split `category_code`).
