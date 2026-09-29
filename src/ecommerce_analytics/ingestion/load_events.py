"""
Step 4 of ingestion: load the daily exports into the raw (bronze) layer with dlt.

Design choices (each one is a paragraph in the blog post):

  * Raw means raw. Every column lands as text, duplicates and late events included.
    Cleaning and typing happen later in dbt, where they are tested and versioned.
    If a bad cast lived here, one malformed row could block an entire day's load.

  * The unit of loading is one export file. Each row carries lineage columns:
      _source_file   which file it came from
      _export_date   which daily batch it arrived in
      _extracted_at  when this pipeline read it

  * Idempotency, twice over:
      1. dlt state remembers which files were loaded, so reruns skip them.
      2. The table uses a delete-insert merge keyed on _source_file. Reloading a
         file first deletes that file's old rows, then inserts the new ones.
         So even with --reload, or if the state is lost, a file's rows are never
         doubled. (Duplicate EVENTS inside the feed are kept on purpose; those
         are a data problem for staging, not a loading problem.)

Usage:
    uv run python ingestion/load_events.py                         # everything not yet loaded
    uv run python ingestion/load_events.py --through 2019-10-07    # replay: first week only
    uv run python ingestion/load_events.py --date 2019-10-08       # one day ("tonight's batch")
    uv run python ingestion/load_events.py --date 2019-10-08 --reload   # backfill / redo a day

Output:
    data/warehouse.duckdb, table raw.events
Close any DuckDB session that has the warehouse open before running (DuckDB allows
one writer at a time).
"""

import argparse
import re
import sys
from pathlib import Path

import dlt
import duckdb

EXPORTS = Path("../data/exports/events")
WAREHOUSE = "../data/warehouse.duckdb"
DATE_IN_PATH = re.compile(r"export_date=(\d{4}-\d{2}-\d{2})")
BATCH_ROWS = 500_000


def find_export_files(only_date: str | None, through: str | None) -> list[tuple[Path, str]]:
    """Return (file, export_date) pairs in date order, filtered by the CLI options."""
    selected = []
    for path in sorted(EXPORTS.glob("export_date=*/*.csv.gz")):
        match = DATE_IN_PATH.search(path.as_posix())
        if not match:
            continue
        export_date = match.group(1)
        if only_date and export_date != only_date:
            continue
        if through and export_date > through:
            continue
        selected.append((path, export_date))
    return selected


@dlt.resource(
    name="events",
    write_disposition={"disposition": "merge", "strategy": "delete-insert"},
    merge_key="_source_file",
)
def events(files: list[tuple[Path, str]], reload: bool = False):
    state = dlt.current.resource_state()
    loaded: list[str] = state.setdefault("loaded_files", [])

    reader = duckdb.connect()  # in-memory: used only to stream the gzipped CSVs
    for path, export_date in files:
        file_key = path.relative_to(EXPORTS).as_posix()
        if file_key in loaded and not reload:
            print(f"  skip   {file_key} (already loaded)")
            continue

        print(f"  load   {file_key}")
        safe_path = path.as_posix().replace("'", "''")
        result = reader.execute(
            f"""
            select
                *,
                date '{export_date}'  as _export_date,
                '{file_key}'          as _source_file,
                current_timestamp     as _extracted_at
            from read_csv('{safe_path}', header = true, all_varchar = true)
            """
        )
        # Stream Arrow batches instead of materializing the whole day in memory.
        for batch in result.fetch_record_batch(BATCH_ROWS):
            yield batch

        if file_key not in loaded:
            loaded.append(file_key)  # dlt commits state only if the load succeeds


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="load only this export date (YYYY-MM-DD)")
    parser.add_argument("--through", help="load export dates up to and including this one")
    parser.add_argument("--reload", action="store_true", help="reload files even if already loaded")
    args = parser.parse_args()

    files = find_export_files(args.date, args.through)
    if not files:
        sys.exit("No export files matched. Did you run ingestion/make_exports.py?")
    print(f"{len(files)} export file(s) selected.")

    pipeline = dlt.pipeline(
        pipeline_name="clickstream",
        destination=dlt.destinations.duckdb(WAREHOUSE),
        dataset_name="raw",
    )
    info = pipeline.run(events(files, reload=args.reload))
    print(info)

    with duckdb.connect(WAREHOUSE, read_only=True) as con:
        try:
            rows, days = con.execute(
                "select count(*), count(distinct _export_date) from raw.events"
            ).fetchone()
            print(f"raw.events now holds {rows:,} rows across {days} export day(s).")
        except duckdb.CatalogException:
            print("raw.events does not exist yet (nothing was loaded).")


if __name__ == "__main__":
    main()