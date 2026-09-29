"""
Step 3 of ingestion: turn one static monthly CSV into a realistic daily event feed.

Real product analytics data (Segment, Snowplow, RudderStack exports) arrives as one
batch of files per day, and it is never perfectly clean. This script simulates that
upstream "vendor bucket" so the loader has a real feed to consume:

  * adds an event_id (like Segment's messageId): a hash of the row's contents
  * adds received_at: when the tracking server got the event
  * partitions files by the day they were RECEIVED, not the day they happened
  * injects known, reproducible chaos:
      - LATE_PCT of events arrive 1-3 days late (mobile app offline, retry queues)
      - DUP_PCT of events are delivered twice, one hour apart (client retries)

Because the chaos is derived from a hash of each event, rerunning produces identical
output. You know exactly what went in, so you can prove later that dbt cleans it up.

Usage:
    uv run python ingestion/make_exports.py --month 2019-Oct

Output:
    data/exports/events/export_date=2019-10-01/from_2019-Oct_0.csv.gz
    data/exports/events/export_date=2019-10-02/from_2019-Oct_0.csv.gz
    ...
Late October events spill into the Nov 1-3 folders; the month in the file name keeps
them from clashing with November's own files when you split November later.
"""

import argparse
import sys
from pathlib import Path

import duckdb

LANDING = Path("../data/landing")
EXPORTS = Path("../data/exports/events")

# Chaos settings, in tenths of a percent (buckets out of 1000).
LATE_BUCKETS = 10  # 1.0% of events arrive 1-3 days late
DUP_BUCKETS = 5    # 0.5% of events are delivered twice


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2019-Oct", help="2019-Oct or 2019-Nov")
    parser.add_argument("--memory-limit", default="4GB", help="DuckDB memory cap")
    args = parser.parse_args()

    source = LANDING / f"{args.month}.csv"
    if not source.exists():
        sys.exit(f"{source} not found. Run ingestion/download.py --month {args.month} first.")
    EXPORTS.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute(f"set memory_limit = '{args.memory_limit}'")
    # Row order inside a file doesn't matter to us; dropping it lets DuckDB stream
    # a multi-GB file without holding it all in memory.
    con.execute("set preserve_insertion_order = false")

    late_end = LATE_BUCKETS
    dup_end = LATE_BUCKETS + DUP_BUCKETS

    query = f"""
    copy (
        with base as (
            select
                md5(concat_ws('|',
                    event_time, event_type, product_id, category_id,
                    coalesce(category_code, ''), coalesce(brand, ''),
                    price, user_id, coalesce(user_session, '')
                )) as event_id,
                *,
                cast(replace(event_time, ' UTC', '') as timestamp) as event_ts
            from read_csv('{source.as_posix()}', header = true, all_varchar = true)
        ),

        tagged as (
            select *, cast(hash(event_id) % 1000 as integer) as bucket
            from base
        ),

        first_delivery as (
            select
                *,
                case
                    -- late: arrives 1-3 days after it happened
                    when bucket < {late_end}
                        then event_ts + to_days(1 + bucket % 3) + to_seconds(bucket % 60)
                    -- normal: arrives a few seconds after it happened
                    else event_ts + to_seconds(bucket % 5)
                end as received_at
            from tagged
        ),

        retries as (
            -- the same event (same event_id) delivered again an hour later
            select * replace (received_at + to_hours(1) as received_at)
            from first_delivery
            where bucket >= {late_end} and bucket < {dup_end}
        ),

        all_deliveries as (
            select * from first_delivery
            union all
            select * from retries
        )

        select
            event_id,
            event_time,
            strftime(received_at, '%Y-%m-%d %H:%M:%S UTC') as received_at,
            event_type,
            product_id,
            category_id,
            category_code,
            brand,
            price,
            user_id,
            user_session,
            cast(received_at as date) as export_date
        from all_deliveries
    )
    to '{EXPORTS.as_posix()}' (
        format csv,
        header true,
        compression gzip,
        file_extension 'csv.gz',
        partition_by (export_date),
        overwrite_or_ignore true,
        filename_pattern 'from_{args.month}_{{i}}'
    )
    """

    print(f"Splitting {source} into daily exports under {EXPORTS}/ ...")
    con.execute(query)

    folders = sorted(p.name for p in EXPORTS.glob("export_date=*"))
    print(f"Done. {len(folders)} daily folders now exist ({folders[0]} to {folders[-1]}).")
    print(
        f"Injected chaos: ~{LATE_BUCKETS / 10:.1f}% late events, "
        f"~{DUP_BUCKETS / 10:.1f}% duplicate deliveries."
    )


if __name__ == "__main__":
    main()
