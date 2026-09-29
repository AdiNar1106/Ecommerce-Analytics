"""
Step 1 of ingestion: download one month of raw clickstream from Kaggle.

Usage:
    uv run python ingestion/download.py --month 2019-Oct
    uv run python ingestion/download.py --month 2019-Nov

Output:
    data/landing/2019-Oct.csv   (~5 GB, ~42M rows)

The download is skipped if the CSV already exists, so this is safe to rerun.
"""

import argparse
import subprocess
import sys
import zipfile
from pathlib import Path

DATASET = "mkechinov/ecommerce-behavior-data-from-multi-category-store"
LANDING = Path("../data/landing")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", default="2019-Oct", help="2019-Oct or 2019-Nov")
    args = parser.parse_args()

    LANDING.mkdir(parents=True, exist_ok=True)
    csv_name = f"{args.month}.csv"
    csv_path = LANDING / csv_name

    if csv_path.exists():
        print(f"{csv_path} already exists, skipping download.")
        return

    print(f"Downloading {csv_name} from Kaggle (several GB, be patient)...")
    subprocess.run(
        ["kaggle", "datasets", "download", DATASET, "-f", csv_name, "-p", str(LANDING)],
        check=True,
    )

    # Kaggle usually serves large files zipped. Unzip and remove the archive.
    zip_path = LANDING / f"{csv_name}.zip"
    if zip_path.exists():
        print(f"Unzipping {zip_path}...")
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(LANDING)
        zip_path.unlink()

    if not csv_path.exists():
        sys.exit(f"Expected {csv_path} after download, but it is missing. Check {LANDING}.")

    size_gb = csv_path.stat().st_size / 1e9
    print(f"Done: {csv_path} ({size_gb:.1f} GB)")


if __name__ == "__main__":
    main()
