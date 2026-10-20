"""
Downloads all CMS Provider Data datasets with the "Hospitals" theme,
converts the CSV column names to snake_case, and saves them locally.

Meant to run once a day. Each run checks the `modified` date in the CMS
metastore against what we loaded last time (stored in a small SQLite db),
so only new or changed datasets get downloaded.

    python cms_hospitals_etl.py                  # normal daily run
    python cms_hospitals_etl.py --full-refresh   # reload everything

No third-party packages needed, just Python 3.9+.
"""

import argparse
import csv
import io
import json
import logging
import os
import re
import sqlite3
import sys
import time
import unicodedata
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

METASTORE_URL = "https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items"
THEME = "Hospitals"
MAX_RETRIES = 3

log = logging.getLogger("cms_etl")


@dataclass
class Dataset:
    identifier: str
    title: str
    modified: str
    download_url: str


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def to_snake_case(name):
    # e.g. "Patients' rating of the facility linear mean score"
    #   -> "patients_rating_of_the_facility_linear_mean_score"
    # Some headers have curly quotes/accents, so flatten to plain ascii first.
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    # A few psych facility headers use camelCase ("MedCont %"), split those into med_cont
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = s.lower().replace("'", "")  # patients' -> patients, not patients_
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    if s and s[0].isdigit():
        s = "_" + s  # a lot of databases won't accept a column starting with a number
    return s or "unnamed"


def clean_headers(headers):
    # Two different headers could end up with the same snake_case name,
    # so add _2, _3 etc. to keep them unique.
    seen = {}
    result = []
    for h in headers:
        name = to_snake_case(h)
        seen[name] = seen.get(name, 0) + 1
        result.append(name if seen[name] == 1 else f"{name}_{seen[name]}")
    return result


# ---- run tracking ----
# runs:     one row per execution, so there's a history of what happened each day
# datasets: the `modified` date of each dataset as of its last successful load

class StateDB:
    def __init__(self, path):
        self.conn = sqlite3.connect(path)
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at     TEXT,
                finished_at    TEXT,
                status         TEXT,
                datasets_found INTEGER,
                downloaded     INTEGER,
                skipped        INTEGER,
                failed         INTEGER
            );
            CREATE TABLE IF NOT EXISTS datasets (
                identifier     TEXT PRIMARY KEY,
                title          TEXT,
                modified       TEXT,
                download_url   TEXT,
                file_path      TEXT,
                row_count      INTEGER,
                last_loaded_at TEXT,
                last_run_id    INTEGER
            );
        """)

    def start_run(self):
        cur = self.conn.execute(
            "INSERT INTO runs (started_at, status) VALUES (?, 'running')", (now(),))
        self.conn.commit()
        return cur.lastrowid

    def finish_run(self, run_id, status, found, downloaded, skipped, failed):
        self.conn.execute(
            "UPDATE runs SET finished_at=?, status=?, datasets_found=?, downloaded=?, "
            "skipped=?, failed=? WHERE run_id=?",
            (now(), status, found, downloaded, skipped, failed, run_id))
        self.conn.commit()

    def get_last_modified(self):
        return dict(self.conn.execute("SELECT identifier, modified FROM datasets"))

    def save_dataset(self, run_id, ds, file_path, row_count):
        self.conn.execute(
            "INSERT OR REPLACE INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (ds.identifier, ds.title, ds.modified, ds.download_url,
             str(file_path), row_count, now(), run_id))
        self.conn.commit()


# ---- download / processing ----

def http_get(url):
    # CMS occasionally times out or drops a connection, so retry a few times
    # with a growing wait (2s, 4s) before giving up.
    req = urllib.request.Request(url, headers={"User-Agent": "cms-hospitals-etl"})
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return urllib.request.urlopen(req, timeout=120)
        except Exception as e:
            if attempt == MAX_RETRIES:
                raise
            log.warning("Request failed (%s), retrying: %s", e, url)
            time.sleep(2 ** attempt)


def get_hospital_datasets():
    with http_get(METASTORE_URL) as resp:
        items = json.load(resp)

    datasets = []
    for item in items:
        if THEME not in item.get("theme", []):
            continue
        csv_urls = [d["downloadURL"] for d in item.get("distribution", [])
                    if d.get("mediaType") == "text/csv"]
        if not csv_urls:
            log.warning("No CSV found for %s, skipping", item["identifier"])
            continue
        datasets.append(Dataset(item["identifier"], item.get("title", ""),
                                item.get("modified", ""), csv_urls[0]))
    return datasets


def process_dataset(ds, output_dir):
    out_file = output_dir / f"{ds.identifier}_{Path(ds.download_url).stem.lower()}.csv"
    tmp_file = out_file.with_suffix(".tmp")

    # Stream it row by row instead of loading the whole file into memory.
    # Some of these files are 300k+ rows.
    with http_get(ds.download_url) as resp:
        # utf-8-sig drops the BOM some of the files start with, otherwise
        # the first column name comes through as "﻿facility_id"
        reader = csv.reader(io.TextIOWrapper(resp, encoding="utf-8-sig", newline=""))
        rows = 0
        with open(tmp_file, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(clean_headers(next(reader)))
            for row in reader:
                writer.writerow(row)
                rows += 1

    # Write to a temp file first and rename at the end, so if something dies
    # halfway through we don't leave a partial CSV sitting in the output folder.
    os.replace(tmp_file, out_file)
    return out_file, rows


def run(output_dir, db_path, workers, full_refresh):
    output_dir.mkdir(parents=True, exist_ok=True)
    db = StateDB(db_path)
    run_id = db.start_run()
    log.info("Run %d started", run_id)

    found = downloaded = skipped = failed = 0
    status = "failed"
    try:
        datasets = get_hospital_datasets()
        found = len(datasets)

        last_modified = {} if full_refresh else db.get_last_modified()
        to_download = [d for d in datasets if last_modified.get(d.identifier) != d.modified]
        skipped = found - len(to_download)
        log.info("%d Hospitals datasets, %d new or changed, %d unchanged",
                 found, len(to_download), skipped)

        # The work is almost all waiting on the network, so threads are enough here.
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(process_dataset, d, output_dir): d for d in to_download}
            for future in as_completed(futures):
                ds = futures[future]
                try:
                    out_file, rows = future.result()
                except Exception:
                    # Don't let one bad file stop the rest. Since it never gets saved
                    # to the datasets table, it'll be picked up again next run.
                    log.exception("Failed: %s (%s)", ds.identifier, ds.title)
                    failed += 1
                    continue
                # Only the main thread writes to sqlite (connections aren't thread safe)
                db.save_dataset(run_id, ds, out_file, rows)
                downloaded += 1
                log.info("Loaded %s: %d rows - %s", ds.identifier, rows, ds.title)

        if failed == 0:
            status = "success"
        elif downloaded > 0:
            status = "partial"
    finally:
        db.finish_run(run_id, status, found, downloaded, skipped, failed)
        log.info("Run %d finished (%s): %d downloaded, %d skipped, %d failed",
                 run_id, status, downloaded, skipped, failed)

    # non-zero exit code so the scheduler can flag the run if anything failed
    return 0 if failed == 0 else 1


def main():
    here = Path(__file__).parent
    parser = argparse.ArgumentParser(description="Download CMS Hospitals datasets")
    parser.add_argument("--output-dir", type=Path, default=here / "output")
    parser.add_argument("--state-db", type=Path, default=here / "cms_etl_state.db")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--full-refresh", action="store_true",
                        help="ignore previous runs and download everything")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(message)s")
    sys.exit(run(args.output_dir, args.state_db, args.workers, args.full_refresh))


if __name__ == "__main__":
    main()
