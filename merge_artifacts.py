"""
merge_artifacts.py
==================
Merges all GitHub Actions artifacts produced by the collect-traffic workflow
(each artifact holds one 5-minute snapshot of two CSV files) into two
deduplicated CSV files ready for analysis.

Why SQLite: ~8000 snapshots x ~700 rows = millions of rows; SQLite stores and
deduplicates them without loading everything into RAM. Duplicate rows happen
when a cron run polls the same feed period twice.

Deduplication keys:
  - detector rows: (feed_date, period_from, period_to, detector_id, lane_id)
  - segment rows: (feed_date, feed_time, segment_id)

Usage:
  python merge_artifacts.py --repo OWNER/REPO --token GITHUB_PAT
                           --out-detectors detector_readings.csv
                           --out-segments segment_readings.csv

Requires a GitHub personal access token with read access to Actions artifacts
(public repo). The script is resumable: processed artifact ids are recorded in
progress.txt next to the SQLite database.
"""

import argparse
import csv
import io
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
import zipfile

API = "https://api.github.com"
USER_AGENT = "int3095-artifact-merger"

DETECTOR_FIELDS = [
    "poll_time_utc", "feed_date", "period_from", "period_to",
    "detector_id", "district", "road_en", "lookup_direction", "direction",
    "lane_id", "speed_kmh", "occupancy_pct", "volume", "sd", "valid",
]
SEGMENT_FIELDS = [
    "poll_time_utc", "feed_date", "feed_time", "irn_version",
    "segment_id", "speed_kmh", "valid",
]

DETECTOR_KEY = ["feed_date", "period_from", "period_to", "detector_id", "lane_id"]
SEGMENT_KEY = ["feed_date", "feed_time", "segment_id"]


def api_request(url, token):
    """GET a GitHub API URL, handling rate limits (403/429) with Retry-After."""
    headers = {"Accept": "application/vnd.github+json", "User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = "Bearer " + token
    while True:
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.load(resp), dict(resp.headers)
        except urllib.error.HTTPError as exc:
            if exc.code in (403, 429):
                wait = int(exc.headers.get("Retry-After", 60))
                print("rate limited, sleeping %ds ..." % wait, flush=True)
                time.sleep(wait)
            else:
                raise
        except urllib.error.URLError:
            print("network error, retrying in 15s ...", flush=True)
            time.sleep(15)


def list_artifacts(repo, token, prefix="traffic-"):
    artifacts = []
    page = 1
    while True:
        data, _ = api_request(
            "%s/repos/%s/actions/artifacts?per_page=100&page=%d" % (API, repo, page),
            token,
        )
        batch = [a for a in data.get("artifacts", []) if a["name"].startswith(prefix)]
        artifacts.extend(batch)
        if not data.get("artifacts"):
            break
        page += 1
    print("found %d artifacts" % len(artifacts))
    return artifacts


def download_artifact(archive_url, token):
    headers = {"User-Agent": USER_AGENT}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = urllib.request.Request(archive_url, headers=headers)
    with urllib.request.urlopen(req, timeout=300) as resp:
        return resp.read()


def create_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS detectors (
            poll_time_utc TEXT, feed_date TEXT, period_from TEXT, period_to TEXT,
            detector_id TEXT, district TEXT, road_en TEXT, lookup_direction TEXT,
            direction TEXT, lane_id TEXT, speed_kmh TEXT, occupancy_pct TEXT,
            volume TEXT, sd TEXT, valid TEXT,
            UNIQUE (feed_date, period_from, period_to, detector_id, lane_id)
        )""")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS segments (
            poll_time_utc TEXT, feed_date TEXT, feed_time TEXT, irn_version TEXT,
            segment_id TEXT, speed_kmh TEXT, valid TEXT,
            UNIQUE (feed_date, feed_time, segment_id)
        )""")
    conn.commit()


def merge_zip(conn, blob):
    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        names = zf.namelist()
        det_name = next((n for n in names if n.endswith("detector_readings.csv")), None)
        seg_name = next((n for n in names if n.endswith("segment_readings.csv")), None)
        if det_name:
            text = zf.read(det_name).decode("utf-8", errors="replace")
            reader = csv.DictReader(text.splitlines())
            conn.executemany(
                "INSERT OR IGNORE INTO detectors VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [[row.get(f, "") for f in DETECTOR_FIELDS] for row in reader],
            )
        if seg_name:
            text = zf.read(seg_name).decode("utf-8", errors="replace")
            reader = csv.DictReader(text.splitlines())
            conn.executemany(
                "INSERT OR IGNORE INTO segments VALUES (?,?,?,?,?,?,?)",
                [[row.get(f, "") for f in SEGMENT_FIELDS] for row in reader],
            )
    conn.commit()


def export_csv(conn, table, fields, path):
    conn.execute("CREATE INDEX IF NOT EXISTS idx_%s_time ON %s (poll_time_utc)" % (table, table))
    rows = conn.execute("SELECT %s FROM %s ORDER BY poll_time_utc" % (", ".join(fields), table))
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(fields)
        writer.writerows(rows)
    print("exported %s -> %s" % (table, path))


def main():
    parser = argparse.ArgumentParser(description="Merge traffic snapshot artifacts")
    parser.add_argument("--repo", required=True, help="OWNER/REPO")
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""),
                        help="GitHub PAT (or set GITHUB_TOKEN env var)")
    parser.add_argument("--db", default="merge.db")
    parser.add_argument("--out-detectors", default="detector_readings_merged.csv")
    parser.add_argument("--out-segments", default="segment_readings_merged.csv")
    args = parser.parse_args()

    progress_path = args.db + ".progress"
    done = set()
    if os.path.exists(progress_path):
        with open(progress_path) as f:
            done = set(line.strip() for line in f)

    conn = sqlite3.connect(args.db)
    create_tables(conn)

    artifacts = list_artifacts(args.repo, args.token)
    pending = [a for a in artifacts if str(a["id"]) not in done]
    print("%d new artifacts to merge" % len(pending), flush=True)

    for i, art in enumerate(pending, 1):
        art_id = str(art["id"])
        blob = download_artifact(art["archive_download_url"], args.token)
        merge_zip(conn, blob)
        with open(progress_path, "a") as f:
            f.write(art_id + "\n")
        if i % 50 == 0:
            print("merged %d/%d" % (i, len(pending)), flush=True)

    export_csv(conn, "detectors", DETECTOR_FIELDS, args.out_detectors)
    export_csv(conn, "segments", SEGMENT_FIELDS, args.out_segments)
    conn.close()


if __name__ == "__main__":
    main()
