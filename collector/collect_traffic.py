"""
INT3095 Group Project - Traffic Data Collector
===============================================
Polls the Hong Kong Transport Department real-time traffic feeds published
via DATA.GOV.HK (資料一線通) and appends readings to CSV files for offline
machine learning use.

Data sources:
  1. Traffic Speed, Volume and Road Occupancy (raw detector data)
     https://resource.data.one.gov.hk/td/traffic-detectors/rawSpeedVol-all.xml
  2. Traffic Speeds of Road Network Segments (processed segment speeds)
     https://resource.data.one.gov.hk/td/traffic-detectors/irnAvgSpeed-all.xml
  3. HKO current weather report (temperature, humidity, district rainfall)
     https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread

These feeds contain ONLY the current snapshot, so this script must be run
on a schedule (e.g. every 5 minutes) to build up a history.

Filters (editable below):
  - Detectors: road name contains TOLO or TAI PO ROAD (Tolo Highway + the
    parallel Tai Po Road corridor: Ma Liu Shui, Science Park, Tai Po Kau,
    Yuen Chau Tsai / Kwong Fuk)
  - Segments: Route 9 (Tolo Highway) + road names TOLO HIGHWAY / TAI PO ROAD

Usage:
  python collect_traffic.py --once            # one poll, then exit
  python collect_traffic.py --loop 5          # keep polling every 5 minutes
"""

import argparse
import csv
import datetime
import logging
import os
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
DATA_DIR = os.environ.get("TRAFFIC_DATA_DIR") or os.path.join(PROJECT_DIR, "data", "traffic")
LOG_FILE = os.path.join(BASE_DIR, "collector.log")
LOCK_FILE = os.path.join(BASE_DIR, "collector.lock")

# ---------------------------------------------------------------- endpoints
DETECTOR_LOOKUP_URL = (
    "https://static.data.gov.hk/td/traffic-data-strategic-major-roads/"
    "info/traffic_speed_volume_occ_info.csv"
)
SEGMENT_LOOKUP_URL = (
    "https://static.data.gov.hk/td/traffic-data-strategic-major-roads/"
    "info/speed_segments_info.csv"
)
RAW_DETECTOR_URL = "https://resource.data.one.gov.hk/td/traffic-detectors/rawSpeedVol-all.xml"
SEGMENT_SPEED_URL = "https://resource.data.one.gov.hk/td/traffic-detectors/irnAvgSpeed-all.xml"
WEATHER_URL = "https://data.weather.gov.hk/weatherAPI/opendata/weather.php?dataType=rhrread&lang=en"

# ---------------------------------------------------------------- filters
DETECTOR_ROAD_FILTER = ("TOLO", "TAI PO ROAD")          # substring, uppercase
SEGMENT_ROUTE_FILTER = {"9"}                            # Route 9 = Tolo Highway
SEGMENT_NAME_FILTER = ("TOLO HIGHWAY", "TAI PO ROAD")   # substring, uppercase

# ---------------------------------------------------------------- outputs
DETECTOR_CSV = os.path.join(DATA_DIR, "detector_readings.csv")
SEGMENT_CSV = os.path.join(DATA_DIR, "segment_readings.csv")
DETECTOR_LOOKUP_CACHE = os.path.join(DATA_DIR, "detector_lookup.csv")
SEGMENT_LOOKUP_CACHE = os.path.join(DATA_DIR, "segment_lookup.csv")
WEATHER_CSV = os.path.join(PROJECT_DIR, "data", "weather", "hko_current_2026.csv")

HEADERS = {"User-Agent": "Mozilla/5.0 (INT3095 traffic data collector)"}

DETECTOR_FIELDS = [
    "poll_time_utc", "feed_date", "period_from", "period_to",
    "detector_id", "district", "road_en", "lookup_direction", "direction",
    "lane_id", "speed_kmh", "occupancy_pct", "volume", "sd", "valid",
]
SEGMENT_FIELDS = [
    "poll_time_utc", "feed_date", "feed_time", "irn_version",
    "segment_id", "speed_kmh", "valid",
]
WEATHER_FIELDS = ["poll_time_utc", "record_time", "metric", "place", "value"]


def fetch(url, retries=3):
    """Download a URL with retry/backoff; raise RuntimeError after giving up."""
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=60) as resp:
                return resp.read()
        except Exception as exc:  # network / HTTP errors
            logging.warning("fetch failed (%s): %s (attempt %d/%d)",
                            url, exc, attempt, retries)
            time.sleep(5 * attempt)
    raise RuntimeError("gave up fetching: " + url)


def load_lookup(url, cache_path):
    """Load a lookup CSV, using a local copy if the network fetch fails."""
    text = None
    try:
        text = fetch(url).decode("utf-8-sig", errors="replace")
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8", newline="") as f:
            f.write(text)
    except Exception:
        if os.path.exists(cache_path):
            with open(cache_path, encoding="utf-8") as f:
                text = f.read()
            logging.warning("using cached lookup: %s", cache_path)
        else:
            raise
    return list(csv.DictReader(text.splitlines()))


def build_detector_map():
    """detector_id -> (district, road_en, lookup_direction) for target roads."""
    rows = load_lookup(DETECTOR_LOOKUP_URL, DETECTOR_LOOKUP_CACHE)
    selected = {}
    for row in rows:
        road = (row.get("Road_EN") or "").upper()
        if any(key in road for key in DETECTOR_ROAD_FILTER):
            det_id = row.get("AID_ID_Number", "").strip()
            if det_id:
                selected[det_id] = (
                    row.get("District", ""),
                    row.get("Road_EN", ""),
                    row.get("Direction", ""),
                )
    logging.info("detector map: %d target detectors", len(selected))
    return selected


def build_segment_filter():
    """Set of segment ids for Route 9 / Tolo Highway / Tai Po Road."""
    rows = load_lookup(SEGMENT_LOOKUP_URL, SEGMENT_LOOKUP_CACHE)
    selected = set()
    for row in rows:
        route = (row.get("ucase(route)") or "").upper()
        if route in SEGMENT_ROUTE_FILTER or any(
            key in route for key in SEGMENT_NAME_FILTER
        ):
            sid = (row.get("irn_id") or "").strip()
            if sid:
                selected.add(sid)
    logging.info("segment filter: %d target segments", len(selected))
    return selected


def poll_detectors(detector_map, poll_time):
    """Parse the raw detector XML and keep rows for target detectors only."""
    root = ET.fromstring(fetch(RAW_DETECTOR_URL))
    feed_date = root.findtext("date") or ""
    rows = []
    for period in root.findall(".//period"):
        period_from = period.findtext("period_from") or ""
        period_to = period.findtext("period_to") or ""
        for det in period.findall(".//detector"):
            det_id = det.findtext("detector_id") or ""
            info = detector_map.get(det_id)
            if info is None:
                continue
            x_direction = det.findtext("direction") or ""
            for lane in det.findall(".//lane"):
                rows.append({
                    "poll_time_utc": poll_time,
                    "feed_date": feed_date,
                    "period_from": period_from,
                    "period_to": period_to,
                    "detector_id": det_id,
                    "district": info[0],
                    "road_en": info[1],
                    "lookup_direction": info[2],
                    "direction": x_direction,
                    "lane_id": lane.findtext("lane_id") or "",
                    "speed_kmh": lane.findtext("speed") or "",
                    "occupancy_pct": lane.findtext("occupancy") or "",
                    "volume": lane.findtext("volume") or "",
                    "sd": lane.findtext("s.d.") or "",
                    "valid": lane.findtext("valid") or "",
                })
    return rows


def poll_segments(segment_filter, poll_time):
    """Parse the segment speed XML and keep rows for target segments only."""
    root = ET.fromstring(fetch(SEGMENT_SPEED_URL))
    feed_date = root.findtext("date") or ""
    feed_time = root.findtext("time") or ""
    irn_version = root.findtext("irn_version") or ""
    rows = []
    for seg in root.findall(".//segment"):
        seg_id = seg.findtext("segment_id") or ""
        if seg_id not in segment_filter:
            continue
        rows.append({
            "poll_time_utc": poll_time,
            "feed_date": feed_date,
            "feed_time": feed_time,
            "irn_version": irn_version,
            "segment_id": seg_id,
            "speed_kmh": seg.findtext("speed") or "",
            "valid": seg.findtext("valid") or "",
        })
    return rows


def poll_weather(poll_time):
    """Parse the HKO current weather report into long-format rows."""
    import json
    body = fetch(WEATHER_URL).decode("utf-8", errors="replace")
    data = json.loads(body)
    rows = []
    for group_key, metric in (("temperature", "temp_c"), ("humidity", "humidity_pct")):
        group = data.get(group_key) or {}
        record_time = group.get("recordTime", "")
        for item in group.get("data", []):
            rows.append({
                "poll_time_utc": poll_time,
                "record_time": record_time,
                "metric": metric,
                "place": item.get("place", ""),
                "value": item.get("value", ""),
            })
    rainfall = data.get("rainfall") or {}
    record_time = rainfall.get("recordTime", "")
    for item in rainfall.get("data", []):
        rows.append({
            "poll_time_utc": poll_time,
            "record_time": record_time,
            "metric": "rain_mm",
            "place": item.get("place", ""),
            "value": item.get("max", ""),
        })
    return rows


def append_rows(path, fields, rows):
    """Append rows to a CSV, writing the header when the file is new."""
    os.makedirs(DATA_DIR, exist_ok=True)
    new_file = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if new_file:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def lock_is_stale():
    """A lock is stale if its owner process is dead or it is very old."""
    try:
        with open(LOCK_FILE, encoding="utf-8") as f:
            pid = int(f.read().strip() or 0)
    except (OSError, ValueError):
        pid = 0
    try:
        age = time.time() - os.path.getmtime(LOCK_FILE)
    except OSError:
        return True
    if pid:
        try:
            os.kill(pid, 0)  # existence check; raises if the process is gone
            return False     # owner alive -> lock is valid
        except (OSError, ProcessLookupError):
            return True      # owner dead -> stale
    return age > 15 * 60     # no pid readable -> fall back to age check


def acquire_lock():
    """Create the lock file; return False if another run is still active."""
    try:
        fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
        return True
    except FileExistsError:
        if lock_is_stale():
            logging.warning("removing stale lock file (previous run crashed)")
            try:
                os.remove(LOCK_FILE)
            except OSError:
                return False
            return acquire_lock()
        return False


def release_lock():
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)


def poll_once():
    poll_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    detector_map = build_detector_map()
    segment_filter = build_segment_filter()

    n_det = append_rows(DETECTOR_CSV, DETECTOR_FIELDS,
                        poll_detectors(detector_map, poll_time))
    n_seg = append_rows(SEGMENT_CSV, SEGMENT_FIELDS,
                        poll_segments(segment_filter, poll_time))
    try:
        n_wx = append_rows(WEATHER_CSV, WEATHER_FIELDS,
                           poll_weather(poll_time))
    except Exception as exc:
        logging.warning("weather poll failed: %s", exc)
        n_wx = 0
    logging.info("poll %s: %d detector rows, %d segment rows, %d weather rows appended",
                 poll_time, n_det, n_seg, n_wx)
    return n_det, n_seg


def main():
    parser = argparse.ArgumentParser(description="Collect Tolo Highway traffic data")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--once", action="store_true",
                       help="poll once and exit (use with Task Scheduler / cron)")
    group.add_argument("--loop", type=int, metavar="MINS",
                       help="poll continuously every MINS minutes")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

    if not acquire_lock():
        logging.warning("another collector instance is running; exiting")
        return
    try:
        if args.loop:
            interval = max(args.loop, 1)
            logging.info("starting continuous collection every %d minute(s)", interval)
            while True:
                try:
                    poll_once()
                except Exception as exc:
                    logging.error("poll failed: %s", exc)
                time.sleep(interval * 60)
        else:
            poll_once()
    finally:
        release_lock()


if __name__ == "__main__":
    main()
