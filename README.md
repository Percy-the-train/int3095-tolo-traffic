# INT3095 Group Project — Traffic Data Collection Setup

## Project goal
Predict traffic congestion (speed drop) on **Tolo Highway (吐露港公路)** — Ma Liu Shui,
Science Park, Tai Po Kau, Kwong Fuk corridor — using machine learning.

## Data sources (all from DATA.GOV.HK / 資料一線通)

| Source | Dataset | Used as |
|---|---|---|
| TD Traffic Speed, Volume, Road Occupancy (raw) | `hk-td-sm_4-traffic-data-strategic-major-roads` | Features X₁: speed, volume, occupancy per lane per detector |
| TD Traffic Speeds of Road Network Segments (processed) | same dataset | Target Y: segment speed on Route 9 / Tolo Highway |
| HKO weather (planned) | `hk-hko-*` / data.weather.gov.hk | Features X₂: rain, wind, visibility |
| Holiday calendar (planned) | 1823 iCal/JSON | Features X₃: is_weekend, is_public_holiday |
| Road accident statistics (planned) | `hk-td-wcms_9~18` | X₄ / outlier filtering |

## Key finding (go/no-go check)
The TD feeds are **real-time snapshots only** — no history is published. Therefore
history must be **self-collected** by polling on a schedule. Both endpoints work
**without an API key**.

## What was built

### 1. Local collector — `collector/collect_traffic.py`
- Polls both TD XML feeds and appends filtered rows to CSV
- Filters: 46 detectors on Tolo Highway / Tai Po Road corridor; 561 Route 9 + Tai Po Road segments
- Stdlib only, retry/backoff, lock file, logging
- Tested: first poll captured 154 detector rows + 561 segment rows

Usage:
```
python collector/collect_traffic.py --once        # one poll
python collector/collect_traffic.py --loop 5      # every 5 minutes
```

Windows Task Scheduler (PC must be on):
```
schtasks /Create /TN "INT3095TrafficCollector" /TR "\"C:\Program Files\Python313\python.exe\" \"<project>\collector\collect_traffic.py\" --once" /SC MINUTE /MO 5 /F
schtasks /Change /TN "INT3095TrafficCollector" /ED 20/11/2026   # auto-stop
```

### 2. GitHub Actions pipeline — repo `Percy-the-train/int3095-tolo-traffic` (public)
- `collector/collect_traffic.py` — same script
- `.github/workflows/collect.yml` — cron `*/5 * * * *` + manual trigger
- Each run polls the feeds and uploads one **artifact** containing that snapshot's two CSVs
  (data is never committed to git, avoiding repo bloat)
- `merge_artifacts.py` — merges all artifacts into two deduplicated CSVs (SQLite, resumable,
  handles GitHub rate limits)

Merge usage (after collection period):
```
python merge_artifacts.py --repo Percy-the-train/int3095-tolo-traffic --token <PAT> \
    --out-detectors detector_readings_merged.csv --out-segments segment_readings_merged.csv
```

## Verification log

| Check | Result |
|---|---|
| data.gov.hk endpoints reachable, no API key needed | PASS (2026-10-27) |
| Tolo Highway detectors exist in feed (AID09122/123, TDS90001/002, TDSTPR…) | PASS |
| Route 9 segments exist in segment feed | PASS (423 segments) |
| Local collector run | PASS — 154 detector + 561 segment rows |
| GitHub Actions manual run (workflow_dispatch) | PASS — artifact `traffic-1` verified |
| GitHub Actions 5-min cron auto-fire | **UNCONFIRMED** — no scheduled run observed ~1 hr after setup; re-registered schedule with a no-op push |

## Open items
1. **Confirm cron fires** — check with:
   ```
   gh run list --repo Percy-the-train/int3095-tolo-traffic
   ```
   If it still doesn't fire, fall back to local Task Scheduler + PC-on collection.
2. Add weather / holiday / accident data (history already available for these — no scraping needed).
3. Analysis phase: merge artifacts, dedupe, feature engineering (is_weekend, rush hour, rain), then
   compare models (baseline vs Linear Regression vs Random Forest/XGBoost vs LSTM) under different parameters.
4. Deadline: submit dataset backup + code + ~2500-word report by **24 Nov 2026 11:59pm**.
