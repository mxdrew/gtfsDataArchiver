#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 25, 2026 (Last updated July 29, 2026)
#
# Description:
# Lightweight, single-process data ingestion engine for real-time and static transit feeds. 
# Captures live Server-Sent Events (SSE) with dynamic route batching, polls enhanced bulk 
# JSON endpoints, executes hourly contextual REST snapshots, and manages cumulative static GTFS 
# history for each agency.
#
# Data Integrity & Storage:
# Implements a crash-proof Write-Ahead Log (WAL) pattern. All network records are instantly 
# streamed to append-only, daily-tagged JSONL files under data/events/. An automated background 
# thread rotates files at the configured timezone's midnight boundary and safely compresses 
# the previous day's JSONL logs into highly efficient Parquet archives under data/archive/. 
# Active stream consumers are never blocked during rotation.
#
# If a Parquet archive already exists for a given day (due to container restarts), the worker 
# merges the JSONL data into the existing Parquet file and deduplicates records using a SHA-256 hash.
# Static GTFS snapshots are merged into stable per-table Parquet files under data/archive/gtfs/.
# Identical rows are stored once using SHA-256 deduplication; `first_logged` and `last_logged`
# metadata columns are added exclusively to static GTFS tables to track when each unique record
# first appeared and was most recently seen. Live event records do not carry these columns.
#
# Startup routines include an orphaned JSONL recovery sweep and automated scheduling for 
# twice-daily GTFS static package downloads (03:00 and 15:00 local time) routed to data/archive/gtfs/.
# All timestamps and rotation boundaries strictly adhere to the configured timezone.
#
# Output Schema:
#   hash_id — SHA-256 deterministic deduplication fingerprint
#   ts      — ISO 8601 timestamp in the configured timezone
#   event   — add | update | remove | reset | snapshot | error
#   id      — entity ID string from the API
#   data    — full API payload (JSON string in Parquet, raw dict in JSONL)
#
# License:
# BSD 3-Clause License
# Find the full agreement at https://github.com/mxdrew/gtfsDataArchiver/blob/main/LICENSE

import os
import io
import json
import re
import time
import queue
import zipfile
import threading
import hashlib
import tempfile
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv()

#### CONFIGURATION ####

def env_text(name, default=""):
    val = os.environ.get(name)
    if val is None:
        val = default
    return str(val).strip()


def env_int(name, default):
    raw_value = env_text(name, str(default))
    try:
        return int(raw_value)
    except ValueError:
        return default


def env_float(name, default):
    raw_value = env_text(name, str(default))
    try:
        return float(raw_value)
    except ValueError:
        return default


LOCAL_TZ = ZoneInfo(env_text("SYNC_TIMEZONE"))
API_KEY = env_text("API_KEY")
BASE_URL = env_text("BASE_URL")
GTFS_URL = env_text("GTFS_URL")
AGENCY_NAME = env_text("AGENCY_NAME", "agency") or "agency"


def env_flag(name, default="false"):
    val = os.environ.get(name)
    if val is None:
        val = default
    return str(val).strip().lower() == "true"


def env_list(name, default=""):
    raw_value = env_text(name, default)
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def safe_prefix(value):
    cleaned_value = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return cleaned_value.strip("_") or "agency"


AGENCY_PREFIX = safe_prefix(AGENCY_NAME)
AGENCY_LOG_LABEL = AGENCY_NAME.strip() or AGENCY_PREFIX

DATA_DIR = Path("data")
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
GTFS_DIR = ARCHIVE_DIR / "gtfs"

for d in [EVENTS_DIR, ARCHIVE_DIR, GTFS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

ENABLE_BASE_STREAMS = env_flag("ENABLE_BASE_STREAMS", "true")
ENABLE_ROUTE_STREAMS = env_flag("ENABLE_ROUTE_STREAMS", "true")
ENABLE_SNAPSHOT_PULLS = env_flag("ENABLE_SNAPSHOT_PULLS", "true")
ENABLE_ENHANCED_STREAMS = env_flag("ENABLE_ENHANCED_STREAMS", "true")
ENABLE_GTFS_STATIC = env_flag("ENABLE_GTFS_STATIC", "true")

GTFS_DOWNLOAD_HEADERS = {
    "Accept": "application/zip,application/octet-stream,*/*",
    # Keep user-agent configurable from .env without adding extra module globals.
    "User-Agent": env_text("GTFS_DOWNLOAD_USER_AGENT"),
}

# 1. Base Streams
BASE_STREAMS = env_list("BASE_STREAMS")

# 2. Batched Streams
ROUTE_STREAMS = env_list("ROUTE_STREAMS")

# 3. Snapshot Endpoints
SNAPSHOT_EPS = env_list("SNAPSHOT_EPS")

# 4. Enhanced Bulk Feeds
ENHANCED_BULK_STREAMS = {}
if ENABLE_ENHANCED_STREAMS:
    if env_text("VEHICLES_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["vehicles_enhanced"] = env_text("VEHICLES_ENHANCED_URL")
    if env_text("ALERTS_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["alerts_enhanced"] = env_text("ALERTS_ENHANCED_URL")
    if env_text("TRIPS_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["trips_enhanced"] = env_text("TRIPS_ENHANCED_URL")

# Tuning Parameters
DEFAULT_BATCH_SIZE = env_int("DEFAULT_BATCH_SIZE", 25)
PREDICTIONS_BATCH_SIZE = env_int("PREDICTIONS_BATCH_SIZE", max(DEFAULT_BATCH_SIZE, 75))
SNAPSHOT_RETRY_MINUTES = env_int("SNAPSHOT_RETRY_MINUTES", 10)

# Retry tracking: set to a future datetime when fetch_snapshots has failures; cleared by scheduler.
_snapshot_retry_lock = threading.Lock()
_snapshot_retry_at = None  # datetime | None
ENHANCED_POLL_INTERVAL_SECONDS = env_float("ENHANCED_POLL_INTERVAL_SECONDS", 7)
ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
LOG_LEVEL = env_text("LOG_LEVEL", "info").lower() or "info"

# Event-stream parquet retention. Applies only to dated event-stream archives
# (e.g. AGENCY_vehicles_07012026.parquet) directly under ARCHIVE_DIR. Static
# GTFS tables live under ARCHIVE_DIR/gtfs/ with stable, non-dated filenames and
# are never touched by this. Set to 0 (or a negative value) to disable cleanup.
EVENT_RETENTION_DAYS = env_int("EVENT_RETENTION_DAYS", 30)

write_queue = queue.Queue(maxsize=500000)
stop_event = threading.Event()
_http_session = None


def get_http_session():
    global _http_session

    if _http_session is None:
        session = requests.Session()
        # Keep retries tight and idempotent (GET only) to avoid duplicate side effects.
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _http_session = session

    return _http_session


def dated_output_filename(stem, extension):
    date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    return f"{AGENCY_PREFIX}_{stem}_{date_str}.{extension}"


def filename_date_key(filename):
    try:
        return filename.rsplit("_", 1)[-1].split(".", 1)[0]
    except Exception:
        return ""


def close_stale_event_handles(handles, current_date_key):
    stale_filenames = [name for name in handles if filename_date_key(name) != current_date_key]
    for filename in stale_filenames:
        try:
            handles[filename].close()
        finally:
            del handles[filename]


def gtfs_row_hash(table_name, row_data):
    normalized_row = {}
    for key, value in row_data.items():
        normalized_row[key] = None if pd.isna(value) else value

    payload = json.dumps(
        {"table": table_name, "row": normalized_row},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def prepare_gtfs_snapshot(df, table_name, logged_date):
    frame = df.copy()
    records = [dict(zip(frame.columns, row)) for row in frame.itertuples(index=False, name=None)]
    frame["hash_id"] = [gtfs_row_hash(table_name, record) for record in records]
    frame["first_logged"] = logged_date
    frame["last_logged"] = logged_date
    return frame


def normalize_gtfs_logged_dates(series):
    normalized_values = []
    for value in series:
        if pd.isna(value):
            normalized_values.append(pd.NaT)
            continue

        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            parsed = pd.to_datetime(value, format="%m%d%Y", errors="coerce")

        if pd.isna(parsed):
            normalized_values.append(pd.NaT)
            continue

        if getattr(parsed, "tzinfo", None) is None:
            parsed = parsed.tz_localize(LOCAL_TZ)
        else:
            parsed = parsed.tz_convert(LOCAL_TZ)

        normalized_values.append(parsed)

    return pd.Series(normalized_values, index=series.index)


def merge_gtfs_snapshots(existing_df, new_df, table_name, logged_date):
    frames = []

    for frame in (existing_df, new_df):
        if frame is not None and not frame.empty:
            frames.append(frame.copy())

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True, sort=False)

    if "hash_id" not in combined.columns:
        data_columns = [column for column in combined.columns if column not in {"first_logged", "last_logged"}]
        combined["hash_id"] = [gtfs_row_hash(table_name, record) for record in combined[data_columns].to_dict(orient="records")]

    if "first_logged" not in combined.columns:
        combined["first_logged"] = logged_date
    if "last_logged" not in combined.columns:
        combined["last_logged"] = combined["first_logged"]

    combined["first_logged"] = normalize_gtfs_logged_dates(combined["first_logged"])
    combined["last_logged"] = normalize_gtfs_logged_dates(combined["last_logged"])

    if combined["first_logged"].isna().all():
        combined["first_logged"] = pd.to_datetime(logged_date, format="%Y-%m-%d", errors="coerce")
    if combined["last_logged"].isna().all():
        combined["last_logged"] = pd.to_datetime(logged_date, format="%Y-%m-%d", errors="coerce")

    agg_map = {}
    for column in combined.columns:
        if column == "hash_id":
            continue
        if column == "first_logged":
            agg_map[column] = "min"
        elif column == "last_logged":
            agg_map[column] = "max"
        else:
            agg_map[column] = "first"

    merged = combined.groupby("hash_id", as_index=False).agg(agg_map)
    merged["first_logged"] = normalize_gtfs_logged_dates(merged["first_logged"]).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    merged["last_logged"] = normalize_gtfs_logged_dates(merged["last_logged"]).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return merged

#### LOGGING ####

def log(message, is_error=False, is_warning=False):
    if LOG_LEVEL == "errors" and not is_error and not is_warning:
        return
        
    ts = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    agency_prefix = f"[{AGENCY_LOG_LABEL}] " if AGENCY_LOG_LABEL else ""
    if is_error:
        print(f"[{ts}] 🔴 ERROR {agency_prefix}{message}".rstrip())
    elif is_warning:
        print(f"[{ts}] 🟡 WARNING {agency_prefix}{message}".rstrip())
    else:
        print(f"[{ts}] 🟢 {agency_prefix}{message}".rstrip())

#### WRITER & COMPACTION ####

def writer_worker():
    """Drains the queue, computes deduplication hashes, and appends to today's JSONL file."""
    log("Writer thread engaged. Streaming to JSONL.")
    handles = {}
    
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                current_date_key = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                # Close old file handles immediately after date rollover.
                close_stale_event_handles(handles, current_date_key)

                endpoint, evt_type, record_data = write_queue.get(timeout=1.0)
                
                # Deterministic hash excludes timestamp so deduplication works across days.
                payload_str = f"{endpoint}|{evt_type}|{record_data.get('id', '')}|{json.dumps(record_data, sort_keys=True)}"
                hash_id = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
                
                record = {
                    "hash_id": hash_id,
                    "ts": datetime.now(LOCAL_TZ).isoformat(),
                    "event": evt_type,
                    "id": str(record_data.get("id", "")),
                    "data": record_data
                }
                
                filename = dated_output_filename(endpoint, "jsonl")
                filepath = EVENTS_DIR / filename
                
                if filename not in handles:
                    # Open once per day/file and reuse handle for lower IO overhead.
                    handles[filename] = open(filepath, "a", encoding="utf-8")
                
                handles[filename].write(json.dumps(record) + "\n")
                handles[filename].flush()
                write_queue.task_done()
                
            except queue.Empty:
                pass
            except Exception as e:
                log(f"Writer exception: {e}", is_error=True)
    finally:
        for f in handles.values():
            f.close()

def parquet_compaction_worker():
    # Streams orphaned/yesterday's JSONL files into Parquet and removes the source after success.
    # On startup: removes stale .tmp files left by compaction runs that were interrupted by a
    # container crash. Any .tmp older than 2 hours is safe to delete.
    log("Parquet compaction watchdog and recovery sweep active.")
    _STALE_TMP_AGE = 7200  # seconds (2 hours)
    now_ts = time.time()
    swept = []
    for tmp_file in ARCHIVE_DIR.glob("*.tmp"):
        try:
            if now_ts - tmp_file.stat().st_mtime > _STALE_TMP_AGE:
                tmp_file.unlink()
                swept.append(tmp_file.name)
        except Exception:
            pass
    if swept:
        log(f"Startup sweep: removed {len(swept)} stale .tmp file(s): {', '.join(swept)}")

    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            
            for file in EVENTS_DIR.glob("*.jsonl"):
                if today_str not in file.name:
                    log(f"Compacting orphaned/rotated {file.name} to Parquet archive...")
                    temp_path = None
                    writer = None
                    try:
                        parquet_filename = file.with_suffix(".parquet").name
                        parquet_path = ARCHIVE_DIR / parquet_filename

                        temp_file = tempfile.NamedTemporaryFile(
                            delete=False,
                            dir=parquet_path.parent,
                            prefix=f"{parquet_path.stem}.",
                            suffix=".tmp",
                        )
                        temp_file.close()
                        temp_path = Path(temp_file.name)

                        row_count = 0

                        for chunk in pd.read_json(file, lines=True, chunksize=100000):
                            if "data" in chunk.columns:
                                chunk["data"] = chunk["data"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))

                            table = pa.Table.from_pandas(chunk, preserve_index=False)

                            if writer is None:
                                writer = pq.ParquetWriter(
                                    str(temp_path),
                                    table.schema,
                                    compression="zstd",
                                    compression_level=ARCHIVE_ZSTD_LEVEL,
                                )

                            writer.write_table(table)
                            row_count += len(chunk)

                        if writer is None:
                            log(f"Skipping empty JSONL file: {file.name}", is_warning=True)
                            continue

                        writer.close()
                        writer = None
                        # Atomic replacement avoids partial archives on interruption.
                        temp_path.replace(parquet_path)
                        file.unlink()
                        log(f"Success: {file.name} compacted into {parquet_path.name} with {row_count} rows.")
                    except Exception as e:
                        log(f"Failed to compact {file.name}: {e}", is_error=True)
                    finally:
                        if writer is not None:
                            writer.close()
                        if temp_path is not None and temp_path.exists():
                            try:
                                temp_path.unlink()
                            except Exception:
                                pass
            cleanup_old_event_parquets()
        except Exception as e:
            log(f"Compaction thread error: {e}", is_error=True)
            
        for _ in range(60):
            if stop_event.is_set(): break
            time.sleep(1)

# Matches the dated event-stream parquet naming convention, e.g.
# "MBTA_vehicles_07012026.parquet" -> month=07, day=01, year=2026.
_EVENT_PARQUET_DATE_RE = re.compile(r"_(\d{2})(\d{2})(\d{4})\.parquet$")


def cleanup_old_event_parquets():
    """Deletes compacted event-stream parquet archives older than EVENT_RETENTION_DAYS.

    Only ever considers dated files sitting directly in ARCHIVE_DIR (event streams
    like vehicles, alerts, predictions, stop_events, and the hourly snapshot
    endpoints). Static GTFS tables live in ARCHIVE_DIR/gtfs/ under stable,
    non-dated filenames (AGENCY_gtfs_<table>.parquet); the non-recursive glob
    below never descends into that subdirectory, and the "_gtfs_" name check
    below is a second, defensive guard against ever deleting a static table.
    """
    if EVENT_RETENTION_DAYS <= 0:
        return

    cutoff_date = (datetime.now(LOCAL_TZ) - timedelta(days=EVENT_RETENTION_DAYS)).date()
    removed = []

    for file in ARCHIVE_DIR.glob("*.parquet"):
        if "_gtfs_" in file.name:
            continue  # static GTFS tables never expire

        match = _EVENT_PARQUET_DATE_RE.search(file.name)
        if not match:
            continue  # unrecognized filename shape; leave it alone rather than guess

        month_str, day_str, year_str = match.groups()
        try:
            file_date = datetime(int(year_str), int(month_str), int(day_str)).date()
        except ValueError:
            continue

        if file_date < cutoff_date:
            try:
                file.unlink()
                removed.append(file.name)
            except Exception as e:
                log(f"Failed to remove expired parquet {file.name}: {e}", is_warning=True)

    if removed:
        log(f"Retention sweep: removed {len(removed)} event parquet(s) older than {EVENT_RETENTION_DAYS} days.")

#### NETWORK CONSUMERS ####

def fetch_api_ids(endpoint):
    """Helper to fetch active route IDs for batching."""
    if not BASE_URL: return []
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY
    try:
        res = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, timeout=15)
        if res.status_code == 200:
            return [x.get("id") for x in res.json().get("data", []) if x.get("id")]
    except Exception as e:
        log(f"Failed to fetch {endpoint} IDs: {e}", is_warning=True)
    return []


def start_batched_streams(endpoint, routes):
    batch_size = PREDICTIONS_BATCH_SIZE if endpoint == "predictions" else DEFAULT_BATCH_SIZE
    for i in range(0, len(routes), batch_size):
        batch = routes[i:i + batch_size]
        threading.Thread(
            target=sse_consumer,
            args=(endpoint, {"filter[route]": ",".join(batch)}),
            daemon=True,
        ).start()
        time.sleep(0.25)

def sse_consumer(endpoint, params=None):
    """Maintains a persistent live connection to transit agency SSE endpoints."""
    if not BASE_URL: return
    url = f"{BASE_URL}/{endpoint}"
    headers = {
        "Accept": "text/event-stream",
        "Accept-Encoding": "identity",
        "Cache-Control": "no-cache",
    }
    if API_KEY: headers["X-API-Key"] = API_KEY
    req_params = params or {}
    session = get_http_session()
    reconnect_delay = 5

    while not stop_event.is_set():
        log(f"Connecting to live stream: /{endpoint} {req_params}")
        response = None
        try:
            response = session.get(url, headers=headers, params=req_params, stream=True, timeout=(10, 120))
            current_event = "message"
            data_buffer = []
            received_any_data = False

            for line in response.iter_lines(decode_unicode=True):
                if stop_event.is_set(): break
                if not line:
                    # Blank line marks one complete SSE event payload.
                    if data_buffer:
                        payload = json.loads("\n".join(data_buffer))
                        records = payload if isinstance(payload, list) else [payload]
                        for r in records:
                            write_queue.put((endpoint, current_event, r))
                        received_any_data = True
                    current_event = "message"
                    data_buffer = []
                    continue

                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data_buffer.append(line.split(":", 1)[1].strip())

            if not stop_event.is_set():
                reconnect_delay = 5 if received_any_data else min(60, reconnect_delay * 2)
                time.sleep(reconnect_delay)
                    
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            log(f"Stream /{endpoint} disconnected: {e}. Reconnecting in {reconnect_delay}s...", is_warning=True)
            time.sleep(reconnect_delay)
            reconnect_delay = min(60, reconnect_delay * 2)
        except Exception as e:
            log(f"Stream /{endpoint} disconnected: {e}. Reconnecting in {reconnect_delay}s...", is_warning=True)
            time.sleep(reconnect_delay)
            reconnect_delay = min(60, reconnect_delay * 2)
        finally:
            if response is not None:
                response.close()

def enhanced_poller(endpoint, url):
    """Polls the heavy bulk JSON files based on configured interval."""
    if not url: return
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY
    
    while not stop_event.is_set():
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                data = res.json()
                records = data.get("entity", []) if "entity" in data else data
                records = records if isinstance(records, list) else [records]
                for r in records:
                    write_queue.put((endpoint, "update", r))
        except Exception as e:
            log(f"Enhanced poll failed for {endpoint}: {e}", is_warning=True)
            
        sleep_ticks = int(ENHANCED_POLL_INTERVAL_SECONDS * 10)
        for _ in range(max(10, sleep_ticks)):
            if stop_event.is_set(): break
            time.sleep(0.1)

#### SCHEDULER TASKS ####

def fetch_snapshots():
    # Pulls contextual snapshots for all configured endpoints.
    # The MBTA V3 API rejects unfiltered requests for several endpoints with HTTP 400.
    # For those endpoints we fetch all route IDs first and pull in batches using
    # filter[route] — confirmed via curl: schedules, shapes, services, and trips all
    # require filter[route] and return HTTP 400 without it.
    #
    # Retry behavior: each GET is retried up to 3 times on 5xx / connection errors,
    # with 30s then 60s backoff. If any endpoint still fails after all retries, a full
    # retry is scheduled in SNAPSHOT_RETRY_MINUTES (default 10) via the scheduler.
    global _snapshot_retry_at
    if not BASE_URL or not ENABLE_SNAPSHOT_PULLS:
        return
    log("Pulling hourly snapshots...")
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY

    # Endpoints confirmed to require filter[route] — fetched in per-route batches.
    REQUIRE_ROUTE_FILTER = {"trips", "schedules", "shapes", "services"}

    # Endpoints requiring a non-route filter, mapped to (source_resource, filter_param).
    # live_facilities requires filter[id] using facility IDs fetched from /facilities.
    REQUIRE_ID_FILTER = {
        "live_facilities": ("facilities", "filter[id]"),
    }

    # Retry on transient 5xx/connection errors: 3 attempts total with increasing backoff.
    _RETRY_DELAYS = [30, 60]  # seconds before 2nd and 3rd attempt

    def _get_with_retry(url, params=None):
        max_attempts = len(_RETRY_DELAYS) + 1
        for attempt in range(max_attempts):
            if stop_event.is_set():
                return None
            err_msg = None
            try:
                res = requests.get(url, headers=headers, params=params, timeout=30)
            except Exception as e:
                err_msg = str(e)
            else:
                if res.status_code == 200:
                    return res
                if res.status_code < 500:
                    # 4xx errors are configuration problems — don't retry.
                    log(f"Snapshot returned HTTP {res.status_code}: {url}", is_warning=True)
                    return None
                err_msg = f"HTTP {res.status_code}"

            is_last = attempt >= len(_RETRY_DELAYS)
            if is_last:
                log(f"Snapshot failed ({err_msg}), all {max_attempts} attempts exhausted: {url}", is_warning=True)
                return None
            delay = _RETRY_DELAYS[attempt]
            log(
                f"Snapshot failed ({err_msg}), retrying in {delay}s "
                f"(attempt {attempt + 1}/{max_attempts}): {url}",
                is_warning=True,
            )
            # Interruptible sleep so the container shuts down cleanly.
            for _ in range(delay):
                if stop_event.is_set():
                    return None
                time.sleep(1)
        return None

    any_failed = False
    endpoint_counts = {}  # endpoint -> record count written (None = failed)

    for endpoint in SNAPSHOT_EPS:
        if stop_event.is_set():
            break
        count = 0
        ep_failed = False
        try:
            if endpoint in REQUIRE_ROUTE_FILTER:
                # Fetch route IDs then pull per-route batch to satisfy the API filter requirement.
                route_ids = fetch_api_ids("routes")
                if not route_ids:
                    log(f"Skipping {endpoint} snapshot: could not retrieve route IDs", is_warning=True)
                    any_failed = True
                    endpoint_counts[endpoint] = None
                    continue
                for i in range(0, len(route_ids), DEFAULT_BATCH_SIZE):
                    if stop_event.is_set():
                        break
                    batch = route_ids[i : i + DEFAULT_BATCH_SIZE]
                    res = _get_with_retry(
                        f"{BASE_URL}/{endpoint}",
                        params={"filter[route]": ",".join(batch)},
                    )
                    if res is not None:
                        for r in res.json().get("data", []):
                            write_queue.put((endpoint, "snapshot", r))
                            count += 1
                    else:
                        ep_failed = True
            elif endpoint in REQUIRE_ID_FILTER:
                # Fetch resource IDs first, then batch using the endpoint-specific filter param.
                source_resource, filter_param = REQUIRE_ID_FILTER[endpoint]
                ids = fetch_api_ids(source_resource)
                if not ids:
                    log(f"Skipping {endpoint} snapshot: could not retrieve {source_resource} IDs", is_warning=True)
                    any_failed = True
                    endpoint_counts[endpoint] = None
                    continue
                for i in range(0, len(ids), DEFAULT_BATCH_SIZE):
                    if stop_event.is_set():
                        break
                    batch = ids[i : i + DEFAULT_BATCH_SIZE]
                    res = _get_with_retry(
                        f"{BASE_URL}/{endpoint}",
                        params={filter_param: ",".join(batch)},
                    )
                    if res is not None:
                        for r in res.json().get("data", []):
                            write_queue.put((endpoint, "snapshot", r))
                            count += 1
                    else:
                        ep_failed = True
            else:
                res = _get_with_retry(f"{BASE_URL}/{endpoint}")
                if res is not None:
                    for r in res.json().get("data", []):
                        write_queue.put((endpoint, "snapshot", r))
                        count += 1
                else:
                    ep_failed = True
        except Exception:
            ep_failed = True

        if ep_failed:
            any_failed = True
            endpoint_counts[endpoint] = None
        else:
            endpoint_counts[endpoint] = count

    # Summary log so it's clear what succeeded and what didn't.
    ok = [f"{ep}={ct}" for ep, ct in endpoint_counts.items() if ct is not None]
    fail = [ep for ep, ct in endpoint_counts.items() if ct is None]
    if ok:
        log(f"Snapshots OK: {', '.join(ok)}")
    if fail:
        log(f"Snapshots FAILED: {', '.join(fail)}", is_warning=True)

    if any_failed and not stop_event.is_set():
        with _snapshot_retry_lock:
            _snapshot_retry_at = datetime.now(LOCAL_TZ) + timedelta(minutes=SNAPSHOT_RETRY_MINUTES)
        log(f"Some snapshots failed — scheduled retry in {SNAPSHOT_RETRY_MINUTES} min", is_warning=True)

def fetch_gtfs_static(skip_if_cached=False):
    # Downloads GTFS zip, deduplicates rows, and updates first/last seen dates.
    # skip_if_cached=True (startup): skips if parquet files already exist for this agency —
    # they'll be refreshed at the next scheduled 3AM/3PM run instead.
    if not GTFS_URL or not ENABLE_GTFS_STATIC:
        return
    if skip_if_cached and list(GTFS_DIR.glob(f"{AGENCY_PREFIX}_gtfs_*.parquet")):
        return
    log("Pulling static GTFS assets...")
    try:
        res = requests.get(GTFS_URL, headers=GTFS_DOWNLOAD_HEADERS, timeout=120)
        res.raise_for_status()
        
        logged_at = datetime.now(LOCAL_TZ).isoformat(timespec="seconds")
        z = zipfile.ZipFile(io.BytesIO(res.content))
        
        for file_name in z.namelist():
            if file_name.endswith(".txt"):
                table_name = file_name.replace(".txt", "")
                
                df = pd.read_csv(z.open(file_name), dtype=str, low_memory=False)
                out_path = GTFS_DIR / f"{AGENCY_PREFIX}_gtfs_{table_name}.parquet"

                current_df = prepare_gtfs_snapshot(df, table_name, logged_at)
                existing_df = pd.read_parquet(out_path) if out_path.exists() else None
                merged_df = merge_gtfs_snapshots(existing_df, current_df, table_name, logged_at)

                if merged_df is None:
                    log(f"Skipping empty GTFS table: {table_name}", is_warning=True)
                    continue

                temp_path = out_path.with_name(f"{out_path.name}.tmp")
                if temp_path.exists():
                    temp_path.unlink()

                table = pa.Table.from_pandas(merged_df, preserve_index=False)
                pq.write_table(table, temp_path, compression="zstd", compression_level=ARCHIVE_ZSTD_LEVEL)
                temp_path.replace(out_path)
                
        log("Static GTFS update complete.")
    except Exception as e:
        log(f"GTFS pull failed: {e}", is_error=True)

def scheduler_thread():
    # Triggers hourly and daily automated tasks.
    # Also drains _snapshot_retry_at: if fetch_snapshots had failures it schedules a retry
    # after SNAPSHOT_RETRY_MINUTES rather than waiting until the next top-of-hour run.
    global _snapshot_retry_at
    last_snapshot_hour = -1
    last_gtfs_pull = -1

    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)

        if ENABLE_SNAPSHOT_PULLS and now.hour != last_snapshot_hour and now.minute == 0:
            threading.Thread(target=fetch_snapshots, daemon=True).start()
            last_snapshot_hour = now.hour

        # Retry snapshots that failed in a previous run (sooner than next top-of-hour).
        with _snapshot_retry_lock:
            due = _snapshot_retry_at is not None and now >= _snapshot_retry_at
            if due:
                _snapshot_retry_at = None
        if due:
            log("Retrying failed snapshots")
            threading.Thread(target=fetch_snapshots, daemon=True).start()

        if ENABLE_GTFS_STATIC and now.hour in [3, 15] and now.hour != last_gtfs_pull:
            threading.Thread(target=fetch_gtfs_static, daemon=True).start()
            last_gtfs_pull = now.hour

        time.sleep(10)

#### MAIN ORCHESTRATOR ####

if __name__ == "__main__":
    try:
        log("Starting transit ingestion engine...")
        
        threading.Thread(target=writer_worker, daemon=True).start()
        threading.Thread(target=parquet_compaction_worker, daemon=True).start()
        if ENABLE_SNAPSHOT_PULLS or ENABLE_GTFS_STATIC:
            threading.Thread(target=scheduler_thread, daemon=True).start()
        
        # Initial baseline pulls
        if BASE_URL and ENABLE_SNAPSHOT_PULLS:
            threading.Thread(target=fetch_snapshots, daemon=True).start()
        if GTFS_URL and ENABLE_GTFS_STATIC:
            threading.Thread(target=fetch_gtfs_static, kwargs={"skip_if_cached": True}, daemon=True).start()
        
        # Deploy Base Streams
        if BASE_URL and ENABLE_BASE_STREAMS:
            for endpoint in BASE_STREAMS:
                threading.Thread(target=sse_consumer, args=(endpoint,), daemon=True).start()
                
            # Deploy Route-Batched Streams
            if ENABLE_ROUTE_STREAMS:
                routes = fetch_api_ids("routes")
                for endpoint in ROUTE_STREAMS:
                    start_batched_streams(endpoint, routes)
                
        # Deploy Enhanced Pollers
        for endpoint, url in ENHANCED_BULK_STREAMS.items():
            threading.Thread(target=enhanced_poller, args=(endpoint, url), daemon=True).start()
            
        while not stop_event.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        log("Shutdown signal received. Closing pipelines cleanly...")
        stop_event.set()
        time.sleep(2)