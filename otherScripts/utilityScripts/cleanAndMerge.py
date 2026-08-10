#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# June 23, 2026 (Last updated August 07, 2026)
#
# Description:
# Combined transit data lake maintenance utility.
# 1. Standardizes legacy raw stream names inside staging and archive directories
#    while keeping standard base streams and enhanced streams strictly separate.
# 2. Converts raw JSONL Write-Ahead Logs (WAL) into highly compressed Parquet files.
# 3. Reconciles and merges daily shards into clean unified historical datasets.
#
# Safety Features:
# - Pre-processing standardization step runs first on all scan roots in place.
# - Strict overwrite guard: Will NEVER overwrite or duplicate existing files.
#
# License:
# BSD 3-Clause License
# Find the full agreement at https://github.com/mxdrew/gtfsDataArchiver/blob/main/LICENSE

import os
import re
import sys
import hashlib
import json
import gc
import mmap
import tempfile
import sqlite3
import shutil
from pathlib import Path
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
from tqdm import tqdm
from dotenv import load_dotenv

# Load environment variables: prefer a .env alongside this script, otherwise walk
# up parent directories (otherScripts/, then the repo root) and use the first found.
for _env_dir in Path(__file__).resolve().parents:
    _env_candidate = _env_dir / ".env"
    if _env_candidate.exists():
        load_dotenv(_env_candidate)
        break

#### SAFETY & PIPELINE CONFIGURATION ####

# Explicit stream mappings (standardized to lower_snake_case)
# This mapping guarantees base streams and enhanced streams remain strictly isolated!
STREAM_MAPPING = {
    # Standard Base Vehicles
    "vehicle_positions": "vehicles",
    "vehiclepositions": "vehicles",
    "vehicles": "vehicles",

    # Enhanced Vehicles (polled JSON layers)
    "enhancedvehicles": "vehicles_enhanced",
    "vehicles_enhanced": "vehicles_enhanced",
    "enhanced_vehicles": "vehicles_enhanced",

    # Standard Base Alerts
    "service_alerts": "alerts",
    "alerts": "alerts",
    "servicealerts": "alerts",

    # Enhanced Alerts
    "alerts_enhanced": "alerts_enhanced",
    "enhanced_alerts": "alerts_enhanced",

    # Standard Base Trips (static schedule data)
    "trips": "trips",

    # Live Predictions / Trip Updates (real-time arrival estimates → always predictions)
    "tripupdates": "predictions",
    "tripupdate": "predictions",
    "trip_updates": "predictions",

    # Enhanced Trips
    "trips_enhanced": "trips_enhanced",
    "enhanced_trips": "trips_enhanced",

    # Standard Routes
    "active_routes": "routes",
    "routes": "routes",

    # Enhanced Routes
    "routes_enhanced": "routes_enhanced",

    # Predictions / Bus Details
    "predictionsbystop": "predictions_by_stop",
    "predictionsbystops": "predictions_by_stop",
    "predictions_by_stop": "predictions_by_stop",
    "predictionsbybus": "predictions_by_bus",
    "predictions_by_bus": "predictions_by_bus",
    "busesbyroute": "buses_by_route",
    "buses_by_route": "buses_by_route",

    # Configurations
    "config": "config",

    # GTFS-RT protobuf ingestors (gtfsrt.py — PVTA, MVRTA)
    # safe_prefix() lowercases and strips non-alphanumeric, so CamelCase collapses.
    "vehicleposition": "vehicles",
    "alert": "alerts",

    # RouteMatch ingestor output names (after GTFS_MAP translation)
    "vehicle_positions": "vehicles",
    "vehicle_positions_enhanced": "vehicles_enhanced",

    # PassioGo ingestor (passigo.py — BAT, FRTA, MART)
    "vehicle_stream": "vehicles",

    # SWIV CAD AVL ingestor (swiv.py — GATRA, WRTA, LRTA)
    # "vehicles" and "alerts" already handled above.
    # TopoBase = full Avail network topology snapshot (hourly).
    # TopoRefresh = incremental topology deltas (poll loop).
    "topobase": "topo_base",
    "topo_base": "topo_base",
    "toporefresh": "topo_refresh",
    "topo_refresh": "topo_refresh",

    # BusTime ingestor (bustime.py — SRTA)
    # BusTime uses GTFS_MAP before safe_prefix; "vehicles" already handled above.
    "predictions": "predictions",
    "stops": "stops",
}

DEFERRED_RENAMES = []

# Known 511 SF Bay operator ids (from the gtfsoperators roster). Any of these
# appearing as an agency token (or as a "511_<id>" doubled prefix from older runs)
# is normalized to the bare uppercase id. Capitol Corridor is operator "AM"
# (Amtrak-operated), so it folds into AMTRAK instead; the per-record "source"
# field (511 vs amtrak) preserves provenance. NOTE: "CC" is County Connection,
# NOT Capitol Corridor.
FIVEONEONE_OPERATOR_IDS = {
    "3D", "AC", "AF", "AM", "BA", "CC", "CE", "CM", "CR", "CT", "DE", "EE", "EM",
    "FS", "GF", "GG", "GP", "MA", "MB", "MC", "MV", "PE", "PG", "RG", "RV", "SA",
    "SB", "SC", "SF", "SI", "SL", "SM", "SO", "SR", "SS", "ST", "TF", "UC", "VC",
    "VN", "WC", "WH",
}

# Explicit agency-token remaps applied in STEP 0. Capitol Corridor (511 operator
# "AM") is stored under the AMTRAK archive regardless of source.
AGENCY_MAPPING = {
    "AM": "AMTRAK",           # 511 operator AM = Capitol Corridor (Amtrak-operated)
    "CAPITOLCORRIDOR": "AMTRAK",
    "CAPITAL_CORRIDOR": "AMTRAK",
}


def resolve_agency_token(agency, stream_raw):
    """STEP 0 helper. Given the greedy-parsed (agency, stream_raw) from a filename,
    return a corrected (agency, stream_raw) pair that:
      * unwraps a legacy "511_<id>_<stream>" doubled prefix into "<ID>_<stream>",
      * maps Capitol Corridor (511 operator AM) to AMTRAK,
      * uppercases bare 511 operator ids,
    while leaving all existing single-token agencies (MBTA, SRTA, ...) untouched.
    """
    # Case 0: legacy multi-word "Capitol_Corridor_<stream>" (an earlier naming
    # experiment) parses greedily as agency="Capitol", stream="Corridor_<stream>".
    # Fold it into AMTRAK and drop the trailing "Corridor_" from the stream.
    if agency.upper() == "CAPITOL" and stream_raw.upper().startswith("CORRIDOR_"):
        agency = "AMTRAK"
        stream_raw = stream_raw.split("_", 1)[1]

    # Case 1: legacy doubled prefix "511_<id>_<rest>" -> agency parsed as "511",
    # stream_raw begins with the operator id (e.g. "CC_vehiclepositions").
    if agency == "511" and "_" in stream_raw:
        maybe_id, rest = stream_raw.split("_", 1)
        if maybe_id.upper() in FIVEONEONE_OPERATOR_IDS:
            agency = maybe_id.upper()
            stream_raw = rest

    # Case 2: bare 511 operator id as the agency token (already unwrapped, or from
    # the current 511org.py naming). Uppercase it for consistency.
    if agency.upper() in FIVEONEONE_OPERATOR_IDS:
        agency = agency.upper()

    # Case 3: explicit remaps (AM -> AMTRAK, etc.).
    agency = AGENCY_MAPPING.get(agency.upper(), agency)

    return agency, stream_raw


def _resolve_local_tz():
    """Resolve timezone from env with safe fallbacks for Windows/WSL installs."""
    tz_name = os.environ.get("SYNC_TIMEZONE")
    if tz_name:
        try:
            return ZoneInfo(tz_name.strip())
        except Exception:
            pass
    return timezone.utc


def _resolve_data_dir():
    """Resolve data directory in a cross-platform way (Windows + WSL translation)."""
    data_dir_env = os.environ.get("DATA_DIR_WSL", "").strip()

    if not data_dir_env:
        return Path.cwd() # Fallback to current directory if not set

    is_windows = os.name == 'nt'

    # Translate WSL path to Windows path (e.g., /mnt/Q/data -> Q:\data)
    if is_windows and data_dir_env.startswith("/mnt/"):
        parts = data_dir_env.split('/')
        if len(parts) >= 3:
            drive_letter = parts[2].upper()
            rest_of_path = "/".join(parts[3:])
            data_dir_env = f"{drive_letter}:\\{rest_of_path}"

    # Translate Windows path to WSL path (e.g., Q:\data -> /mnt/Q/data)
    elif not is_windows and len(data_dir_env) >= 2 and data_dir_env[1] == ':':
        drive_letter = data_dir_env[0].lower()
        rest_of_path = data_dir_env[2:].replace('\\', '/').lstrip('/')
        data_dir_env = f"/mnt/{drive_letter}/{rest_of_path}"

    return Path(data_dir_env)


def _resolve_staging_dir():
    """
    Resolve the staging directory in a cross-platform way (Windows Native + WSL).
    Reads STAGING_DIR_WSL from the environment and applies Windows <-> WSL translations.
    """
    raw = os.environ.get("STAGING_DIR_WSL", "").strip()

    if not raw:
        return _resolve_data_dir()   # Fallback: treat data dir as the only source

    is_windows = os.name == "nt"

    # Translate WSL path -> Windows path (e.g. /mnt/c/Users/... -> C:\Users\...)
    if is_windows and raw.startswith("/mnt/"):
        parts = raw.split("/")
        if len(parts) >= 3:
            drive = parts[2].upper()
            rest  = "\\".join(parts[3:])
            raw   = f"{drive}:\\{rest}"

    # Translate Windows path -> WSL path (e.g. C:\Users\... -> /mnt/c/Users/...)
    elif not is_windows and len(raw) >= 2 and raw[1] == ":":
        drive = raw[0].lower()
        rest  = raw[2:].replace("\\", "/").lstrip("/")
        raw   = f"/mnt/{drive}/{rest}"

    return Path(raw)


LOCAL_TZ = _resolve_local_tz()
ARCHIVE_ZSTD_LEVEL = int(os.environ.get("ARCHIVE_ZSTD_LEVEL", "12"))

JSONL_CHUNK_SIZE = max(1, int(os.environ.get("JSONL_CHUNK_SIZE", "250000")))

# Target Directories
DATA_DIR = _resolve_data_dir()
EVENTS_TARGET_DIR = DATA_DIR / "archive" / "events"
GTFS_TARGET_DIR = DATA_DIR / "archive" / "gtfs"
STAGING_DIR = _resolve_staging_dir()

for d in [EVENTS_TARGET_DIR, GTFS_TARGET_DIR, STAGING_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# File naming and cleanup filters
DATE_PATTERN = re.compile(r'_(\d{8})(?:\.jsonl|\.parquet)?$')
ERROR_PATTERN = re.compile(r'error', re.IGNORECASE)

CORRUPT_DIR = STAGING_DIR / "corrupt"

LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").strip().lower()
RUN_HAD_ERROR = False


#### CORRUPT FILE HANDLING ####

def _tolerant_row_group_read(source):
    """
    Read a parquet file row group by row group, keeping whatever groups decode
    cleanly and skipping corrupt ones. Requires an intact footer. Returns a
    PyArrow Table of recovered rows, or None.
    """
    try:
        pf = pq.ParquetFile(str(source))
        recovered = []
        for rg_index in range(pf.metadata.num_row_groups):
            try:
                recovered.append(pf.read_row_group(rg_index))
            except Exception:
                continue
        if recovered:
            return pa.concat_tables(recovered, promote_options="permissive")
    except Exception:
        pass
    return None


def try_read_parquet_salvage(path):
    """
    Attempt to read a corrupt or partially-written parquet file using fallback strategies.
    Returns a PyArrow Table with whatever rows are recoverable, or None if unreadable.
    """
    # Strategy 1: full table read (sometimes succeeds when ParquetFile init fails)
    try:
        return pq.read_table(str(path))
    except Exception:
        pass
    # Strategy 2: pyarrow dataset API (different code path, handles some edge cases)
    try:
        import pyarrow.dataset as ds
        return ds.dataset(str(path), format="parquet").to_table()
    except Exception:
        pass
    # Strategy 3: memory-mapped read
    try:
        with pa.memory_map(str(path), "r") as mm:
            return pq.read_table(mm)
    except Exception:
        pass
    # Strategy 4: tolerant per-row-group read (footer intact, some pages corrupt)
    partial = _tolerant_row_group_read(path)
    if partial is not None and partial.num_rows > 0:
        log(f"  [SALVAGE] {path.name}: recovered {partial.num_rows:,} rows from intact row groups", is_warning=True)
        return partial
    # Strategy 5: footer reconstruction for truncated or over-appended files.
    # A valid parquet file ends with [4-byte footer length][b"PAR1"]. If the tail
    # magic is missing (write died mid-flush) or garbage was appended after a valid
    # footer, an intact complete file may still exist as a prefix. Scan backward for
    # embedded b"PAR1" markers and try each prefix as a candidate file. Streaming
    # chunked copies keep this safe for multi-GB files.
    try:
        file_size = path.stat().st_size
        if file_size > 12:
            with open(path, "rb") as fh:
                mm = mmap.mmap(fh.fileno(), 0, access=mmap.ACCESS_READ)
                try:
                    # Note: header magic is deliberately not required here — pyarrow
                    # validates the footer, so files with a clobbered header can still
                    # be recovered. Each candidate prefix self-validates via real reads.
                    search_end = file_size
                    for _ in range(8):  # bound the number of candidate prefixes tried
                        magic_at = mm.rfind(b"PAR1", 4, search_end)
                        if magic_at <= 4:
                            break
                        candidate_len = magic_at + 4
                        search_end = magic_at
                        if candidate_len == file_size or candidate_len <= 12:
                            continue  # full-length read already failed above
                        temp_path = None
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as tf:
                                temp_path = Path(tf.name)
                                remaining = candidate_len
                                fh.seek(0)
                                while remaining > 0:
                                    chunk = fh.read(min(64 * 1024 * 1024, remaining))
                                    if not chunk:
                                        break
                                    tf.write(chunk)
                                    remaining -= len(chunk)
                            try:
                                repaired = pq.read_table(str(temp_path))
                            except Exception:
                                repaired = _tolerant_row_group_read(temp_path)
                            if repaired is not None and repaired.num_rows > 0:
                                log(
                                    f"  [REPAIR] {path.name}: rebuilt valid parquet from first "
                                    f"{candidate_len:,} of {file_size:,} bytes ({repaired.num_rows:,} rows)",
                                    is_warning=True,
                                )
                                return repaired
                        except Exception:
                            continue
                        finally:
                            if temp_path is not None:
                                temp_path.unlink(missing_ok=True)
                finally:
                    mm.close()
    except Exception:
        pass
    return None


def ensure_readable_target(target_path, zstd_level):
    """
    Validate that an existing target archive opens as parquet. If it doesn't,
    attempt salvage: recovered rows are rewritten to a clean file that atomically
    replaces the target (the corrupt original is quarantined first). If nothing is
    recoverable, the corrupt file is quarantined so the merge can proceed fresh.
    Returns True if a readable target exists at target_path afterward.
    """
    if not target_path.exists():
        return False
    try:
        with open(target_path, 'rb') as f_chk:
            pq.ParquetFile(f_chk)
        return True
    except Exception as target_ex:
        log(f"  [TARGET CORRUPT] {target_path.name}: {target_ex} — attempting repair", is_warning=True)

    repaired = try_read_parquet_salvage(target_path)
    if repaired is not None and repaired.num_rows > 0:
        repair_tmp = target_path.with_suffix(".repair_tmp")
        try:
            pq.write_table(
                repaired, str(repair_tmp),
                compression="zstd", compression_level=zstd_level,
            )
            quarantine_file(target_path)  # preserve the corrupt original bytes
            repair_tmp.replace(target_path)
            log(f"  [TARGET REPAIRED] {target_path.name}: rewrote {repaired.num_rows:,} recovered rows", is_warning=True)
            return True
        except Exception as repair_ex:
            log(f"  [TARGET REPAIR FAILED] {target_path.name}: {repair_ex}", is_warning=True)
            repair_tmp.unlink(missing_ok=True)

    log(f"  [TARGET UNRECOVERABLE] {target_path.name}: quarantining; merge will rebuild from source shards", is_warning=True)
    quarantine_file(target_path)
    return False


def quarantine_file(path):
    """
    Move an unrecoverable corrupt file to CORRUPT_DIR so subsequent runs skip it.
    Adds a numeric suffix if a same-named file is already quarantined.
    """
    CORRUPT_DIR.mkdir(parents=True, exist_ok=True)
    dest = CORRUPT_DIR / path.name
    counter = 0
    while dest.exists():
        counter += 1
        dest = CORRUPT_DIR / f"{path.stem}.corrupt_{counter}{path.suffix}"
    try:
        shutil.move(str(path), str(dest))
        log(f"  [QUARANTINE] {path.name} → corrupt/{dest.name}", is_warning=True)
    except Exception as qe:
        log(f"  [QUARANTINE FAILED] Could not move {path.name}: {qe}", is_warning=True)


#### LOGGING ####

def log(message, is_error=False, is_warning=False):
    """Simple timestamped logger that writes to stderr, avoiding conflicts with tqdm progress bars."""
    global RUN_HAD_ERROR
    ts = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    if is_error:
        RUN_HAD_ERROR = True
        msg = f"[{ts}] 🔴 ERROR: {message}"
    elif is_warning:
        msg = f"[{ts}] 🟡 WARNING: {message}"
    else:
        msg = f"[{ts}] 🟢 {message}"
    
    # Use tqdm.write instead of sys.stderr.write so it doesn't break the progress bar formatting
    tqdm.write(msg, file=sys.stderr)


def standardize_filenames_in_scan_roots(scan_roots, is_deferred_pass=False):
    """Scan and standardize raw stream names across both staging and final folders recursively."""
    global DEFERRED_RENAMES
    if is_deferred_pass:
        if not DEFERRED_RENAMES:
            return
        log("  Retrying deferred filename standardizations...")
        files_to_process = DEFERRED_RENAMES.copy()
        DEFERRED_RENAMES.clear()

        for source_file, target_file in files_to_process:
            if not source_file.exists():
                continue
            try:
                if source_file.parent == target_file.parent and source_file.name.lower() == target_file.name.lower():
                    temp_file = source_file.parent / (source_file.name + ".case_temp")
                    shutil.move(str(source_file), str(temp_file))
                    shutil.move(str(temp_file), str(target_file))
                else:
                    shutil.move(str(source_file), str(target_file))
                log(f"    [RENAME] Standardized deferred file: {source_file.name} -> {target_file.name}")
            except Exception as e:
                log(f"    [FAIL] Still failed to rename deferred file {source_file.name}: {e}. Leaving in staging.", is_warning=True)
        return

    log("STEP 0: Running filename standardization pass...")
    file_pattern = re.compile(r"^([A-Za-z0-9]+)_(.+)_(\d{8})\.(parquet|jsonl)$")
    rename_count = 0
    skipped_count = 0

    for scan_root in scan_roots:
        if not scan_root.exists():
            continue
        for root, _, files in os.walk(scan_root):
            root_path = Path(root)
            for f in files:
                # Bypass script configurations or logging standard items
                if f in ('standardize_filenames.py', 'clean_and_merge.py') or ERROR_PATTERN.search(f):
                    continue

                match = file_pattern.match(f)
                if not match:
                    continue

                agency = match.group(1)
                stream_raw = match.group(2)
                date_str = match.group(3)
                ext = match.group(4)

                # Normalize the agency token: unwrap legacy "511_<id>" prefixes,
                # fold Capitol Corridor into AMTRAK, uppercase 511 operator ids.
                agency, stream_raw = resolve_agency_token(agency, stream_raw)

                stream_lower = stream_raw.lower()
                standard_stream = STREAM_MAPPING.get(stream_lower, stream_lower)

                target_name = f"{agency}_{standard_stream}_{date_str}.{ext}"
                if f == target_name:
                    continue

                source_file = root_path / f
                target_file = root_path / target_name

                # Overwrite Guard: Do not rename if doing so would overwrite an existing data file.
                # Instead, resolve the collision by moving to Staging with a conflict suffix.
                if target_file.exists():
                    if source_file.parent == target_file.parent and source_file.name.lower() == target_file.name.lower():
                        # Casing-only collision (e.g. Config -> config). Resolve directly using case-safe renaming.
                        pass
                    else:
                        counter = 0
                        resolved = False
                        while not resolved:
                            suffix = f".conflict_{counter}" if counter > 0 else ".conflict"
                            conflict_name = f"{agency}_{standard_stream}_{date_str}{suffix}.{ext}"
                            conflict_file = STAGING_DIR / conflict_name
                            if not conflict_file.exists():
                                target_file = conflict_file
                                resolved = True
                            counter += 1
                        log(f"  [REDIRECT] Target collision detected. Routing source to staging conflict path: {target_file.name} (Source: {f})", is_warning=True)

                try:
                    # Case-safe renaming support for Windows/WSL case-insensitive filesystem paths
                    if source_file.parent == target_file.parent and source_file.name.lower() == target_file.name.lower():
                        temp_file = source_file.parent / (source_file.name + ".case_temp")
                        shutil.move(str(source_file), str(temp_file))
                        shutil.move(str(temp_file), str(target_file))
                    else:
                        shutil.move(str(source_file), str(target_file))
                    log(f"  [RENAME] Standardized: {f} -> {target_file.name}")
                    rename_count += 1
                except Exception as e:
                    log(f"  [DEFER] Failed to rename raw file {f}: {e}. Deferring to end.", is_warning=True)
                    DEFERRED_RENAMES.append((source_file, target_file))

    if rename_count > 0 or skipped_count > 0 or len(DEFERRED_RENAMES) > 0:
        log(f"  Standardization pass complete: renamed={rename_count}, deferred={len(DEFERRED_RENAMES)}")


#### ROW HASH INDEXING ####

# Dedup keys are the leading 128 bits of each row's SHA-256 hash_id, stored as two
# uint64 fields. A Python set of the 64-char hex strings costs ~170 bytes per row,
# so a 6M-row archive burns roughly a gigabyte before a single row is merged and
# the process gets OOM-killed mid-rewrite. 16 bytes per row keeps collisions
# impossible in practice (birthday odds ~1e-20 at these volumes) at a tenth the cost.
HASH_KEY_DTYPE = np.dtype([("hi", "<u8"), ("lo", "<u8")])
_UINT64_MASK = 0xFFFFFFFFFFFFFFFF

# Nibble values for ASCII hex digits; every other byte stays 0 and is rejected by
# the validity mask before it can silently produce a wrong key.
_HEX_NIBBLE_LUT = np.zeros(256, dtype=np.uint8)
_HEX_NIBBLE_LUT[ord("0"):ord("9") + 1] = np.arange(10, dtype=np.uint8)
_HEX_NIBBLE_LUT[ord("a"):ord("f") + 1] = np.arange(10, 16, dtype=np.uint8)
_HEX_NIBBLE_LUT[ord("A"):ord("F") + 1] = np.arange(10, 16, dtype=np.uint8)
_HEX_IS_DIGIT = np.zeros(256, dtype=bool)
_HEX_IS_DIGIT[ord("0"):ord("9") + 1] = True
_HEX_IS_DIGIT[ord("a"):ord("f") + 1] = True
_HEX_IS_DIGIT[ord("A"):ord("F") + 1] = True


def _hex_matrix_from_arrow(array):
    """Expose a fixed-width string column as an (n, 32) uint8 matrix of hex characters.

    Reads straight out of Arrow's value buffer so a multi-million-row hash column
    never becomes multi-million Python string objects. Returns None whenever the
    layout is not uniform 64-byte values, which sends the caller to the slow path.
    """
    if len(array) == 0 or array.null_count:
        return None
    if pa.types.is_large_string(array.type) or pa.types.is_large_binary(array.type):
        offset_dtype = np.int64
    elif pa.types.is_string(array.type) or pa.types.is_binary(array.type):
        offset_dtype = np.int32
    else:
        return None

    buffers = array.buffers()
    if len(buffers) < 3 or buffers[1] is None or buffers[2] is None:
        return None

    itemsize = np.dtype(offset_dtype).itemsize
    offsets = np.frombuffer(
        buffers[1],
        dtype=offset_dtype,
        count=len(array) + 1,
        offset=array.offset * itemsize,
    )
    if not np.all(offsets[1:] - offsets[:-1] == 64):
        return None

    values = np.frombuffer(buffers[2], dtype=np.uint8, count=64 * len(array), offset=int(offsets[0]))
    return values.reshape(-1, 64)[:, :32]


def _keys_from_hex_matrix(matrix):
    """Fold 32 hex characters per row into two uint64 halves. None if any byte isn't hex."""
    if not _HEX_IS_DIGIT[matrix].all():
        return None

    nibbles = _HEX_NIBBLE_LUT[matrix]
    keys = np.empty(matrix.shape[0], dtype=HASH_KEY_DTYPE)
    high = np.zeros(matrix.shape[0], dtype=np.uint64)
    low = np.zeros(matrix.shape[0], dtype=np.uint64)
    shift = np.uint64(4)
    # Shift-accumulate rather than multiplying a widened (n, 32) uint64 matrix,
    # which would allocate 256 bytes of scratch per row on every block.
    for column in range(16):
        high <<= shift
        high |= nibbles[:, column]
        low <<= shift
        low |= nibbles[:, 16 + column]
    keys["hi"] = high
    keys["lo"] = low
    return keys


def _keys_from_python(values):
    """Per-row fallback for null, short, or non-hex hash_id values."""
    keys = np.empty(len(values), dtype=HASH_KEY_DTYPE)
    for position, value in enumerate(values):
        text = "" if value is None else str(value)
        number = None
        if len(text) == 64:
            try:
                number = int(text, 16) >> 128
            except ValueError:
                number = None
        if number is None:
            number = int.from_bytes(hashlib.blake2b(text.encode("utf-8"), digest_size=16).digest(), "big")
        keys["hi"][position] = number >> 64
        keys["lo"][position] = number & _UINT64_MASK
    return keys


def hash_keys_from_column(column):
    """Convert a hash_id column (Array or ChunkedArray) into compact dedup keys."""
    chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
    parts = []
    for chunk in chunks:
        if len(chunk) == 0:
            continue
        matrix = _hex_matrix_from_arrow(chunk)
        keys = _keys_from_hex_matrix(matrix) if matrix is not None else None
        if keys is None:
            keys = _keys_from_python(chunk.to_pylist())
        parts.append(keys)

    if not parts:
        return np.empty(0, dtype=HASH_KEY_DTYPE)
    return parts[0] if len(parts) == 1 else np.concatenate(parts)


class HashKeyIndex:
    """Membership set over 128-bit row hashes, backed by sorted numpy blocks.

    Additions land in a pending block that is folded into the main block only once
    it passes the compaction threshold, so appending millions of rows never costs a
    full re-sort per row group.
    """

    __slots__ = ("_main", "_pending", "_compact_threshold")

    def __init__(self, seed_keys=None, compact_threshold=1_000_000):
        self._main = seed_keys if seed_keys is not None else np.empty(0, dtype=HASH_KEY_DTYPE)
        self._pending = np.empty(0, dtype=HASH_KEY_DTYPE)
        self._compact_threshold = compact_threshold

    @staticmethod
    def _block_contains(block, keys):
        if block.size == 0 or keys.size == 0:
            return np.zeros(keys.size, dtype=bool)
        positions = np.searchsorted(block, keys)
        np.minimum(positions, block.size - 1, out=positions)
        # Compare through a plain uint64 view: elementwise equality on structured
        # dtypes has shifted behaviour across numpy releases.
        block_pairs = block.view(np.uint64).reshape(-1, 2)
        key_pairs = keys.view(np.uint64).reshape(-1, 2)
        return ((block_pairs[positions, 0] == key_pairs[:, 0])
                & (block_pairs[positions, 1] == key_pairs[:, 1]))

    def contains(self, keys):
        found = self._block_contains(self._main, keys)
        if self._pending.size:
            found |= self._block_contains(self._pending, keys)
        return found

    def add(self, keys):
        if keys.size == 0:
            return
        self._pending = np.unique(np.concatenate([self._pending, keys]))
        if self._pending.size >= self._compact_threshold:
            self._main = np.unique(np.concatenate([self._main, self._pending]))
            self._pending = np.empty(0, dtype=HASH_KEY_DTYPE)

    @property
    def size(self):
        return self._main.size + self._pending.size


def select_unseen_event_rows(table_block, hash_index):
    """Keep only rows whose hash is neither already archived nor already written this run."""
    if "hash_id" not in table_block.column_names or table_block.num_rows == 0:
        return table_block

    keys = hash_keys_from_column(table_block.column("hash_id"))
    fresh_positions = np.flatnonzero(~hash_index.contains(keys))
    if fresh_positions.size == 0:
        return table_block.slice(0, 0)

    unique_keys, first_seen = np.unique(keys[fresh_positions], return_index=True)
    if unique_keys.size != fresh_positions.size:
        # np.unique returns positions in sorted-key order; re-sort to preserve the
        # original row order so the archive stays append-ordered.
        fresh_positions = fresh_positions[np.sort(first_seen)]
    hash_index.add(unique_keys)

    if fresh_positions.size == table_block.num_rows:
        return table_block
    return table_block.take(pa.array(fresh_positions, type=pa.int64()))


#### FILE PROCESSING ####

def row_hash(table_name, row_dict):
    """Compute a deterministic SHA-256 hash for a row dict. (Optimized)"""
    # Optimized: Avoid multi-stage comprehension and use rapid dictionary generation
    ignore_keys = {'hash_id', 'first_logged', 'last_logged'}
    cleaned = {k: v for k, v in row_dict.items() if k not in ignore_keys and not pd.isna(v)}
    payload = json.dumps({"table": table_name, "row": cleaned}, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def convert_jsonl_to_parquet(source_path, target_path, chunk_size=None):
    """Convert a line-delimited JSONL file into a compressed Parquet file."""
    if chunk_size is None:
        chunk_size = JSONL_CHUNK_SIZE

    # NOTE: pandas' pyarrow JSON engine does NOT support the chunksize parameter
    # (it raises "currently pyarrow engine doesn't support chunksize parameter").
    # Since we stream large JSONL files in chunks to bound memory, we must use the
    # default ujson/python engine here. The pyarrow engine is only usable for a
    # single-shot (unchunked) read, which we deliberately avoid for large files.
    read_kwargs = {
        "path_or_buf": source_path,
        "lines": True,
        "chunksize": chunk_size,
        "dtype": str,
    }

    parquet_writer = None
    try:
        sub_pbar = tqdm(
            desc=f"  🔵 Chunks ({source_path.name[:30]})",
            unit="chunk",
            leave=False,
            mininterval=0.5,
            bar_format="{desc}: {n_fmt} {unit} [{elapsed}] {postfix}",
            file=sys.stderr,
        )

        json_reader = pd.read_json(**read_kwargs)

        writer_schema = None
        total_rows = 0
        for chunk in json_reader:
            if chunk.empty:
                continue

            table = pa.Table.from_pandas(chunk, preserve_index=False)

            if parquet_writer is None:
                writer_schema = table.schema
                parquet_writer = pq.ParquetWriter(
                    str(target_path),
                    writer_schema,
                    compression="zstd",
                    # OPTIMIZATION: This file is purely a temporary staging file that will be read
                    # and merged into the final archive a minute later. Compressing it at Level 12
                    # is a huge waste of CPU. Level 3 writes 5x-10x faster.
                    compression_level=3,
                )
            elif not table.schema.equals(writer_schema):
                aligned_columns = []
                for field in writer_schema:
                    idx = table.schema.get_field_index(field.name)
                    if idx != -1:
                        col = table.column(idx)
                        if not col.type.equals(field.type):
                            col = pc.cast(col, field.type, safe=False)
                        aligned_columns.append(col)
                    else:
                        aligned_columns.append(pa.nulls(table.num_rows, type=field.type))
                table = pa.Table.from_arrays(aligned_columns, schema=writer_schema)

            parquet_writer.write_table(table)
            sub_pbar.update(1)
            total_rows += len(chunk)
            sub_pbar.set_postfix_str(f"rows={total_rows:,}", refresh=False)

        sub_pbar.close()
        if parquet_writer is not None:
            parquet_writer.close()

        gc.collect()
        return True
    except Exception as e:
        if parquet_writer is not None:
            try: parquet_writer.close()
            except: pass
        target_path.unlink(missing_ok=True)
        log(f"  Failed to convert {source_path.name}: {e}", is_error=True)
        return False


def run_cleanup_and_merge():
    """Main entrypoint for the offline cleanup and merge workflow."""
    global RUN_HAD_ERROR
    RUN_HAD_ERROR = False

    scan_roots = [DATA_DIR]
    if STAGING_DIR.resolve() != DATA_DIR.resolve() and STAGING_DIR.exists():
        scan_roots.append(STAGING_DIR)

    log(f"Starting cleanup scan. Final archive: {DATA_DIR} | Staging: {STAGING_DIR}")
    run_started = datetime.now(LOCAL_TZ)

    # PRE-STEP: Purge error + system_config files from staging (and system_config from archive)
    log("PRE-STEP: Purging unwanted files...")
    if STAGING_DIR.exists():
        purged = 0
        staging_parquets = list(STAGING_DIR.rglob("*.parquet"))
        for err_file in tqdm(staging_parquets, desc="  Scanning staging for error files", unit="file", file=sys.stderr, leave=False):
            if ERROR_PATTERN.search(err_file.name):
                try:
                    err_file.unlink(missing_ok=True)
                    log(f"  [PURGE] Deleted error parquet: {err_file.name}")
                    purged += 1
                except Exception as purge_ex:
                    log(f"  [PURGE] Could not delete {err_file.name}: {purge_ex}", is_warning=True)
        if purged:
            log(f"  Error parquet purge: removed {purged} file(s).")

    # Delete system_config files (parquet + jsonl) from staging and archive — no useful transit data
    purged_cfg = 0
    for cfg_dir in [STAGING_DIR, EVENTS_TARGET_DIR]:
        if not cfg_dir.exists():
            continue
        for cfg_file in cfg_dir.rglob("*"):
            if (cfg_file.is_file()
                    and "system_config" in cfg_file.name.lower()
                    and cfg_file.suffix in (".parquet", ".jsonl")):
                try:
                    cfg_file.unlink(missing_ok=True)
                    log(f"  [PURGE] Deleted system_config file: {cfg_file.name}")
                    purged_cfg += 1
                except Exception as e:
                    log(f"  [PURGE] Could not delete {cfg_file.name}: {e}", is_warning=True)
    if purged_cfg:
        log(f"  system_config purge: removed {purged_cfg} file(s).")

    # PRE-STEP: Filter known error rows from SRTA staging parquets
    # BusTime returns {"stop": null, "msg": "Unsupported function"} or
    # {"stop": {"noPredictionMessage": "Stop not found"}, "msg": null} for bad stops/endpoints.
    def _is_srta_error_payload(data_val):
        if isinstance(data_val, str):
            try:
                data_val = json.loads(data_val)
            except Exception:
                return False
        if not isinstance(data_val, dict):
            return False
        if data_val.get("msg") == "Unsupported function":
            return True
        stop_val = data_val.get("stop")
        if isinstance(stop_val, dict) and stop_val.get("noPredictionMessage") == "Stop not found":
            return True
        return False

    def _read_parquet_with_salvage(path):
        """Try normal read; fall back to salvage; quarantine if unrecoverable. Returns table or None."""
        try:
            return pq.read_table(str(path))
        except Exception:
            pass
        tbl = try_read_parquet_salvage(path)
        if tbl is not None:
            log(f"  [SALVAGE] Recovered {path.name} via fallback read", is_warning=True)
            return tbl
        log(f"  [CORRUPT] {path.name}: unrecoverable — quarantining", is_warning=True)
        quarantine_file(path)
        return None

    srta_filtered = 0
    if STAGING_DIR.exists():
        srta_files = sorted(STAGING_DIR.rglob("SRTA_*.parquet"))
        for srta_path in tqdm(srta_files, desc="  Filtering SRTA error rows", unit="file", file=sys.stderr, leave=False):
            try:
                tbl = _read_parquet_with_salvage(srta_path)
                if tbl is None or "data" not in tbl.schema.names:
                    continue
                data_col = tbl.column("data").to_pylist()
                keep_mask = [not _is_srta_error_payload(v) for v in data_col]
                dropped = len(keep_mask) - sum(keep_mask)
                if dropped == 0:
                    continue
                kept = sum(keep_mask)
                if kept == 0:
                    srta_path.unlink(missing_ok=True)
                    log(f"  [FILTER] Deleted SRTA file (all rows were errors): {srta_path.name}")
                else:
                    keep_arr = pa.array(keep_mask, type=pa.bool_())
                    filtered_tbl = tbl.filter(keep_arr)
                    tmp = srta_path.with_suffix(".filter_tmp")
                    pq.write_table(filtered_tbl, str(tmp), compression="zstd")
                    tmp.replace(srta_path)
                    log(f"  [FILTER] SRTA {srta_path.name}: dropped {dropped} error rows, kept {kept}")
                srta_filtered += dropped
            except Exception as e:
                log(f"  [FILTER] Could not filter {srta_path.name}: {e}", is_warning=True)
    if srta_filtered:
        log(f"  SRTA error-row filter: removed {srta_filtered} rows total.")

    # PRE-STEP: Filter BRTA "Request Rejected" HTML rows from staging parquets + JSONLs
    def _is_brta_rejection_payload(data_val):
        text = None
        if isinstance(data_val, str):
            try:
                parsed = json.loads(data_val)
                text = parsed.get("data") if isinstance(parsed, dict) else data_val
                if not isinstance(text, str): text = None
            except Exception:
                text = data_val
        elif isinstance(data_val, dict):
            inner = data_val.get("data")
            if isinstance(inner, str):
                text = inner
        return bool(text and "<title>Request Rejected</title>" in text)

    brta_filtered = 0
    brta_parquets = sorted(STAGING_DIR.rglob("BRTA_*.parquet")) if STAGING_DIR.exists() else []
    for brta_path in tqdm(brta_parquets, desc="  Filtering BRTA rejection rows", unit="file", file=sys.stderr, leave=False):
        try:
            tbl = _read_parquet_with_salvage(brta_path)
            if tbl is None or "data" not in tbl.schema.names:
                continue
            data_col = tbl.column("data").to_pylist()
            keep_mask = [not _is_brta_rejection_payload(v) for v in data_col]
            dropped = len(keep_mask) - sum(keep_mask)
            if dropped == 0:
                continue
            kept = sum(keep_mask)
            if kept == 0:
                brta_path.unlink(missing_ok=True)
                log(f"  [FILTER] Deleted BRTA file (all rows were rejections): {brta_path.name}")
            else:
                keep_arr = pa.array(keep_mask, type=pa.bool_())
                filtered_tbl = tbl.filter(keep_arr)
                tmp = brta_path.with_suffix(".filter_tmp")
                pq.write_table(filtered_tbl, str(tmp), compression="zstd")
                tmp.replace(brta_path)
                log(f"  [FILTER] BRTA {brta_path.name}: dropped {dropped} rejection rows, kept {kept}")
            brta_filtered += dropped
        except Exception as e:
            log(f"  [FILTER] Could not filter {brta_path.name}: {e}", is_warning=True)

    brta_jsonls = sorted(STAGING_DIR.rglob("BRTA_*.jsonl")) if STAGING_DIR.exists() else []
    for brta_jsonl in tqdm(brta_jsonls, desc="  Filtering BRTA JSONL rejections", unit="file", file=sys.stderr, leave=False):
        try:
            kept_lines = []
            dropped = 0
            with open(brta_jsonl, "r", encoding="utf-8") as fh:
                for line in fh:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    try:
                        rec = json.loads(stripped)
                        if _is_brta_rejection_payload(rec.get("data")):
                            dropped += 1
                            continue
                    except Exception:
                        pass
                    kept_lines.append(line)
            if dropped:
                if not kept_lines:
                    brta_jsonl.unlink(missing_ok=True)
                    log(f"  [FILTER] Deleted BRTA JSONL (all rows were rejections): {brta_jsonl.name}")
                else:
                    with open(brta_jsonl, "w", encoding="utf-8") as fh:
                        fh.writelines(kept_lines)
                    log(f"  [FILTER] BRTA {brta_jsonl.name}: dropped {dropped} rejection rows")
                brta_filtered += dropped
        except Exception as e:
            log(f"  [FILTER] Could not filter {brta_jsonl.name}: {e}", is_warning=True)

    if brta_filtered:
        log(f"  BRTA rejection-row filter: removed {brta_filtered} rows total.")

    # STEP 0: Standardization Pass
    standardize_filenames_in_scan_roots(scan_roots)

    def canonical_parquet_name(path_obj):
        """Normalize shard names so tmp/clean/conflict variants merge into their base parquet."""
        name = path_obj.name.replace(".from_jsonl", "")
        # Remove conflict numbering and standard processing tokens cleanly
        name = re.sub(r"\.conflict(_\d+)?", "", name)
        if name.endswith(".parquet.tmp"):
            return name[:-4]
        if name.endswith(".parquet.clean"):
            return name[:-6]
        return name

    def classify_source(normalized_name):
        """Classify a shard name and return (kind, canonical target filename)."""
        lowered = normalized_name.lower()
        date_match = DATE_PATTERN.search(normalized_name)

        if "gtfs" in lowered:
            return "gtfs", normalized_name
        if date_match:
            return "event", normalized_name
        return "skip", normalized_name

    def archive_ingested_source(seg_path):
        """Delete consumed source shards after successful merge."""
        if not seg_path.exists():
            return
        if GTFS_TARGET_DIR in seg_path.parents or EVENTS_TARGET_DIR in seg_path.parents:
            return
        seg_path.unlink(missing_ok=True)

    def source_shard_id(path_obj):
        """Create a stable identity for a source shard so reruns can skip already-merged inputs."""
        try:
            rel_path = path_obj.relative_to(DATA_DIR).as_posix()
        except ValueError:
            try:
                rel_path = "staging/" + path_obj.relative_to(STAGING_DIR).as_posix()
            except ValueError:
                rel_path = path_obj.name

        try:
            stat = path_obj.stat()
            return f"{rel_path}|{stat.st_size}|{int(stat.st_mtime_ns)}"
        except Exception:
            return rel_path

    def read_parquet_safe(pf, columns=None, row_group=None):
        """Read a Parquet file or row group without thread parallelism to limit memory overhead."""
        if row_group is None:
            return pf.read(columns=columns, use_threads=False)
        return pf.read_row_group(row_group, columns=columns, use_threads=False)

    def read_manifest_ids(manifest_path):
        """Load processed shard IDs from a manifest file."""
        if not manifest_path.exists():
            return set()
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            if isinstance(payload, list):
                return set(str(item) for item in payload)
        except Exception:
            pass
        return set()

    def write_manifest_ids(manifest_path, processed_ids):
        """Atomically persist processed shard IDs after a successful merge commit."""
        temp_manifest = manifest_path.with_suffix(manifest_path.suffix + ".tmp")
        temp_manifest.write_text(json.dumps(sorted(processed_ids), indent=2), encoding="utf-8")
        temp_manifest.replace(manifest_path)

    def manifest_has_source(processed_ids, path_obj):
        """Support both legacy name-only manifests and current source IDs."""
        shard_id = source_shard_id(path_obj)
        return shard_id in processed_ids or path_obj.name in processed_ids

    def is_probably_same_parquet(source_path, target_path):
        """Cheap structural check used to skip obvious duplicate shard rewrites."""
        try:
            if not source_path.exists() or not target_path.exists():
                return False
            if source_path.stat().st_size != target_path.stat().st_size:
                return False
            with open(source_path, 'rb') as f_source, open(target_path, 'rb') as f_target:
                source_pf = pq.ParquetFile(f_source)
                target_pf = pq.ParquetFile(f_target)
                if source_pf.metadata.num_rows != target_pf.metadata.num_rows:
                    return False
                if source_pf.metadata.num_row_groups != target_pf.metadata.num_row_groups:
                    return False
                return source_pf.schema_arrow.remove_metadata().equals(target_pf.schema_arrow.remove_metadata())
        except Exception:
            return False

    def resolve_compatible_schema(source_schemas):
        """Resolve mixed schemas by falling back to string for conflicting field types."""
        field_order = []
        field_meta = {}
        field_types = {}

        for schema in source_schemas:
            for field in schema:
                if field.name not in field_order:
                    field_order.append(field.name)
                    field_meta[field.name] = field.metadata
                    field_types[field.name] = []

                if all(not existing_type.equals(field.type) for existing_type in field_types[field.name]):
                    field_types[field.name].append(field.type)

        resolved_fields = []
        for field_name in field_order:
            types = [field_type for field_type in field_types[field_name] if not pa.types.is_null(field_type)]
            if not types:
                resolved_type = pa.string()
            elif len(types) == 1:
                resolved_type = types[0]
            else:
                resolved_type = pa.string()

            resolved_fields.append(pa.field(field_name, resolved_type, nullable=True, metadata=field_meta.get(field_name)))

        return pa.schema(resolved_fields)

    def coerce_series_to_field(series, field):
        """Coerce a series to a target field, stringifying as a last-resort fallback."""
        if series.type.equals(field.type):
            return series

        try:
            return pc.cast(series, field.type, safe=False)
        except Exception:
            if pa.types.is_string(field.type):
                return pa.array(
                    [None if value is None else str(value) for value in series.to_pylist()],
                    type=pa.string(),
                )

            return pa.nulls(len(series), type=field.type)

    jsonl_files = []
    parquet_paths = []

    for scan_root in scan_roots:
        for root, _, files in os.walk(scan_root):
            for f in files:
                full_path = Path(root) / f

                # Skip standard active scripts or empty/corrupt files
                if f in ('standardize_filenames.py', 'clean_and_merge.py') or ERROR_PATTERN.search(f) or full_path.stat().st_size == 0:
                    full_path.unlink(missing_ok=True)
                    continue

                if f.endswith('.jsonl'):
                    jsonl_files.append((Path(root), f, full_path))
                elif f.endswith('.parquet') or f.endswith('.parquet.tmp') or f.endswith('.parquet.clean'):
                    parquet_paths.append(full_path)

    # STEP 1: JSONL to Parquet conversion
    log("STEP 1: Converting JSONL logs to Parquet frames...")
    if jsonl_files:
        log(f"  Found {len(jsonl_files)} active JSONL files requiring format conversion.")
        converted_jsonl = 0
        failed_jsonl = 0
        pbar_1 = tqdm(
            jsonl_files,
            desc="🔵 Step 1: Converting JSONL files to Parquet",
            file=sys.stderr
        )
        for folder, filename, full_path in pbar_1:
            pbar_1.set_postfix_str(f"Current: {filename[:30]}", refresh=True)

            parquet_name = filename.replace('.jsonl', '.parquet')
            local_parquet_path = folder / parquet_name
            if local_parquet_path.exists():
                local_parquet_path = folder / f"{Path(parquet_name).stem}.from_jsonl.parquet"

            if convert_jsonl_to_parquet(full_path, local_parquet_path):
                converted_jsonl += 1
                full_path.unlink(missing_ok=True)
                parquet_paths.append(local_parquet_path)
            else:
                failed_jsonl += 1
        pbar_1.close()
        log(f"  STEP 1 complete: converted={converted_jsonl}, failed={failed_jsonl}")
    else:
        log("  Found 0 active JSONL files requiring format conversion. Skipping step.")


    # STEP 2: Group files by target output type
    log("STEP 2: Classifying and grouping source shards...")
    gtfs_grouping = {}
    events_grouping = {}

    for path in parquet_paths:
        name = canonical_parquet_name(path)

        if EVENTS_TARGET_DIR in path.parents or GTFS_TARGET_DIR in path.parents:
            continue

        shard_kind, target_name = classify_source(name)
        if shard_kind == "event":
            events_grouping.setdefault(target_name, []).append(path)
        elif shard_kind == "gtfs":
            gtfs_grouping.setdefault(name, []).append(path)

    if len(gtfs_grouping) > 0 or len(events_grouping) > 0:
        log(f"  STEP 2 complete: GTFS groups={len(gtfs_grouping)}, event groups={len(events_grouping)}")
    else:
        log("  No active shards found to group. Skipping step.")

    # STEP 3: Merge static GTFS tables
    if gtfs_grouping:
        log("STEP 3: Merging duplicate GTFS tables...")
        def normalize_gtfs_schema(schema):
            """Avoid impossible casts by promoting null-typed GTFS columns to string."""
            normalized_fields = []
            for field in schema:
                if pa.types.is_null(field.type) or pa.types.is_large_string(field.type):
                    normalized_fields.append(pa.field(field.name, pa.string(), nullable=True, metadata=field.metadata))
                else:
                    normalized_fields.append(field)
            return pa.schema(normalized_fields, metadata=schema.metadata)

        def make_nullable_schema(schema):
            return pa.schema([
                pa.field(field.name, field.type, nullable=True, metadata=field.metadata)
                for field in schema
            ], metadata=schema.metadata)

        def aligned_gtfs_table(table, target_schema):
            table_schema = table.schema
            aligned_columns = []
            for field in target_schema:
                field_index = table_schema.get_field_index(field.name)
                if field_index != -1:
                    series = table.column(field_index)
                    if not series.type.equals(field.type):
                        series = pc.cast(series, field.type, safe=False)
                else:
                    series = pa.nulls(table.num_rows, type=field.type)
                aligned_columns.append(series)
            return pa.Table.from_arrays(aligned_columns, schema=target_schema)

        pbar_2 = tqdm(
            gtfs_grouping.items(),
            desc="🔵 STEP 3: Merging duplicate GTFS tables",
            file=sys.stderr,
        )
        for final_name, segments in pbar_2:
            pbar_2.set_postfix_str(f"→ {final_name[:30]}", refresh=True)

            final_target_path = GTFS_TARGET_DIR / final_name
            temp_target_path = GTFS_TARGET_DIR / f"{final_name}.tmp"

            consumable_segments = []
            table_name = final_name.replace("MBTA_gtfs_", "").replace(".parquet", "")
            logged_at = datetime.now(LOCAL_TZ).isoformat(timespec="seconds")

            arrow_tables = []
            if final_target_path.exists():
                try:
                    with open(final_target_path, 'rb') as f_ex:
                        existing_table = pq.read_table(f_ex)
                    if existing_table.num_rows > 0:
                        arrow_tables.append(existing_table)
                except Exception as load_ex:
                    # Without recovery, proceeding would REPLACE the archive with
                    # segment rows only — silently discarding its history.
                    log(f"  Could not load existing GTFS archive {final_name}: {load_ex} — attempting salvage", is_warning=True)
                    salvaged_existing = try_read_parquet_salvage(final_target_path)
                    if salvaged_existing is not None and salvaged_existing.num_rows > 0:
                        arrow_tables.append(salvaged_existing)
                        quarantine_file(final_target_path)  # preserve corrupt original; merge rewrites the target
                    else:
                        log(f"  [TARGET UNRECOVERABLE] {final_name}: quarantining; rebuilding from source shards", is_warning=True)
                        quarantine_file(final_target_path)

            merge_success = False
            try:
                for seg_path in segments:
                    try:
                        with open(seg_path, 'rb') as f_seg:
                            pf = pq.ParquetFile(f_seg)
                            sub_pbar = tqdm(
                                desc=f"  🔵 Reading ({seg_path.name[:20]})",
                                total=pf.metadata.num_rows,
                                unit="row",
                                leave=False,
                                file=sys.stderr
                            )
                            for rg in range(pf.metadata.num_row_groups):
                                rg_table = pf.read_row_group(rg)
                                sub_pbar.update(rg_table.num_rows)
                                if rg_table.num_rows > 0:
                                    arrow_tables.append(rg_table)
                            sub_pbar.close()
                        consumable_segments.append(seg_path)
                    except Exception as seg_ex:
                        salvaged = try_read_parquet_salvage(seg_path)
                        if salvaged is not None and salvaged.num_rows > 0:
                            arrow_tables.append(salvaged)
                            # Quarantine (not delete) the corrupt original: recovered rows
                            # are merged, but the source is preserved for manual recovery
                            # and won't fail-read again on subsequent runs.
                            quarantine_file(seg_path)
                        else:
                            log(f"  [CORRUPT] GTFS shard {seg_path.name}: unrecoverable ({seg_ex}) — quarantining", is_warning=True)
                            quarantine_file(seg_path)
                    continue

                if not arrow_tables:
                    merge_success = bool(consumable_segments)
                else:
                    total_src_rows = sum(t.num_rows for t in arrow_tables)
                    log(f"  [{final_name}] Combining {len(arrow_tables)} source blocks ({total_src_rows:,} rows)...")
                    # Single Arrow-level concat + one pandas conversion is far cheaper
                    # than converting each row group to a DataFrame and pd.concat-ing.
                    try:
                        combined = pa.concat_tables(arrow_tables, promote_options="permissive").to_pandas()
                    except (pa.ArrowInvalid, pa.ArrowTypeError):
                        # Fall back to pandas concat if shard schemas are genuinely incompatible
                        combined = pd.concat(
                            [t.to_pandas() for t in arrow_tables], ignore_index=True, sort=False
                        )
                    del arrow_tables

                    if 'hash_id' not in combined.columns:
                        combined['hash_id'] = [row_hash(table_name, r) for r in combined.to_dict(orient="records")]
                    if 'first_logged' not in combined.columns:
                        combined['first_logged'] = logged_at
                    if 'last_logged' not in combined.columns:
                        combined['last_logged'] = combined['first_logged']

                    # Optimized timestamp column replacement
                    for ts_col in ('first_logged', 'last_logged'):
                        if ts_col in combined.columns:
                            combined[ts_col] = combined[ts_col].astype(str).str.strip()
                            mask = combined[ts_col].isin({'None', 'nan', 'NaT', '<NA>', ''})
                            if mask.any():
                                combined.loc[mask, ts_col] = logged_at

                    # Rows sharing a hash_id are identical in every data column by
                    # construction (the hash covers all non-null data values), so only
                    # the timestamp columns need real aggregation. drop_duplicates is
                    # C-optimized and avoids the very slow per-column agg('first') path.
                    if combined['hash_id'].is_unique:
                        merged_df = combined
                    else:
                        log(f"  [{final_name}] Deduplicating {len(combined):,} rows by hash_id...")
                        # Aggregate timestamps as datetime64 (C-speed min/max) rather than
                        # object-dtype strings, which fall into slow Python-level comparison.
                        # This also makes min/max chronological even across mixed formats.
                        # NOTE: timestamps in this pipeline mix naive local values with
                        # local-offset ISO values (e.g. "...T00:00:00-04:00"). Mixed offsets
                        # make pandas return object dtype (breaking .agg and .dt), and
                        # utc=True would misread the naive values — so the offset suffix is
                        # stripped first, yielding uniform naive local wall-clock datetimes.
                        # Timestamp cardinality is tiny relative to row count (a handful of
                        # daily logging batches), so only the UNIQUE values are parsed and
                        # the result mapped back — avoiding a per-row 'mixed' format parse
                        # that can silently take minutes-to-hours on tables like stop_times.
                        def _parse_logged(series):
                            lookup_keys = series.astype(str)
                            parse_map = {
                                raw: pd.to_datetime(
                                    re.sub(r'(?:[+-]\d{2}:?\d{2}|Z)$', '', raw),
                                    format='mixed', errors='coerce'
                                )
                                for raw in lookup_keys.unique()
                            }
                            # Re-coerce so the mapped result is guaranteed datetime64
                            return pd.to_datetime(lookup_keys.map(parse_map), errors='coerce')

                        ts_stats = pd.DataFrame({
                            'hash_id': combined['hash_id'],
                            'fl': _parse_logged(combined['first_logged']),
                            'll': _parse_logged(combined['last_logged']),
                        }).groupby('hash_id', sort=False).agg(fl=('fl', 'min'), ll=('ll', 'max'))

                        merged_df = combined.drop_duplicates(subset='hash_id', keep='first').copy()
                        merged_df['first_logged'] = (
                            merged_df['hash_id'].map(ts_stats['fl']).dt.strftime('%Y-%m-%dT%H:%M:%S').fillna(logged_at)
                        )
                        merged_df['last_logged'] = (
                            merged_df['hash_id'].map(ts_stats['ll']).dt.strftime('%Y-%m-%dT%H:%M:%S').fillna(logged_at)
                        )

                    log(f"  [{final_name}] Writing {len(merged_df):,} merged rows...")
                    base_table = pa.Table.from_pandas(merged_df, preserve_index=False)
                    del merged_df
                    if 'combined' in locals() and combined is not None:
                        del combined
                    gc.collect()
                    target_schema = make_nullable_schema(normalize_gtfs_schema(base_table.schema))
                    aligned = aligned_gtfs_table(base_table, target_schema)
                    del base_table

                    writer = pq.ParquetWriter(
                        str(temp_target_path),
                        target_schema,
                        compression="zstd",
                        compression_level=ARCHIVE_ZSTD_LEVEL,
                    )
                    # Write in bounded batches: keeps memory flat during compression and
                    # gives visible progress on very large tables (e.g. stop_times).
                    write_batch_rows = 250_000
                    write_pbar = tqdm(
                        desc=f"  🔵 Writing ({final_name[:20]})",
                        total=aligned.num_rows,
                        unit="row",
                        leave=False,
                        file=sys.stderr,
                    )
                    for batch_start in range(0, aligned.num_rows, write_batch_rows):
                        batch = aligned.slice(batch_start, write_batch_rows)
                        writer.write_table(batch)
                        write_pbar.update(batch.num_rows)
                    write_pbar.close()
                    writer.close()

                    temp_target_path.replace(final_target_path)
                    merge_success = True
                    gc.collect()

            except Exception as gx:
                log(f"  Failed to merge GTFS table {final_name}: {gx}", is_error=True)
                temp_target_path.unlink(missing_ok=True)

            if merge_success:
                for seg_path in consumable_segments:
                    archive_ingested_source(seg_path)
        pbar_2.close()
        log(f"  STEP 3 complete: merged {len(gtfs_grouping)} GTFS tables.")

    # STEP 4: Merge event tables
    if events_grouping:
        log("STEP 4: Merging duplicate event tables...")
        
        # Sort groups by total size (smallest files first)
        def get_group_size(item):
            final_name, segments = item
            total_bytes = sum(seg.stat().st_size for seg in segments if seg.exists())
            target_path = EVENTS_TARGET_DIR / final_name
            if target_path.exists():
                total_bytes += target_path.stat().st_size
            return total_bytes
            
        sorted_events = sorted(events_grouping.items(), key=get_group_size)

        pbar_3 = tqdm(
            sorted_events,
            desc="🔵 STEP 4: Merging duplicate event tables",
            file=sys.stderr,
        )
        for final_name, segments in pbar_3:
            pbar_3.set_postfix_str(f"→ {final_name[:30]}", refresh=True)

            final_target_path = EVENTS_TARGET_DIR / final_name
            temp_target_path = EVENTS_TARGET_DIR / f"{final_name}.tmp"
            manifest_path = final_target_path.with_suffix(".manifest.json")
            processed_ids = read_manifest_ids(manifest_path)

            # A corrupt existing target would otherwise fail every read below and
            # abort this group's merge on every run. Repair or quarantine it first.
            ensure_readable_target(final_target_path, ARCHIVE_ZSTD_LEVEL)

            if len(segments) == 1 and not final_target_path.exists():
                only_segment = segments[0]
                try:
                    shutil.move(str(only_segment), str(final_target_path))
                    log(f"  STEP 4 fast-path move: {only_segment.name} -> {final_target_path.name}")
                    continue
                except Exception as move_ex:
                    log(f"  STEP 4 fast-path fallback for {final_name}: {move_ex}", is_warning=True)

            merge_success = False
            consumable_segments = []
            merge_segments = []
            dedup_db_path = None
            dedup_conn = None

            for seg_path in segments:
                shard_id = source_shard_id(seg_path)
                if manifest_has_source(processed_ids, seg_path):
                    consumable_segments.append(seg_path)
                    continue
                if final_target_path.exists() and is_probably_same_parquet(seg_path, final_target_path):
                    consumable_segments.append(seg_path)
                    processed_ids.add(shard_id)
                    log(f"  STEP 4 fast-skip duplicate shard: {seg_path.name}")
                else:
                    merge_segments.append(seg_path)

            if not merge_segments and consumable_segments:
                write_manifest_ids(manifest_path, processed_ids)
                merge_success = True

            try:
                log(f"  STEP 4 processing {final_name}: source segments={len(segments)}")
                event_text_fields = {"data"}
                EVENT_DROP_FIELDS = {"first_logged", "last_logged"}

                def normalize_event_schema(schema):
                    return pa.schema([
                        pa.field(field.name, pa.string()) if (field.name in event_text_fields or pa.types.is_large_string(field.type)) else field
                        for field in schema
                        if field.name not in EVENT_DROP_FIELDS
                    ], metadata=schema.metadata)

                def normalize_event_table(table):
                    cols_to_drop = [c for c in table.column_names if c in EVENT_DROP_FIELDS]
                    if cols_to_drop:
                        table = table.drop(cols_to_drop)

                    if not any(field in table.column_names for field in event_text_fields):
                        return table

                    normalized_table = table
                    for field_name in event_text_fields:
                        if field_name not in normalized_table.column_names:
                            continue

                        field_index = normalized_table.schema.get_field_index(field_name)
                        field_column = normalized_table.column(field_name)
                        cast_column = pc.cast(field_column, pa.string(), safe=False)
                        normalized_table = normalized_table.set_column(
                            field_index,
                            field_name,
                            cast_column,
                        )

                    return normalized_table

                def aligned_event_table(table, target_schema):
                    table = normalize_event_table(table)
                    table_schema = table.schema
                    aligned_columns = []
                    for field in target_schema:
                        field_index = table_schema.get_field_index(field.name)
                        if field_index != -1:
                            series = table.column(field_index)
                            series = coerce_series_to_field(series, field)
                        else:
                            series = pa.nulls(table.num_rows, type=field.type)
                        aligned_columns.append(series)
                    return pa.Table.from_arrays(aligned_columns, schema=target_schema)

                def ensure_event_hash_id(table_block, table_name, target_schema):
                    # FAST PATH: Skip Pandas conversion entirely if hash_id is fully populated
                    if 'hash_id' in table_block.column_names:
                        hash_col = table_block.column('hash_id')
                        if hash_col.null_count == 0:
                            if table_block.schema.equals(target_schema):
                                return table_block
                            try:
                                return table_block.cast(target_schema)
                            except Exception:
                                pass # Fallback to Pandas if PyArrow cast fails

                    df = table_block.to_pandas()
                    if df.empty:
                        return pa.Table.from_pandas(df, schema=target_schema, preserve_index=False)

                    if 'hash_id' not in df.columns:
                        row_dicts = df.to_dict(orient="records")
                        df['hash_id'] = [row_hash(table_name, row) for row in row_dicts]
                    else:
                        missing_mask = df['hash_id'].isna() | (df['hash_id'].astype(str).str.strip() == "")
                        if missing_mask.any():
                            row_dicts = df.to_dict(orient="records")
                            replacement_hashes = []
                            for row_idx, row_data in enumerate(row_dicts):
                                if missing_mask.iloc[row_idx]:
                                    replacement_hashes.append(row_hash(table_name, row_data))
                                else:
                                    replacement_hashes.append(str(df.iloc[row_idx]['hash_id']))
                            df['hash_id'] = replacement_hashes
                        else:
                            df['hash_id'] = df['hash_id'].astype(str)

                    return pa.Table.from_pandas(df, schema=target_schema, preserve_index=False)

                if not merge_success:
                    source_schemas = []
                    if final_target_path.exists():
                        with open(final_target_path, 'rb') as f_ex:
                            pf_existing = pq.ParquetFile(f_ex)
                            source_schemas.append(normalize_event_schema(pf_existing.schema_arrow))

                    readable_segments = []
                    _salvaged_tables = {}  # seg_path str → pa.Table for files recovered via fallback
                    for seg_path in merge_segments:
                        try:
                            with open(seg_path, 'rb') as f_shard:
                                pf_shard = pq.ParquetFile(f_shard)
                                source_schemas.append(normalize_event_schema(pf_shard.schema_arrow))
                            readable_segments.append(seg_path)
                        except Exception as seg_ex:
                            salvaged = try_read_parquet_salvage(seg_path)
                            if salvaged is not None and salvaged.num_rows > 0:
                                source_schemas.append(normalize_event_schema(salvaged.schema))
                                _salvaged_tables[str(seg_path)] = salvaged
                                readable_segments.append(seg_path)
                                log(f"  [SALVAGE] {seg_path.name}: recovered {salvaged.num_rows:,} rows via fallback read", is_warning=True)
                            else:
                                log(f"  [CORRUPT] {seg_path.name}: unrecoverable ({seg_ex}) — quarantining", is_warning=True)
                                quarantine_file(seg_path)

                    if not source_schemas:
                        continue

                    # Every shard for this group was corrupt or unreadable, so there is
                    # nothing to merge and no reason to pay for hashing the archive.
                    if not readable_segments and final_target_path.exists():
                        log(f"  [SKIP REWRITE] No readable source shards for {final_name}. Keeping existing file.")
                        write_manifest_ids(manifest_path, processed_ids)

                        for seg_path in consumable_segments:
                            archive_ingested_source(seg_path)

                        continue

                    target_schema = resolve_compatible_schema(source_schemas)
                    if "hash_id" not in target_schema.names:
                        target_schema = target_schema.append(pa.field("hash_id", pa.string(), nullable=True))

                    # 1. LOAD EXISTING ROW HASHES AS COMPACT 128-BIT KEYS
                    existing_needs_full_hash_pass = False
                    existing_keys = np.empty(0, dtype=HASH_KEY_DTYPE)

                    if final_target_path.exists():
                        with open(final_target_path, 'rb') as f_ex:
                            pf_ex = pq.ParquetFile(f_ex)
                            if "hash_id" in pf_ex.schema_arrow.names:
                                log(f"  [FAST PATH] Loading existing hashes for {final_target_path.name}...")
                                key_blocks = []
                                for rg in range(pf_ex.metadata.num_row_groups):
                                    hash_chunk = read_parquet_safe(pf_ex, columns=["hash_id"], row_group=rg).column("hash_id")
                                    key_blocks.append(hash_keys_from_column(hash_chunk))
                                if key_blocks:
                                    existing_keys = np.unique(np.concatenate(key_blocks))
                                del key_blocks
                                gc.collect()
                                log(f"  [FAST PATH] Loaded {existing_keys.size:,} existing unique hashes "
                                    f"({existing_keys.nbytes / (1024 * 1024):.0f} MB).")
                            else:
                                existing_needs_full_hash_pass = True

                    # 2. SET UP A STREAMING REWRITE THAT ONLY STARTS IF THERE IS NEW DATA
                    #    New rows go straight to the writer as each row group is read, so
                    #    memory stays flat no matter how many million rows a shard holds.
                    #    The target is opened on the first new row, which keeps the old
                    #    skip-rewrite behaviour for shards that are pure duplicates without
                    #    paying for a second pass over the staging hashes to discover it.
                    hash_index = HashKeyIndex(existing_keys)
                    writer_state = {"writer": None}
                    coalesce_buffer = []
                    coalesce_state = {"rows": 0, "bytes": 0}
                    total_new_rows = 0

                    # Coalesce blocks into ~250k-row row groups on rewrite. Without this,
                    # every rewrite copies old row groups 1:1 and appends new ones, so the
                    # group count only grows and each day's fast-copy gets slower. The byte
                    # ceiling covers wide JSON payloads, where 250k rows is already too much.
                    COALESCE_TARGET_ROWS = 250_000
                    COALESCE_TARGET_BYTES = 256 * 1024 * 1024

                    def flush_coalesce_buffer(force=False):
                        """Write the buffered blocks out as one row group once they earn it."""
                        if not coalesce_buffer or writer_state["writer"] is None:
                            return
                        if (not force
                                and coalesce_state["rows"] < COALESCE_TARGET_ROWS
                                and coalesce_state["bytes"] < COALESCE_TARGET_BYTES):
                            return
                        writer_state["writer"].write_table(pa.concat_tables(coalesce_buffer))
                        coalesce_buffer.clear()
                        coalesce_state["rows"] = 0
                        coalesce_state["bytes"] = 0
                        gc.collect()

                    def buffer_block(table_block):
                        """Queue an aligned block for the writer, flushing when the buffer fills."""
                        coalesce_buffer.append(table_block)
                        coalesce_state["rows"] += table_block.num_rows
                        coalesce_state["bytes"] += table_block.nbytes
                        flush_coalesce_buffer()

                    def open_target_writer():
                        """Open the temp target and seed it with the existing archive rows."""
                        if writer_state["writer"] is not None:
                            return
                        writer_state["writer"] = pq.ParquetWriter(
                            str(temp_target_path),
                            target_schema,
                            compression='zstd',
                            compression_level=ARCHIVE_ZSTD_LEVEL
                        )
                        if not final_target_path.exists():
                            return

                        with open(final_target_path, 'rb') as f_ex:
                            pf_existing = pq.ParquetFile(f_ex)
                            existing_pbar = tqdm(
                                desc=f"  🔵 Fast-copying existing ({final_target_path.name[:20]})",
                                total=pf_existing.metadata.num_row_groups,
                                unit="block",
                                leave=False,
                                file=sys.stderr
                            )
                            for rg in range(pf_existing.metadata.num_row_groups):
                                table_block = read_parquet_safe(pf_existing, row_group=rg)

                                if existing_needs_full_hash_pass:
                                    table_block = aligned_event_table(table_block, target_schema)
                                    table_block = ensure_event_hash_id(table_block, final_name, target_schema)
                                    table_block = select_unseen_event_rows(table_block, hash_index)
                                    if table_block.num_rows == 0:
                                        existing_pbar.update(1)
                                        continue
                                else:
                                    # Fast path: natively cast to target schema (lightning fast, no pandas)
                                    if not table_block.schema.equals(target_schema):
                                        try:
                                            table_block = table_block.cast(target_schema)
                                        except pa.ArrowInvalid:
                                            table_block = aligned_event_table(table_block, target_schema)

                                buffer_block(table_block)
                                existing_pbar.update(1)
                                del table_block
                            existing_pbar.close()

                    if existing_needs_full_hash_pass:
                        # Existing rows carry no hash_id, so they have to be hashed and
                        # written before staging rows can be compared against them.
                        open_target_writer()

                    # 3. STREAM ONLY TRULY NEW ROWS OUT OF THE STAGING SHARDS
                    for seg_path in readable_segments:
                        salvaged_table = _salvaged_tables.pop(str(seg_path), None)
                        if salvaged_table is not None:
                            try:
                                table_block = aligned_event_table(salvaged_table, target_schema)
                                table_block = ensure_event_hash_id(table_block, final_name, target_schema)
                                table_block = select_unseen_event_rows(table_block, hash_index)
                                if table_block.num_rows > 0:
                                    total_new_rows += table_block.num_rows
                                    open_target_writer()
                                    buffer_block(table_block)
                                consumable_segments.append(seg_path)
                                processed_ids.add(source_shard_id(seg_path))
                            except Exception as sv_ex:
                                log(f"  [SALVAGE WRITE FAIL] {seg_path.name}: {sv_ex}", is_warning=True)
                            del salvaged_table
                            gc.collect()
                            continue

                        try:
                            with open(seg_path, 'rb') as f_seg:
                                pf = pq.ParquetFile(f_seg)
                                sub_pbar = tqdm(
                                    desc=f"  🔵 Extracting new ({seg_path.name[:20]})",
                                    total=pf.metadata.num_row_groups,
                                    unit="block",
                                    leave=False,
                                    file=sys.stderr
                                )
                                rg_failures = 0
                                for rg in range(pf.metadata.num_row_groups):
                                    try:
                                        table_block = aligned_event_table(read_parquet_safe(pf, row_group=rg), target_schema)
                                        table_block = ensure_event_hash_id(table_block, final_name, target_schema)
                                        table_block = select_unseen_event_rows(table_block, hash_index)
                                        if table_block.num_rows > 0:
                                            total_new_rows += table_block.num_rows
                                            open_target_writer()
                                            buffer_block(table_block)
                                    except Exception as rg_ex:
                                        rg_failures += 1
                                        log(f"  [SKIP ROW GROUP {rg}] {seg_path.name}: {rg_ex}", is_warning=True)
                                    finally:
                                        sub_pbar.update(1)
                                        if 'table_block' in locals():
                                            del table_block
                                sub_pbar.close()
                                if rg_failures == 0:
                                    consumable_segments.append(seg_path)
                                    processed_ids.add(source_shard_id(seg_path))
                                else:
                                    # SAFETY: a shard with any failed row groups is NOT marked
                                    # consumed — deleting it would lose the unmerged rows. It
                                    # stays in staging for retry; rows already extracted this
                                    # run will dedup harmlessly by hash_id on the next pass.
                                    log(f"  [RETAIN] {seg_path.name}: {rg_failures} row group(s) failed — keeping shard in staging for retry", is_warning=True)
                        except Exception as seg_read_ex:
                            log(f"  [CORRUPT DURING MERGE] {seg_path.name}: {seg_read_ex} — quarantining", is_warning=True)
                            quarantine_file(seg_path)

                    # 4. IF NOTHING NEW WAS FOUND, THE ARCHIVE WAS NEVER TOUCHED
                    #    Every staging row was already present, so the shards are redundant
                    #    and safe to consume without rewriting a multi-gigabyte target.
                    if writer_state["writer"] is None:
                        if final_target_path.exists():
                            log(f"  [SKIP REWRITE] No new unique rows found for {final_name}. Keeping existing file.")
                            write_manifest_ids(manifest_path, processed_ids)

                            for seg_path in consumable_segments:
                                archive_ingested_source(seg_path)

                            continue

                        # Nothing new and no target yet: materialize the empty table anyway so
                        # this group is not rescanned on every future run.
                        open_target_writer()

                    flush_coalesce_buffer(force=True)
                    writer_state["writer"].close()

                    del hash_index
                    del existing_keys
                    gc.collect()

                    if temp_target_path.exists():
                        temp_target_path.replace(final_target_path)
                        write_manifest_ids(manifest_path, processed_ids)
                        merge_success = True
                        if total_new_rows > 0:
                            log(f"  STEP 4 complete: {final_name} (appended {total_new_rows:,} new rows)")
                        else:
                            log(f"  STEP 4 complete: {final_name} (re-wrote file, 0 new rows)")
            except Exception as ex:
                log(f"  Failed to merge event file {final_name}: {ex}", is_error=True)

                temp_target_path.unlink(missing_ok=True)

            if merge_success:
                for seg_path in consumable_segments:
                    archive_ingested_source(seg_path)
        pbar_3.close()


    # STEP 5: Final cleanup and pruning
    log("STEP 5: Running final system cleanup and pruning...")
    protected_dirs = {str(DATA_DIR), str(EVENTS_TARGET_DIR), str(GTFS_TARGET_DIR), str(STAGING_DIR)}
    for sweep_root in scan_roots:
        for root, dirs, files in os.walk(sweep_root, topdown=False):
            for f in files:
                file_path = Path(root) / f
                if ERROR_PATTERN.search(f) or file_path.stat().st_size == 0:
                    file_path.unlink(missing_ok=True)
                elif f.endswith('.dedup.sqlite') or f.endswith('.manifest.json.tmp'):
                    file_path.unlink(missing_ok=True)
                elif f.endswith('.parquet.tmp') and (str(GTFS_TARGET_DIR) in str(file_path) or str(EVENTS_TARGET_DIR) in str(file_path)):
                    file_path.unlink(missing_ok=True)

            if root not in protected_dirs:
                if not os.listdir(root):
                    try:
                        os.rmdir(root)
                    except:
                        pass

    # Retry deferred standardizations now that DB blocks and handles have safely released
    if DEFERRED_RENAMES:
        standardize_filenames_in_scan_roots(scan_roots, is_deferred_pass=True)

    if not RUN_HAD_ERROR:
        deleted_manifests = 0
        for manifest_path in EVENTS_TARGET_DIR.rglob("*.manifest.json"):
            try:
                manifest_path.unlink()
                deleted_manifests += 1
            except Exception as manifest_ex:
                log(f"  Failed to delete manifest {manifest_path.name}: {manifest_ex}", is_warning=True)

        if deleted_manifests > 0:
            log(f"  Manifest cleanup complete: removed {deleted_manifests} ephemeral scratch manifests.")

    elapsed = datetime.now(LOCAL_TZ) - run_started
    log(f"All files successfully processed and merged in {elapsed}!")


if __name__ == "__main__":
    try:
        run_cleanup_and_merge()
    except KeyboardInterrupt:
        sys.stderr.write("\n")
        log("Process stopped early by user request.", is_warning=True)