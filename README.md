# GTFS Archiver

## Overview

Lightweight ingestion engine for real-time and static GTFS transit feeds.  
Captures live Server-Sent Events (SSE) with dynamic route batching, polls enhanced bulk JSON feeds, and executes scheduled contextual REST snapshots.

Built with the MBTA in mind, but designed to be adaptable to any transit agency publishing [GTFS](https://gtfs.org/) data.

Data is archived to JSONL (real-time write-ahead log) and Parquet (compressed analytics format) using queue-based asynchronous writers optimized for continuous ingestion.

Configuration lives directly in the compose files. [`docker-compose.yaml`](https://github.com/mxdrew/gtfsDataArchiver/blob/main/docker-compose.yaml) contains MBTA values as a default.

This project aims to adhere to the [MassDOT Developers License Agreement](https://cdn.mbta.com/sites/default/files/2023-08/mbta-massdot-develop-license-agreement.pdf). Any data collected is owned by the provider (MBTA & MassDOT) and is not claimed by this project.

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
  - [High-Level Data Flow](#high-level-data-flow)
  - [Core Components](#core-components)
- [Data Flow Summary](#data-flow-summary)
- [Data Schema](#data-schema)
- [Directory Structure](#directory-structure)
- [Key Guarantees](#key-guarantees)
- [Failure Handling](#failure-handling)
- [Runtime Model](#runtime-model)
  - [Threads](#threads)
  - [Scheduled Tasks](#scheduled-tasks)
  - [Runtime Logs](#runtime-logs)
- [Configuration](#configuration)
  - [Required For Any Run](#required-for-any-run)
  - [Recommended For Any Run](#recommended-for-any-run)
  - [Required For Static GTFS Mode](#required-for-static-gtfs-mode)
  - [Required Only For API-Backed Modes](#required-only-for-api-backed-modes)
  - [Mode-Specific Variables](#mode-specific-variables)
  - [Multiple Agencies](#multiple-agencies)
    - [Example Compose Files](#example-compose-files)
- [Extra Scripts](#extra-scripts)
  - [`maGTFSArchiver.py`](#magtfsarchiverpy)
  - [`cleanAndMerge.py`](#cleanandmergepy)
- [Installation](#installation)
  - [Docker (Recommended)](#docker-recommended)
  - [Manual](#manual)
- [Operational Notes](#operational-notes)
- [Data Archive](#data-archive)
- [Future Plans](#future-plans)
- [Other Cool Links](#other-cool-links)
- [Questions](#questions)

---

## System Architecture

### High-Level Data Flow

```
    SSE / REST / Enhanced APIs
    │
    ▼
    write_queue
    │
    ▼
    writer_worker
    │
    ├──► AGENCY_NAME_<endpoint>_<MMDDYYYY>.jsonl (Write-Ahead Log)
    │
    ▼
    parquet_compaction_worker
    │
    ├──► AGENCY_NAME_<endpoint>_<MMDDYYYY>.parquet
    ├──► AGENCY_NAME_gtfs_<table>.parquet
    │
    ▼
    cleanup_old_event_parquets
    │
    └──► deletes AGENCY_NAME_<endpoint>_<MMDDYYYY>.parquet older than
         EVENT_RETENTION_DAYS (static gtfs_<table>.parquet files exempt)
```

### Core Components

- **SSE Consumers** → real-time event ingestion
- **Enhanced Pollers** → bulk snapshot ingestion
- **Writer Worker** → append-only write-ahead log (JSONL)
- **Compaction Worker** → JSONL → Parquet + deduplication
- **Retention Sweep** → deletes event-stream Parquet archives older than `EVENT_RETENTION_DAYS`, leaving static GTFS tables untouched
- **Scheduler** → snapshot + GTFS orchestration
- **Queue System** → backpressure-safe buffering

## Data Flow Summary

1. Data is ingested from SSE, REST snapshots, and enhanced feeds  
2. Events are pushed into a bounded in-memory queue  
3. Writer thread persists events into daily JSONL logs  
4. Background worker compacts older logs into Parquet archives  
5. Static GTFS datasets are periodically downloaded and converted  

## Data Schema

All schema definitions are intentionally centralized in [**DATA.md**](https://github.com/mxdrew/gtfsDataArchiver/blob/main/DATA.md) to avoid duplication and drift.

Static GTFS uses stable per-table files named `AGENCY_NAME_gtfs_<table>.parquet`, with `first_logged` and `last_logged` metadata tracking the exact timestamp when each unique row was first and most recently seen. These files accumulate indefinitely and are never pruned.

Event-stream archives (`AGENCY_NAME_<endpoint>_<MMDDYYYY>.parquet`) are dated per day and are automatically deleted once they're older than `EVENT_RETENTION_DAYS` (default: 30 days).

See:
- `data/events/` → [DATA.md](https://github.com/mxdrew/gtfsDataArchiver/blob/main/DATA.md#1-dataevents-active-write-ahead-logs) (Active ingestion schema)
- `data/archive/` → [DATA.md](https://github.com/mxdrew/gtfsDataArchiver/blob/main/DATA.md#2-dataarchive-compacted-event-logs) (Compacted analytical schema)
- `data/archive/gtfs/` → [DATA.md](https://github.com/mxdrew/gtfsDataArchiver/blob/main/DATA.md#3-dataarchivegtfs-static-schedule-data) (Static GTFS schema)

## Directory Structure

```
    data/
  ├── events/    (Active daily JSONL files)
  └── archive/   (Compacted Parquet files + static GTFS outputs)
    └── gtfs/  (Static GTFS Parquet files)
```

## Key Guarantees

- At-least-once delivery semantics  
- Crash-safe ingestion via write-ahead log (JSONL)  
- Deduplication via SHA-256 `hash_id`  
- Automatic recovery of orphaned logs  
- No ordering guarantees across streams or threads  
- Event-stream Parquet archives older than `EVENT_RETENTION_DAYS` (default: 30) are automatically deleted; static GTFS tables under `data/archive/gtfs/` are never subject to this cleanup  

## Failure Handling

- SSE streams automatically reconnect with backoff  
- Partial writes are recovered on startup  
- Parquet archives are merged and deduplicated on re-runs  
- Queue buffers absorb temporary upstream outages  
- Snapshot pulls retry up to 3 times on 5xx errors (30s / 60s backoff) before marking as failed  
- Failed snapshot endpoints trigger a full re-run after `SNAPSHOT_RETRY_MINUTES` (default: 10) rather than waiting until the next hour  
- Stale `.tmp` files left over from interrupted compaction runs are swept automatically on startup  
- After each compaction pass, event-stream Parquet archives older than `EVENT_RETENTION_DAYS` (default: 30) are deleted; static GTFS tables are matched against a stable, non-dated filename and are never eligible for this cleanup  

## Runtime Model

### Threads

- Writer thread (JSONL WAL)
- Compaction worker (Parquet archiver)
- Scheduler thread (snapshot + GTFS jobs)
- N SSE consumer threads
- N enhanced poller threads

### Scheduled Tasks

- Hourly snapshot ingestion; failed endpoints retry after `SNAPSHOT_RETRY_MINUTES` (default: 10) without waiting for the next hour
- GTFS refresh at 03:00 and 15:00 (local timezone); startup download is skipped if files already exist
- Event-stream retention sweep runs once per compaction cycle (roughly every 60 seconds), deleting dated event-stream Parquet archives older than `EVENT_RETENTION_DAYS` (default: 30 days); static GTFS tables are unaffected

### Runtime Logs

- Container stdout and stderr are the live operational log for this project.
- It includes startup messages, stream connect/reconnect notices, warnings, and errors.
- View it with `docker logs -f <container_name>` or Docker Desktop's Logs tab.
- Keep `LOG_LEVEL` at `warning` or `errors` for normal runs. At `info`, output can grow very quickly during high-throughput stream activity and thus will be hard to keep track of.

## Configuration

Configuration is defined directly in the compose file. `docker-compose.yaml` is the sample setup. It uses MBTA defaults for the API endpoints, feed URLs, stream lists, and tuning values, with `API_KEY` left blank.

The application reads only the values in the table below. Missing values are handled safely, and stream-only, snapshot-only, or GTFS-only runs are all supported by the toggles.

Further info on which variables are used in files from `otherScripts/` is in the [Extra Scripts](#extra-scripts) section.

| Variable | Purpose | Default in `docker-compose.yaml` |
|-||-|
| `API_KEY` | API authentication key | YOUR_API_KEY |
| `SYNC_TIMEZONE` | [IANA timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List) used for scheduling and filenames | `America/New_York` |
| `AGENCY_NAME` | Filename prefix for generated outputs | `MBTA` |
| `BASE_URL` | Transit Agency's API base URL | `https://api-v3.mbta.com` |
| `GTFS_URL` | Static GTFS ZIP source static schedule imports | `https://cdn.mbta.com/MBTA_GTFS.zip` |
| `ENABLE_BASE_STREAMS` | Enables live base SSE consumers | `true` |
| `ENABLE_ROUTE_STREAMS` | Enables route-batched SSE consumers | `true` |
| `ENABLE_SNAPSHOT_PULLS` | Enables scheduled snapshot pulls | `true` |
| `ENABLE_ENHANCED_STREAMS` | Enables enhanced bulk pollers | `true` |
| `ENABLE_GTFS_STATIC` | Enables twice-daily static GTFS downloads | `true` |
| `BASE_STREAMS` | Base SSE endpoints | `vehicles,alerts` |
| `ROUTE_STREAMS` | Route-batched SSE endpoints | `predictions,stop_events` |
| `SNAPSHOT_EPS` | Snapshot endpoints | `schedules,services,shapes,trips,lines,routes,route_patterns,facilities,stops,live_facilities` |
| `ENHANCED_POLL_INTERVAL_SECONDS` | Delay between enhanced feed polls | `7` |
| `VEHICLES_ENHANCED_URL` | Enhanced vehicle feed URL - Potentially MBTA Specific | `https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json` |
| `ALERTS_ENHANCED_URL` | Enhanced alerts feed URL - Potentially MBTA Specific | `https://cdn.mbta.com/realtime/Alerts_enhanced.json` |
| `TRIPS_ENHANCED_URL` | Enhanced trip updates feed URL - Potentially MBTA Specific| `https://cdn.mbta.com/realtime/TripUpdates_enhanced.json` |
| `DEFAULT_BATCH_SIZE` | Batch size for route grouping | `25` |
| `PREDICTIONS_BATCH_SIZE` | Batch size for the predictions route stream (falls back to `max(DEFAULT_BATCH_SIZE, 75)` if unset) | `75` |
| `SNAPSHOT_RETRY_MINUTES` | Minutes to wait before retrying a failed snapshot run | `10` |
| `LOG_LEVEL` | Logging verbosity | `warning` |
| `ARCHIVE_ZSTD_LEVEL` | Parquet compression level used for archive files | `10` |
| `EVENT_RETENTION_DAYS` | Days to retain compacted event-stream Parquet archives before automatic deletion. Static GTFS tables are never affected. `0` or a negative value disables cleanup. | `30` |
| `GTFS_DOWNLOAD_USER_AGENT` | User-Agent header sent on static GTFS ZIP downloads. Optional here, but required by `maGTFSArchiver.py` (see [Extra Scripts](#extra-scripts)). | `gtfsDataArchiver/1.0 (+repo URL)` |

### Required For Any Run

| Variable | Description |
|-|-|
| `SYNC_TIMEZONE` | [IANA timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List) string used for scheduling and timestamp alignment. The archiver will not start without a valid value. |

### Recommended For Any Run

| Variable | Description |
|-|-|
| `AGENCY_NAME` | Prefix used in generated file names; defaults to `agency` if omitted |
| `ARCHIVE_ZSTD_LEVEL` | Compression level for Parquet archives (default: `10`) |
| `LOG_LEVEL` | Logging verbosity (`info`, `warning`, `errors`; default: `info`) |

### Required For Static GTFS Mode

| Variable | Description |
|-|-|
| `GTFS_URL` | Static GTFS ZIP source |

### Required Only For API-Backed Modes

| Variable | Description |
|-|-|
| `API_KEY` | API authentication key |
| `BASE_URL` | Transit API base URL |

### Mode-Specific Variables

| Variable | Description |
|-|-|
| `BASE_STREAMS` | SSE endpoints |
| `ROUTE_STREAMS` | Route-batched SSE endpoints |
| `SNAPSHOT_EPS` | Snapshot endpoints |
| `ENABLE_BASE_STREAMS` | Enables live base SSE consumers |
| `ENABLE_ROUTE_STREAMS` | Enables route-batched SSE consumers |
| `ENABLE_SNAPSHOT_PULLS` | Enables scheduled snapshot pulls |
| `ENABLE_ENHANCED_STREAMS` | Enables enhanced bulk pollers |
| `ENABLE_GTFS_STATIC` | Enables twice-daily static GTFS downloads |
| `DEFAULT_BATCH_SIZE` | Batch size for route grouping |
| `PREDICTIONS_BATCH_SIZE` | Batch size for the predictions route stream |
| `SNAPSHOT_RETRY_MINUTES` | Minutes before retrying a failed snapshot run |
| `ENHANCED_POLL_INTERVAL_SECONDS` | Poll interval for enhanced bulk feeds |
| `EVENT_RETENTION_DAYS` | Days to retain event-stream Parquet archives before automatic deletion (static GTFS tables are exempt) |
| `GTFS_DOWNLOAD_USER_AGENT` | User-Agent header for static GTFS ZIP downloads |

Static-only runs can disable every API-backed mode and leave only GTFS enabled:

```yaml
ENABLE_BASE_STREAMS=false
ENABLE_ROUTE_STREAMS=false
ENABLE_SNAPSHOT_PULLS=false
ENABLE_ENHANCED_STREAMS=false
ENABLE_GTFS_STATIC=true
```

### Multiple Agencies

For static GTFS only, it is fine to keep multiple agencies in one compose file as separate service blocks. For live streams, separate compose files are the recommended production setup.

Separate containers in the same Compose stack are still independent; splitting compose files is about operational isolation, not a hard technical requirement.

- Static-only: one compose file with multiple service blocks is usually fine and easy to manage.
- Live streams: separate compose files are recommended so restarts, logs, and resource limits stay isolated per agency.
- In either case, give each service or container a unique `container_name` and, if needed, a unique Compose project name with `-p`.
- Point each agency at its own upstream URLs and GTFS ZIP source.
- Keep the same `./data` mount if you want all output in one place, because the archive naming already prefixes files by agency.

Why this is the recommendation:

- Live streams are easier to restart, debug, and tune when each agency is isolated in its own compose file.
- Shared live stacks can entangle logs, restarts, and backpressure across agencies.
- Static-only downloads are much less sensitive, so multiple agencies in one file are usually fine there.

#### Example Compose Files

Example for separate compose files:

```bash
cp docker-compose.yaml docker-compose-lrta.yaml
docker compose -f docker-compose.yaml -p mbta up -d --build
docker compose -f docker-compose-lrta.yaml -p lrta up -d --build
```

In the copied compose file, update the values that make the agency unique: `container_name`, `AGENCY_NAME`, `BASE_URL`, `GTFS_URL`, `BASE_STREAMS`, `ROUTE_STREAMS`, `SNAPSHOT_EPS`, `VEHICLES_ENHANCED_URL`, `ALERTS_ENHANCED_URL`, `TRIPS_ENHANCED_URL`, and the `ENABLE_*` flags.

Example for static GTFS only in one compose file:

```yaml
services:
  mbta-archiver:
    build: .
    container_name: mbta-data-archiver
    restart: unless-stopped
    environment:
      - SYNC_TIMEZONE=America/New_York
      - AGENCY_NAME=MBTA
      - GTFS_URL=https://cdn.mbta.com/MBTA_GTFS.zip
      - ENABLE_BASE_STREAMS=false
      - ENABLE_ROUTE_STREAMS=false
      - ENABLE_SNAPSHOT_PULLS=false
      - ENABLE_ENHANCED_STREAMS=false
      - ENABLE_GTFS_STATIC=true
    volumes:
      - ./data:/app/data
  bat-archiver:
    build: .
    container_name: bat-data-archiver
    restart: unless-stopped
    environment:
      - SYNC_TIMEZONE=America/New_York
      - AGENCY_NAME=BAT
      - GTFS_URL=https://data.trilliumtransit.com/gtfs/brockton-ma-us/brockton-ma-us.zip
      - ENABLE_BASE_STREAMS=false
      - ENABLE_ROUTE_STREAMS=false
      - ENABLE_SNAPSHOT_PULLS=false
      - ENABLE_ENHANCED_STREAMS=false
      - ENABLE_GTFS_STATIC=true
    volumes:
      - ./data:/app/data
```
Which would be built and launched with:
```bash
docker compose -f docker-compose.yaml up -d --build
```

## Extra Scripts

The `otherScripts/` folder contains two companion scripts to the main archiver: 
- [`maGTFSArchiver.py`](#maGTFSArchiverpy), a multi-agency ingestion runner.
- [`cleanAndMerge.py`](#cleanAndMergepy), a post-processing pipeline.

Both scripts resolve their `.env` the same way: a `.env` sitting alongside the script (`otherScripts/.env`) is used if present; otherwise the search walks up parent directories and falls back to the repo-root `.env`. In practice this means a single root `.env` configures everything, and an `otherScripts/.env` can optionally override it for just the helper scripts.

### `maGTFSArchiver.py`

A multi-agency ingestion runner, [`maGTFSArchiver.py`](https://github.com/mxdrew/gtfsDataArchiver/blob/main/otherScripts/maGTFSArchiver.py) handles static GTFS pulls across 32+ agencies — including the MBTA, all 15 Massachusetts Regional Transit Authorities, private bus carriers (Coach Company, DATTCO, Peter Pan, Plymouth & Brockton, Yankee Line), ferry services (Bay State Cruise, Block Island Ferry, Cuttyhunk, Freedom Cruise Line, Hy-Line Cruises, Patriot Party Boats, Seastreak, Vineyard Fast Ferry, Steamship Authority), Massport/Logan Express, Lexpress, and Amtrak — with optional MBTA live ingestion.

Environment variables read (from `otherScripts/.env`):

| Variable | Description |
|-|-|
| `GTFS_DOWNLOAD_USER_AGENT` | **Required.** User-Agent header sent on every GTFS download; the script refuses to start without one. |
| `SYNC_TIMEZONE` | Required. IANA timezone for scheduling and filenames. |
| `MBTA_API_KEY` | Optional. Enables the MBTA live ingestion portion. |
| `LOG_LEVEL` | Optional. Logging verbosity (default: `info`). |
| `ARCHIVE_ZSTD_LEVEL` | Optional. Parquet compression level (default: `10`). |
| `EVENT_RETENTION_DAYS` | Optional. Event-stream Parquet retention in days (default: `30`; static GTFS tables exempt). |
| `DEFAULT_BATCH_SIZE` / `PREDICTIONS_BATCH_SIZE` | Optional. Route batching sizes (defaults: `25` / `max(DEFAULT_BATCH_SIZE, 75)`). |
| `SNAPSHOT_RETRY_MINUTES` | Optional. Minutes before retrying a failed snapshot run (default: `10`). |

Output is written to `./data` relative to the working directory the script is launched from.

_**DISCLAIMER**: <u>Live real-time ingestion is only recommended for the MBTA.</u> Static GTFS downloads are published for public consumption, but most agencies' real-time endpoints are not — they exist to feed the agency's own live maps and rider apps, are undocumented and unsupported for third-party use, and may change or vanish without notice. Continuously polling them can put real load on systems that actual riders depend on, and may get your IP blocked. The MBTA is the exception: its [V3 API](https://www.mbta.com/developers/v3-api) and enhanced feeds are publicly documented and intended for developer use. If you choose to ingest live data from any other agency anyway, you do so at your own discretion and risk — review the relevant code carefully, consider whether you _<u>realllllyyyyy</u>_ want to run it, and test in an isolated environment first._

### `cleanAndMerge.py`

A post-processing pipeline that consolidates raw archiver output into clean, unified historical datasets. [`cleanAndMerge.py`](https://github.com/mxdrew/gtfsDataArchiver/blob/main/otherScripts/cleanAndMerge.py) standardizes shard filenames in place, converts any remaining JSONL write-ahead logs into ZSTD-compressed Parquet, then classifies and merges daily shards — static GTFS tables are deduplicated by SHA-256 `hash_id` while preserving `first_logged`/`last_logged` observation windows, and event streams are compacted into per-type archives. Corrupt or partially-written Parquet files are automatically repaired where possible (tolerant row-group reads, footer reconstruction for truncated files); unrecoverable files are quarantined rather than silently dropped. A strict overwrite guard ensures existing archives are never clobbered — merges write to temp files and atomically replace their targets.

Environment variables read (from the repo-root `.env`):

| Variable | Description |
|-|-|
| `DATA_DIR_WSL` | Final archive directory. Accepts Windows (`Z:\gtfs-archiver-data`) or WSL (`/mnt/z/gtfs-archiver-data`) style paths; translated automatically for the current platform. Falls back to the current directory if unset. |
| `STAGING_DIR_WSL` | Staging directory holding incoming shards; same path translation as above. Falls back to `DATA_DIR_WSL` if unset. |
| `SYNC_TIMEZONE` | Optional. IANA timezone for log timestamps (default: UTC). |
| `ARCHIVE_ZSTD_LEVEL` | Optional. Parquet compression level (default: `12`). |
| `JSONL_CHUNK_SIZE` | Optional. Rows per chunk during JSONL → Parquet conversion (default: `50000`). |
| `LOG_LEVEL` | Optional. Logging verbosity (default: `info`). |

## Installation

### Docker (Recommended)

```bash
git clone https://github.com/mxdrew/gtfsDataArchiver.git
cd gtfsDataArchiver
docker compose up -d --build
```

If you add another agency later, use the copy pattern shown above in the Multiple Agencies section, then update the agency-specific values there.

### Manual

```bash
git clone https://github.com/mxdrew/gtfsDataArchiver.git
cd gtfsDataArchiver
pip install -r requirements.txt
python gtfsArchiver.py
```

For manual runs, use the included `.env` sample as the starting point: [`.env`](https://github.com/mxdrew/gtfsDataArchiver/blob/main/.env). Update the values that make the agency unique, especially `AGENCY_NAME`, `SYNC_TIMEZONE`, `API_KEY`, `BASE_URL`, `GTFS_URL`, `BASE_STREAMS`, `ROUTE_STREAMS`, `SNAPSHOT_EPS`, the `VEHICLES_ENHANCED_URL`/`ALERTS_ENHANCED_URL`/`TRIPS_ENHANCED_URL` entries, `GTFS_DOWNLOAD_USER_AGENT`, and the `ENABLE_*` flags. I did not try multiple agencies with this, but I'd imagine in order to do so you'd need to clone `gtfsArchiver.py` and `.env`, rename them, update the new `.env` file, and then launch the cloned script.

## Operational Notes

This system prioritizes durability and recoverability over strict consistency or ordering guarantees. It is designed for continuous ingestion rather than transactional processing.

- Designed for continuous, long-running ingestion workloads
- Uses append-only disk writes for durability
- Queue-based buffering prevents ingestion blocking
- Optimized for high-frequency transit data streams
- JSONL provides durable write-ahead logging and recovery
- Parquet enables efficient analytics and compression
- Separation of WAL and archive layers improves reliability and performance

## Data Archive

The data archiver began continuous operation on May 26, 2026 - focused on MBTA data. Any archived datasets available from this system begin as of that date. The archived data generated by this system is not stored in this repository.

More info, such as schema definitions, file formats, period of time captured, and downtime, will be available [here](https://github.com/mxdrew/gtfs-data-logger/blob/main/DATA.md).

## Future Plans

- Run analytics on the collected data.
- Build an MBTA system map or line map that shows train positions on a given line in real time.
- Explore the MBTA's [MBTA Historical Performance Data](https://www.mbta.com/developers/historical-performance-data).
- Explore other agencies through the [Mobility Database](https://mobilitydatabase.org/) or [Transitland](https://www.transit.land/).
- Explore data from [opentransportdata.swiss](https://opentransportdata.swiss/).

## Other Cool Links

Other useful places to look are:

- [Official GTFS Website](https://gtfs.org/): Official specification and documentation for the General Transit Feed Specification (GTFS), including static schedules and realtime extensions.

- [The Mobility Database](https://mobilitydatabase.org/): An open data catalog with over 6000 GTFS, GTFS Realtime, and GBFS feeds in over 99 countries.

- [Transitland](https://www.transit.land/): A transit data platform for finding and working with agency feeds.

- [MBTA API developer docs](https://www.mbta.com/developers/v3-api): MBTA's public API overview and entry point for the v3 endpoints.

- [MBTA API Swagger docs](https://api-v3.mbta.com/docs/swagger/index.html): Interactive reference for the MBTA v3 API endpoints and response shapes.

- [MBTA GitHub](https://github.com/mbta): Source repositories and related MBTA projects.

- [MBTA Historical Performance Data](https://www.mbta.com/developers/historical-performance-data): Historical performance data and related developer resources.

- [Public transportation in Massachusetts](https://www.mass.gov/info-details/public-transportation-in-massachusetts#regional-transit-authorities): Overview of the MBTA and the 15 Regional Transit Authorities, including a map of service areas, member towns/cities/areas, and contact information.

- [opentransportdata.swiss](https://opentransportdata.swiss/): The open data platform for customer information on Swiss public transport, operated on behalf of the Swiss Federal Office of Transport (FOT). Contains everything from Static and Realtime GTFS data to road traffic counters, traffic light data, and train composititon data.

## Questions

If you have interest in accessing the archived data, or if you have any questions, comments, or concerns, email [github@mxdrew.com](mailto:github@mxdrew.com).