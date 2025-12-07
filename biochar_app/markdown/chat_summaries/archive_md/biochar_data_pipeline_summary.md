# Summary of Biochar Data Download Project

## Project Purpose
The Biochar Water Conservation website is a data‑visualization platform for the Biochar Water Conservation study.  
It uses Python (FastAPI) to expose API endpoints and Jinja2 templates to display plots and dashboards of soil moisture 
(volumetric water content), electrical conductivity, soil temperature, soil water content and other variables.  

Data originate from Campbell Scientific dataloggers (CR series) installed in four crop strips; each strip has  
three loggers (located at the top (north) side, middle, and bottom of the field) connected via a PakBus network. A base datalogger (CR800) with IPv6 address 
2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd acts as a gateway to the leaves. The website reads processed Parquet files to 
serve fast responses and provides API endpoints for downloading raw or summary data.

## Existing Data Pipeline
* Raw .dat files are currently collected with PC400 scripts; each leaf’s Table 1 record consists of a 1990‑epoch 
  timestamp followed by ten floats. These rows are appended to .dat and .tsv files.
* A Python ETL process (biochar_app/scripts/etl.py) reads the .dat files, merges them, and writes year×granularity Parquet 
  slices into data‑processed/parquet/…. The FastAPI app loads these Parquet slices on demand and caches them.

## Transition from PowerShell to Python for Live Fetch
* Original PowerShell scripts requested and parsed Table 1 data via PakBus 0x09 request/0x89 response. They built 
  BD‑framed packets, scanned PC400 logs to guess float‑block offsets and decode floats.
* A Python port (biochar_app/pakbus/scripts/fetch_table1_live.py) was drafted to perform live downloads. It reads a 
  template CSV containing sample 0x09 payloads, identifies the operation and transaction indices in the payload, 
  and constructs a new 0x09 request with a user‑specified record count and start record. The script then connects 
  via socket to the base logger’s IPv6 host, sends the BD‑framed packet, waits for the 0x89 response, extracts the 
  epoch timestamp and ten floats, and writes the results to CSV.
* Command‑line arguments --host, --port, --template, --count, --start-rec, --output allow customization. Defaults 
  should be taken from config.py (see below) so the script can run without specifying every argument.

## Config Integration
* biochar_app/scripts/config.py defines default Pakbus settings (host, port, base_id, logger_ids), default table 
  (Table1), default timezone (America/Denver), default hours and lag for summarisation, and path constants 
  (data‑raw, data‑processed, parquet). It also holds lists of variable labels, unit conversions and logger names.
* To make fetch_table1_live.py aware of these defaults, modify its main() so that it imports and uses:
  * PAKBUS.host and PAKBUS.port for the IPv6 host and port.
  * DEFAULT_TABLE for the table name (usually Table1).
  * BASE_DIR / "pakbus/data/pc400_table1_templates.csv" for the template path.
  * A sensible default record count (e.g. 96 rows → 24 hours of 15‑minute data) and a default start_rec of 0xFFFF 
    to request the newest records.
  * An output path inside DATA_RAW_DIR or DATA_PROCESSED_DIR.

## Directory Restructuring
* The pakbus directory originally contained a mix of scripts, logs, raw BD files and test outputs. It has been 
  reorganised: core holds stable client and link utilities; data (created manually) holds raw BD files (bdFiles), 
  decoded TSVs (decoded), and tables to keep templates and catalog. scripts contains stand‑alone scripts 
  (e.g. fetch_table1_live.py), and utils holds helper modules (e.g. extract_bd_frames.py).
* A helper script (move_outputs_to_pakbus_data.py) was used to move the bdFiles folder and biochar_app.log under 
  a pakbus_data directory; the user ultimately did these moves manually. This restructure keeps the code tree clean 
  and makes it easier to manage raw and processed outputs.

## Remaining Tasks
* Incremental download: The current Python fetch script retrieves one record or a fixed count and writes them to 
  CSV. For production use it must determine the last record already stored in the Parquet or CSV and request only 
  new records (start_rec should be the next index). After downloading, new rows should be appended to the 
  appropriate Parquet file (using pandas to read, concatenate and write Parquet).
* Leaf addressing: The current script connects to the base logger but does not specify which leaf (PakBus address) 
  to query. In PakBus, the 0x09 request contains Dst, Src, Tran and Op fields. To query a specific leaf, set the 
  Dst field to the leaf’s PakBus ID. The config’s logger_ids lists valid leaf IDs. Future improvements could iterate 
  through all logger_ids and fetch data sequentially.
* Deployment: For integration with the website, schedule the fetch script via cron or call it from the FastAPI app. 
  It should handle network timeouts gracefully and log successes and failures.
