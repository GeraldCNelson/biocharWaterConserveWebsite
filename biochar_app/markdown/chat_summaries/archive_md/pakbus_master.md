# PakBus Data Pipeline — Master Notes

**Status (2025-10-20):**  
Live pull of Table 1 via multi-hop PakBus is working robustly using BD-framed “HELLO” + replay of captured TX frames, with float-decoding knobs tuned per leaf/slice. This doc collects TL;DR, recipes, scripts, CRC notes, and troubleshooting into one place.

---

## 📋 TL;DR (What Actually Works)

1. **HELLO handshake**  
   Send once over TCP on connect:
   ```
   bd ef ff 10 01 0f ff 00 01 0e 00 dd f0 bd
   ```
2. **Trigger the logger**  
   Re-send a single small BD frame that includes your route nibble (`station_id * 0x10`) plus the exact slice control words captured from a PCAP (so CRC stays valid).  
3. **Detect “long reply”**  
   Watch for frames beginning with:
   ```
   bd af fd 00 01 1f fd 20 …
   ```
4. **Decode floats**  
   Scan after that signature at a leaf-specific offset (`--seek-after-sig`) stepping by 1/2/4 bytes (`--scan-step`) with the correct endianness (`>` big or `<` little) to extract contiguous float32 runs.  
5. **Label & round**  
   Post-process with `label_and_round_latest.py` to map columns to `BattV_Min`, `VWC_n_Avg`, `EC_n_Avg`, `T_n_Avg`, and format timestamps as `YYYY-MM-DD HH:MM:SS`.

---

## 🛠 Key Scripts & Workflows

### 1. `pcap_longreply_trigger_finder.py`  
**Find** candidate TX hex strings from a short PCAP.  
```bash
python biochar_app/pakbus/scripts/pcap_longreply_trigger_finder.py   --pcap path/to/Table1Leaf3.pcapng   --sig bdaffd00011ffd20   --context 20
```

### 2. `probe_and_pull.py`  
**One-off pull** with rolling BD framer + safe recv + float decode. Good for experimenting on offsets.  
```bash
python biochar_app/pakbus/scripts/probe_and_pull.py   --addr <ipv6> --port 6785 --hello   --tx-hex <POSSIBLE_TRIGGER> --reply-sig bdaffd00011ffd20   --seek-after-sig 12 --scan-step 4 --min-floats 12 --endian ">"   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --out recent_leaf3.csv --rx-frame-log rx_frames.csv
```

### 3. `loop_27b_tx_pull.py`  
**Batch** over a dictionary of known TX variants per leaf/slice and **merge** results.  
```bash
python biochar_app/pakbus/scripts/loop_27b_tx_pull.py   --addr <ipv6> --port 6785 --hello   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --reply-sig bdaffd00011ffd20   --seek-after-sig 12 --scan-step 4 --min-floats 12 --endian ">"   --out-dir pulls_leaf3 --only s3b_9e50 s3b_9e60 s3b_9e70
```

### 4. `snoop_after_sig.py`  
**Auto-tune** offsets by examining the first long frame in a frames-log CSV.  
```bash
python biochar_app/pakbus/scripts/snoop_after_sig.py   --frames-log pulls_leaf3/tx_xxx_frames.csv
```

### 5. `label_and_round_latest.py`  
**Clean up** a “recent_data” CSV: rename the first 10 value cols to  
`["BattV_Min","VWC_1_Avg","EC_1_Avg","T_1_Avg",…]`, round, and reformat the timestamp.

### 6. **New**: `fetch_table1_live.py`  
A **PC400-style simple-framing** helper for one-off reads (no Pandas).  
- Sends BD-framed hello + read request  
- Dumps `raw` frames, inner payload, logs, etc.  
- Usage example:
  ```bash
  python -m biochar_app.pakbus.scripts.fetch_table1_live     --addr 2605:59c0:30f3:2500:2d0:2cff:fe02:1ddd     --port 6785 --hello     --pre-hex "90 01 0f fd 73 d3"     --leaf 3 --table-id 0x0001     --out one_row.bin     --debug --dump-frames
  ```

### 7. **New**: `cr206_download_latest.py`  
A Click-based CLI for incremental or full file-upload download via PyCampbellCR1000.  
- Incremental by default (`--full` falls back if not implemented)  
- Emits CSV, or raw `.bin` via file-upload  
- Make sure you’re importing **your** extended `biochar_app.pakbus.pakbus.PakBus` before falling back to the `pycampellcr1000` version.

---

## ⚙️ CRC Implementation

The low-level framing uses CRC-16/CCITT-X25, **not** Modbus. You should replace:

```python
def crc16_modbus(data: bytes) -> int:
    # CRC-16/Modbus poly 0xA001
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return crc
```

with a CCITT-X25 variant (poly 0x1021, init 0xFFFF, final xor 0xFFFF), for example:

```python
def crc16_x25(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= (b << 8)
        for _ in range(8):
            if crc & 0x8000:
                crc = ((crc << 1) & 0xFFFF) ^ 0x1021
            else:
                crc = (crc << 1) & 0xFFFF
    return crc ^ 0xFFFF
```

Use `crc16_x25(inner)` when building or verifying BD frames.

---

## 🗺 Station → Route Mapping

From `config.py`:
```python
# station_id → leaf node name → route nibble
1: CR800    → 0x10
2: S1T      → 0x20
3: S1M      → 0x30
4: S1B      → 0x40
5: S2T      → 0x50
6: S2M      → 0x60
7: S2B      → 0x70
8: S3T      → 0x80
9: S3M      → 0x90
10: S3B     → 0xA0
11: S4T     → 0xB0
12: S4M     → 0xC0
13: S4B     → 0xD0
```
Compute `route_byte = station_id * 0x10` for your BD trigger frame.

---

## 🚑 Troubleshooting Checklist

- **No “long” reply frames?**  
  - Verify you replayed exactly the TX hex from a PCAP (including CRC & nibble).  
  - Confirm route nibble matches your leaf.  
- **CRC mismatches?**  
  - Double-check you’re using X25 CRC (not Modbus).  
- **No floats decoded?**  
  - Run `snoop_after_sig.py` to find the correct `--seek-after-sig` and `--scan-step`.  
- **Crazy values (1e7, NaN)?**  
  - Try ±1…±8 byte shifts around your offset.  
- **Incremental stuck at zero rows?**  
  - Your full-download fallback may not be implemented in your PyCampbellCR1000 version—keep trying incremental or synthesize a File-Upload transaction.  
- **IPv6 connect errors?**  
  - Pass raw IPv6 literal to AF_INET6 socket connect; no brackets needed in code calls.

---

> 🔎 **Next Steps for a New Leaf**  
> 1. Capture ~10 s of BD traffic polling Table 1 for that leaf only.  
> 2. Run `pcap_longreply_trigger_finder.py` → get 3–4 `POSSIBLE_TRIGGER` lines.  
> 3. Paste them under a new block in `loop_27b_tx_pull.py` (e.g. `s4m_97xy`).  
> 4. Use `snoop_after_sig.py` on the variant’s frame log to lock in `seek`/`step`/`endian`.  
> 5. Merge results and run `label_and_round_latest.py` for friendly CSV.

---

*End of master summary.*
