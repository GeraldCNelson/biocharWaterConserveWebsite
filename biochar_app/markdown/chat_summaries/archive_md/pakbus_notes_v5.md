# PakBus Live Pull Notes (v5 – refreshed 2025-10-07T20:11:07.070651Z)

These notes consolidate what’s working now for **live “pull” of Table 1** values from Campbell Scientific nodes on the multi‑hop PakBus path. They reflect the latest scripts, signatures, and tuning we validated in Oct 2025 and remove earlier conjectures.

---

## TL;DR (what actually works)

- **Handshake**: send `HELLO_HEX = bdefff10010fff00010e00ddf0bd` once per TCP connect.
- **Trigger the logger** with a small **routered TX** that looks like:
  - `bd a0 01 6f ff d1 00 {ROUTE} ff d0 9x xx ... bd`  
    where **`{ROUTE} = station_id * 0x10`** (hex). Examples:
    - S1T (id=2) → `0x20` → `...10020ffd...`
    - S1M (id=3) → `0x30` → `...10030ffd...`
    - S2B (id=7) → `0x70` → `...10070ffd...`
    - S3B (id=10) → `0xA0` → `...100a0ffd...`
    - S4M (id=12) → `0xC0` → `...100c0ffd...`
- **Look for long replies** beginning with signature:  
  `bdaffd00011ffd20` (logger → PC)
- **Decode floats** by scanning **after** the signature (seek offset varies per leaf / slice):
  - Typical settings (examples that produced plausible values):
    - **S1T / Table 1** (routes at `0x20`):  
      `--seek-after-sig 12 --scan-step 4 --endian ">"` for slices `92f0`, `9300`, `9310`
    - **S2B / Table 1** (route `0x70`):  
      `--seek-after-sig 13 --scan-step 1 --endian ">"` worked for slice `9ca0` (gives plausible BattV and soil values).
      `--seek-after-sig 118 --scan-step 1 --endian ">"` worked for slice `9c70` (later block in same frame).
- Use **`snoop_after_sig.py`** on the captured frame log (`*_frames.csv`) to choose a good **seek/step/endian** before final pulls.
- For friendly CSVs, run **`label_and_round_latest.py`** to map 2025 columns to:
  ```
  VALUE_COLS_2024_PLUS = ["BattV_Min",
    "VWC_1_Avg","EC_1_Avg","T_1_Avg",
    "VWC_2_Avg","EC_2_Avg","T_2_Avg",
    "VWC_3_Avg","EC_3_Avg","T_3_Avg"]
  ```
  Timestamps are now formatted as `YYYY-MM-DD HH:MM:SS` (fixed rounding/format).

---

## Key scripts and their current roles

### 1) `pcap_longreply_trigger_finder.py`
- Input: a short `.pcapng` where **only Table 1** for one leaf was being polled.
- Output: prints **[POSSIBLE TRIGGER]** TX frames immediately preceding **long** replies (`bdaffd00011ffd20…`).  
- These TX hex strings already contain the correct **BD CRC** and are safe to replay live.

**Usage**
```bash
PYTHONPATH=. python biochar_app/pakbus/scripts/pcap_longreply_trigger_finder.py   --pcap biochar_app/pakbus/pakbus_data/bdFiles/Table1S2B_new.pcapng   --sig bdaffd00011ffd20   --context 20
```
Copy the printed `POSSIBLE TRIGGER` hex into the pull scripts below.

---

### 2) `probe_and_pull.py` (unified framer + safe recv)
- We replaced the old `parse_bd_frames` dependency with a **rolling BD frame buffer**:
  - `recv_some()` never returns `None`, tolerates timeouts, and supports frames spanning TCP reads.
  - `split_frames()` and the buffered extractor agree on BD…BD framing.
- Flow:
  1. Optional hello
  2. Send one TX
  3. Receive a few batches (probe summary)
  4. Re-send TX periodically while **scanning for long frames** and **decoding float runs** after the signature.
- CSV is written with a header plus optional raw frame hex.

**Typical single‑TX run**
```bash
PYTHONPATH=. python biochar_app/pakbus/scripts/probe_and_pull.py   --addr <ipv6> --port 6785 --hello   --tx-hex <POSSIBLE_TRIGGER_HEX>   --reply-sig bdaffd00011ffd20   --seek-after-sig 12 --scan-step 4 --min-floats 12 --endian ">"   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --out recent_data.csv   --rx-hex-log rx_dump.hex   --rx-frame-log rx_frames_log.csv
```

---

### 3) `loop_27b_tx_pull.py` (batch known TXs)
- Maintains a dictionary of **TX variants** per leaf/slice (S1T, S2B examples below).
- Runs each variant, logs frames, decodes, and **merges** latest rows to a single CSV for convenience.
- Accepts `--only` to run a subset.

**Examples we validated**

**S1T / Table 1 (route 0x20)** – variants discovered from `Table1S1T_new.pcapng`:
```
s1t_92e0: bda0016ffd10020ffd092e00000500022c790000000100003156bd   # short stats (no floats)
s1t_92f0: bda0016ffd10020ffd092f00000600022c79000110fb000111360000d025bd
s1t_9300: bda0016ffd10020ffd093000000600022c79000111000001113600001dc9bd
s1t_9310: bda0016ffd10020ffd093100000600022c79000111050001113600005b7dbd
```
**Good decode knobs**
- `92f0`: `--seek-after-sig 12 --scan-step 4 --endian ">"` → plausible soil triplet series.
- `9300`, `9310`: same decode knobs also produced stable floats.

**S2B / Table 1 (route 0x70)** – variants from `Table1S2B_new.pcapng`:
```
s2b_9c70: bda0016ffd10070ffd09c700000600022c79000110bcdc000110f700008a03bd
s2b_9ca0: bda0016ffd10070ffd09ca00000600022c79000110cb000110f70000d43abd
s2b_9cb0: bda0016ffd10070ffd09cb00000600022c79000110d0000110f70000ad22bd
s2b_9cf0: bda0016ffd10070ffd09cf00000600022c79000110e4000110f7000088fcbd
```
**Good decode knobs (examples)**
- `9ca0`: `--seek-after-sig 13 --scan-step 1 --endian ">"` → yields plausible `BattV_Min` and VWC/EC/T triplets.
- `9c70`: `--seek-after-sig 118 --scan-step 1 --endian ">"` → later block in same frame.
- If results look noisy/huge, use `snoop_after_sig.py` to re‑tune `seek/step/endian` for that slice.

**Run only selected variants**
```bash
PYTHONPATH=. python biochar_app/pakbus/scripts/loop_27b_tx_pull.py   --addr <ipv6> --port 6785 --hello   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --reply-sig bdaffd00011ffd20   --seek-after-sig 13 --scan-step 1 --min-floats 20 --endian ">"   --out-dir pulls_27b_S2B   --only s2b_9ca0 s2b_9c70
```

---

### 4) `snoop_after_sig.py` (pick offsets quickly)
- Input: a `*_frames.csv` produced by a pull.
- It finds the **first** long frame, prints the 128 B after the signature, then scores float/int16 runs for several **offset/step/endian** combos.
- Use its “TOP float32 candidates” to set `--seek-after-sig`, `--scan-step`, and `--endian` for your next run.

**Example output snippet**
```
[TOP float32 candidates]
  float32 endian=> step=1 count= 58 offset=1
  float32 endian=< step=1 count= 57 offset=2
  ...
[HINT] Try --seek-after-sig 12, match endian/step.
```

---

### 5) `label_and_round_latest.py` (friendly CSV for 2025)
- Maps the first 10 value columns to `VALUE_COLS_2024_PLUS` (BattV + three VWC/EC/T triplets).
- Rounds values (e.g., VWC/EC to 5–6 sig figs, Temps to 2 dp—tweak as you like).
- **Fix applied:** timestamp is now formatted as `YYYY-MM-DD HH:MM:SS` (no trailing `Z`, correct rounding).

---

## Station → route mapping (Table 1 path)

From `config.py`:
```
1: CR800, 2: S1T, 3: S1M, 4: S1B,
5: S2T, 6: S2M, 7: S2B,
8: S3T, 9: S3M, 10: S3B,
11: S4T, 12: S4M, 13: S4B
```
Compute **route nibble** as `route = station_id * 0x10` (hex).  
Examples: S1T=0x20, S1M=0x30, S2B=0x70, S3B=0xA0, S4M=0xC0.

> 🔎 Only the route changes across leaves; the rest of the payload (the “slice” control words like `92f0`, `9ca0`, `9300` …) is table‑program specific and we preserve the exact bytes as captured so the CRC remains valid.

---

## Adding a new leaf (e.g., S4M) — two paths

### Path A (fastest today): **one short capture** + replay
1. Capture ~10–15 s of BD traffic while polling **only Table 1** for the new leaf.
2. Run `pcap_longreply_trigger_finder.py` to enumerate **POSSIBLE TRIGGER** TXs.
3. Paste those TXs into `loop_27b_tx_pull.py` under a new block (e.g., `s4m_...`) and run with `--only` to test.
4. Use `snoop_after_sig.py` once to choose `--seek-after-sig / --scan-step / --endian` that produce reasonable ranges.

### Path B (more scalable): **compute CRC and synthesize TXs**
- Implement a tiny **BD frame builder** that recomputes the CRC so we can:
  - Take a working S1M/S1T slice template,
  - swap route to `0xC0` for S4M (or any leaf),
  - **recompute CRC**, then probe live without a pcap.
- Once added, `loop_27b_tx_pull.py` can try a grid of slice codes automatically (e.g., `92f0/9300/9310` family) and lock in those that yield long replies.

> We haven’t checked in a CRC builder yet. If you want this, we’ll add `bd_build.make_frame(payload_bytes)` and wire it into the loop script.

---

## What we **removed** or corrected

- ❌ Removed references to `parse_bd_frames` (no longer used). Replaced with internal **rolling BD framer**.
- ❌ Dropped the assumption that **all** long‑reply float runs started immediately at `sig+12`. We now **discover** offsets per slice/leaf (examples above).
- ✅ Fixed CSV labeling/rounding and timestamp formatting in `label_and_round_latest.py`.
- ✅ Clarified that **route** is derived from `station_id`, but **slice codes** (e.g., `92f0`, `9ca0`) are *program‑specific* and must be learned or synthesized with CRC recompute.

---

## Known‑good example commands

### S1T (route 0x20), slices 92f0/9300/9310
```bash
PYTHONPATH=. python biochar_app/pakbus/scripts/loop_27b_tx_pull.py   --addr <ipv6> --port 6785 --hello   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --reply-sig bdaffd00011ffd20   --seek-after-sig 12 --scan-step 4 --min-floats 12 --endian ">"   --out-dir pulls_27b_S1T --only s1t_92f0 s1t_9300 s1t_9310
```

### S2B (route 0x70), slices 9ca0 and 9c70
```bash
# 9ca0 (BattV + 3 triplets look good)
PYTHONPATH=. python biochar_app/pakbus/scripts/loop_27b_tx_pull.py   --addr <ipv6> --port 6785 --hello   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --reply-sig bdaffd00011ffd20   --seek-after-sig 13 --scan-step 1 --min-floats 20 --endian ">"   --out-dir pulls_27b_S2B --only s2b_9ca0

# 9c70 (later block)
PYTHONPATH=. python biochar_app/pakbus/scripts/loop_27b_tx_pull.py   --addr <ipv6> --port 6785 --hello   --reads 360 --read-gap-ms 200 --idle-timeout 12 --resend-every 6   --reply-sig bdaffd00011ffd20   --seek-after-sig 118 --scan-step 1 --min-floats 20 --endian ">"   --out-dir pulls_27b_S2B --only s2b_9c70
```

---

## Troubleshooting checklist

- **Got short acks but no long reply?** Confirm the TX CRC (replay from pcap output) and that the **route** matches the target leaf.
- **Long reply seen but no decoded floats?**
  - Run `snoop_after_sig.py` on the frames log; try the top float32 offset and matching endian/step.
  - Increase `--min-floats` cautiously; too high may reject valid runs.
- **Numbers look absurd (e.g., 1e7)?** You’re likely off by a few bytes; nudge `--seek-after-sig` by ±1..±8.
- **Timestamps wrong/odd seconds?** Regenerate with the fixed `label_and_round_latest.py` (uses clean `YYYY-MM-DD HH:MM:SS`).

---

*End of notes.*
