# Pakbus Master Notes

## Changelog
- 2025-09-30 – Initial merge of pakbus_hex_summary_v1.md and pakbus_notes_v5.md

---

# PakBus Hex Summary

This file collects all key hex values and inner payloads identified so far.

---

## Hello Frames

- **Hello inner (from logs):**
  - `90 01 0f fd 73 d3`
  - Full BD-framed: `bd 90 01 0f fd 73 d3 c2 d6 bd`

- **Hello-reply candidate:**
  - `bd ef ff 10 01 0f ff 00 01 0e 00 dd f0 bd`

---

## Router Headers

- First guess (auto):  
  `a0 01 6f fd 10 00 30 ff d0 90`

- Extracted from PCAP (row 9, edit removing CRC/BD):  
  `a0 01 6f fd 00 01 0f fd 09 05 01 02 ff ff 28`

- Alternative extracted (row 9907 candidate, data.len=31):  
  `a0 01 6f fd 10 03 0f fd 09 68 00 00 06 00 02`

---

## BD … 2C Candidates (Table Read attempts)

- No confirmed good 2C reply yet, but key transmissions include:
  - Example candidate frame start: `bd 2c … bd`
  - Various attempts with transaction byte after op (`2c 90 …`, `2c 91 …` etc.)

---

## Observed Data Packets (from PCAP)

- **data.len=31:**  
  `bd a0 01 6f fd 10 03 0f fd 09 68 00 00 06 00 02 2c 79 00 01 00 fc 0d 01 00 fe 0d 00 00 91 ec bd`

- **data.len=32:**  
  `bd a0 01 6f fd 10 03 0f fd 09 28 00 00 06 00 02 2c 79 00 01 00 fc 0d 01 00 fe 0d 00 00 4a 29 bd`

- **data.len=42:**  
  `bd af fd 70 01 0f fd 00 01 0a f9 00 f9 20 06 20 09 20 0b 20 02 20 0a 20 07 20 08 20 0c 20 05 20 03 20 0d 20 04 af fd dd c1 cb d`

---

## Notes / Additional Discoveries

(Add new findings here as they come up, with timestamps if possible.)


---

# Field Log for `fetch_table1_live` Experiments (v5)

## What’s working
- **Pre-hello injection from disk / literal hex**  
  - The short hello `90 01 0f fd 73 d3` (seen in S1M pcapng) reliably produces a reply:  
    `bd ef ff 10 01 0f ff 00 01 0e 00 dd f0 bd`.  
  - That proves:  
    - We reach the logger over IPv6:6785.  
    - CRC framing is correct.  
    - The logger acknowledges the hello (via NAK).

- **Flexible packet builder**  
  - Leaf, Tran, TableId, start/count, and timing are configurable.  
  - Injected header templates work (we can pull Dst/Src/Tran fields from a known hello).  

- **Frame introspection utilities**  
  - `dump_all_frames.py`, `scan_sessions.py` confirm that most of the pre-frames we see are AF hellos and EF NAKs.  
  - PC400-style hellos are short, not the longer AF payloads.

## What not to do
- **Don’t mirror full headers from pre-hello**. It caused malformed double-`0x09` inserts.  
- **Don’t combine `--tran` with `--use-last-pre-tran`**. Pick one.  
- **Don’t use deframed `_frames/*.hex` as pre-hex**. Only raw captures or literal hello hex.  
- **Don’t send the longer 66-byte AF “data” pre-frame**. It correlates with silence. Stick to the short hello.  

## Current blockers
- After hello + NAK, all subsequent read attempts (opcode `0x2C` to table 1, leaf 3) return *nothing*:  
  ```
  [ERROR] No BD frames found in reply.
  ```
- We’ve confirmed this across variants:
  - `--start-rec 0xFFFF` vs `0`.  
  - `--count 1` vs `4`.  
  - Leaves 2–13.  
  - Gaps from 600–2000 ms.  
- Always the same: hello acknowledged, but table query ignored.

## Lessons from S1M pcapng (Sept 2025)
- The logger’s first reply after hello was a **14-byte EF NAK**.  
- That shows we’re “in range” but our table read header is still wrong.  
- Most likely culprits:
  1. **Dst/Src mismatch**: PC400 may use specific values (not always “leaf=3”).  
  2. **Table ID offset**: what we call Table 1 may be mapped differently.  
  3. **Tran value expectations**: logger may require we continue the same Tran from hello.  

## Next steps
1. Add **EF NAK reason decoder** to fetch script — the 0xEF tail tells us why it rejected (invalid Tran, bad address, unauthorized, etc).  
2. Replicate **exact post-hello read** from the S1M pcap (pull bytes literally into our script).  
3. Systematically vary:  
   - TableId (try 1, 6).  
   - Leaf (2–13).  
   - Tran (continue from hello vs fixed 0x90).  
4. Keep using **only the short 0xAF hello**.  
5. Log all frames to disk (both hex and parsed).  
