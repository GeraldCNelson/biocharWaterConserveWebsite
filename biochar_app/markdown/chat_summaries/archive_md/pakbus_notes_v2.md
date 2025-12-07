# pakbus_notes_v2

This is an updated version of the previous notes file. It incorporates the latest run results and clarifications.

# Field Log for fetch_table1_live Experiments

## What’s working

- **Pre-hello injection from disk**  
  - We can load ASCII-hex `*_raw.hex` files, split BD-framed messages, and pick the **0xAF hello** frame(s).  
  - The tool sends that 0xAF pre-hello and consistently gets a reply (**0xEF NAK**) from the logger. That proves: IP/port reach the datalogger gateway, framing/CRC are good, and the pre-hello makes it to the other end.

- **Flexible packet builder**  
  - You can set **Leaf** (`--leaf` or `--scan-leaves 2..13`), **Tran** (`--tran`), **TableId** (`--table-id`), start record, count, and timing (`--pre-wait-ms`), and choose to **reconnect** after pre-frames.  
  - **Header templating** works (we can copy specific header bytes from a known 0xAF into our outgoing 0x09 when needed).  
  - Directory scanning for pre-frames works (`--pre-hex-dir ...`), including recursive walks and ignoring deframed `frames.csv`.

- **Frame introspection utilities**  
  - `dump_all_frames.py` & `scan_sessions.py` run and summarize sessions; they correctly identify that the `*_frames/*.hex` are **not** 0x09/0x89 dialogues (mostly 0x44 CSV exports, 0xAF hellos, and 0xEF NAKs).  
  - Quick one-off Python snippets to sanitize hex and list ops are reliable.

## What not to do (things that bit us)

- **Don’t use `--mirror-header-from-pre`** (for now).  
  - It produced `…01 09 09…` (duplicated `0x09`) inside the P-bytes, making a malformed 0x09. Result: “No BD frames found.”

- **Don’t combine `--use-last-pre-tran` with `--tran …`.**  
  - Pick one. If you set `--tran`, let that win.

- **Don’t use deframed `*_frames/*.hex` as pre-hex sources.**  
  - Those contain payloads (e.g., op 0x44) not BD-framed control packets. Stick to `*_raw.hex` or a literal 0xAF.

- **Don’t send the second “big data” pre-frame** you pulled from `_raw.hex` (the 66-byte 0xAF with payload).  
  - That one correlates with the connection going silent. For pre-sync, only send the short 18-byte 0xAF “hello.”

- **Don’t assume Dst/Src defaults.**  
  - If you don’t set Leaf/Dst correctly, the logger can NAK or ignore. We tried scanning leaves; no 0x89 yet. Keep Leaf explicit while we refine header bytes.

## What remains / next steps (in order)

1. **Try the two “safe” run patterns (no mirroring):**
   - Fixed leaf you expect (e.g., S1T=2)  
     ```bash
     python -m biochar_app.pakbus.scripts.fetch_table1_live        --start-rec 0 --count 4 --timeout 10 --debug        --pre-hex "AF FD 70 01 0F FD 00 01 09 47 01 01 FF FD 71 25"        --pre-wait-ms 600        --tran 0x47        --leaf 2        --table-id 1
     ```
   - Or scan all leaves with the same settings (only the short 0xAF pre-hello).

2. **Vary only one thing at a time:**
   - `--tran 0x2F` (some captures used 0x2F).  
   - `--table-id 6` (if Table 1 is filtered/empty, some deployments expose via a different table).  
   - `--start-rec 0` vs `--start-rec 0xFFFF`, and `--count 4`.

3. **Header bite-sized mirroring (targeted, not full):**  
   - If we must mirror, copy only these from the 0xAF into the 0x09 header: **Dst, Src, Tran, and maybe Rx** (first 4–5 P-bytes), and **do not** touch the op code or insert an extra `0x09`.

4. **Timing tweaks:**  
   - Try `--pre-wait-ms 900` and 1200.  
   - Try **without** reconnect (`--reconnect-after-pre` off), then **with** reconnect (on). Some gateways buffer one packet after a pre-hello and require a tiny quiet window.

5. **Decode 0xEF reason (improve logging):**  
   - Add a tiny decoder to print the reason bits from the 0xEF tail (we already display the tail hex). This will tell us if it’s “invalid transaction”, “no route”, “not authorized”, etc., and guide the next adjustment.

6. **Confirm the Leaf↔Station map at the logger:**  
   - You posted:  
     ```
     2:S1T 3:S1M 4:S1B 5:S2T 6:S2M 7:S2B 8:S3T 9:S3M 10:S3B 11:S4T 12:S4M 13:S4B
     ```  
     Keep testing with those IDs. If none respond, either the gateway expects a different **Dst/Src** pair than we’re using or the table/record query isn’t permitted from your path.

## What the symptoms mean

- **“hello-reply 14 bytes … 0xEF …”**  
  - The logger saw the short pre-hello and sent a NAK. This is still “good news”: path is live, framing is fine. The NAK likely complains about context (wrong Dst/Src/Tran or not ready for the following op).

- **“No BD frames found in reply.”**  
  - The logger ignored the 0x09 (malformed or not addressed correctly) or the gateway closed the socket after the pre-hello. We saw this when we (a) mirrored too much (double 0x09), or (b) sent the large 0xAF data frame first, or (c) had the wrong destination leaf.

- **“NAK 0xEF from logger: tail=…”**  
  - Same as above but explicitly parsed; use the tail bytes to decode the reason (todo).

## Quick rules of thumb

- Use **only the short 18-byte 0xAF hello** as pre-frame.  
- Set **either** `--tran` **or** `--use-last-pre-tran` (not both).  
- **Don’t** use `--mirror-header-from-pre` (full) — if needed, mirror only Dst/Src/Tran.  
- Prefer `--start-rec 0` and `--count 4` while testing; switch to `0xFFFF` only once a path works.  
- If you get a 0xEF after the hello, **don’t panic** — it proves the link is alive. Tweak header bytes (Dst/Src/Tran) and timing next.
