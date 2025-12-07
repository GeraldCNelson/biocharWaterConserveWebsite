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
