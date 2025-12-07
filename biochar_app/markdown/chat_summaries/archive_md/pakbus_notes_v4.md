# PakBus Fetch Attempts – Corrected Notes (v4)

## Key Corrections
- S1M’s **PakBus address is 3**, not `0x79` (121).  
- Earlier experiments using `--leaf 121` were invalid and guaranteed to fail.  
- All retry plans must use the correct PakBus addresses (2–13) from PC400 or config.py.

## Current Understanding
- Hello handshake with CR800 router works (BD-framed hello/reply observed).  
- Outbound frames are delivered to the leaf, but no valid reply frames yet.  
- Need exact inner request layout matching what PC400 uses.  

## Next Steps
1. **Re-run tests with correct leaf IDs** (e.g., `--leaf 3` for S1M).  
2. **Extract inner PakBus request from PC400 pcap** and use it verbatim in our payload.  
3. **Adjust prefix/extra fields** until logger replies with non-empty BD frame.  
4. If success: add Table1 parsing logic.  

## Checklist of Experiments to Redo with Correct Leaf
- Mirror-header runs (`--mirror-header-len 9`, `--keep-first-tran`).  
- Opcode variants (`0xA0`, `0x1F` probe).  
- Prefix insertion (`0D 00 00 05 00 02`) before/after PBRouter.  
- Start-rec variations: `0xFFFF`, `0`, and recent mid-values.  

## Conclusion
The blocker was the **wrong PakBus address assumption (0x79)**. With corrected IDs, the next tests have a real chance of triggering replies from the leaf loggers.
