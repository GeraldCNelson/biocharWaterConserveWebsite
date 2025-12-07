# PakBus Data Pipeline — Master Notes

**Status (2025-10-20):**  
Live pull of Table 1 via multi-hop PakBus is working robustly using BD-framed “HELLO” + replay of captured TX frames, with float-decoding knobs tuned per leaf/slice. This doc collects TL;DR, recipes, scripts, CRC notes, and troubleshooting into one place.

---

## 📋 TL;DR (What Actually Works)

1. **HELLO handshake**  
   Send once over TCP on connect: