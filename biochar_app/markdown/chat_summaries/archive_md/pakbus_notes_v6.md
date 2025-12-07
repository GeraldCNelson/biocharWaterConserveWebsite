# PakBus Notes v6

## 1. Fixing Tuple vs Bytes in `bus.write`
**Issue:** `bus.get_collectdata_cmd()` returns a tuple `(cmd_bytes, txn_id)`.  
Calling `bus.write(cmd)` directly caused:
```
TypeError: a bytes-like object is required, not 'tuple'
```
**Fix:** Unpack the tuple and only write the raw bytes:
```python
cmd_bytes, txn_id = bus.get_collectdata_cmd(tbl_nbr, tbl_defsig)
bus.transaction = txn_id
bus.write(cmd_bytes)
```

## 2. Providing `raw` for `unpack_collectdata_response`
**Issue:** `unpack_collectdata_response` expects `msg['raw']`, but `wait_packet` returned `(msg, raw)`, causing:
```
KeyError: 'raw'
```
**Fix:** Attach the raw bytes back into `msg` before unpacking:
```python
msg, raw = bus.wait_packet(bus.transaction)
msg['raw'] = raw
rows = bus.unpack_collectdata_response(msg)
```

## 3. Decoding Table Names from Bytes
**Issue:** `parse_tabledef` may return `TableName` as `bytes`, leading to mixed-type names in logs.  
**Fix:** Use a helper to decode bytes:
```python
def _decode_name(rec: dict) -> str | None:
    nm = rec.get('Header', {}).get('TableName') or rec.get('TableName')
    if isinstance(nm, bytes):
        nm = nm.decode('utf-8', errors='ignore')
    return nm
```

*End of notes.*
