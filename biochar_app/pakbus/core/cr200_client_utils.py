# biochar_app/pakbus/cr200_client_utils.py
# Minimal, Python 3.13-safe PakBus helpers for CR200/CR800 workflows.
#
# Origin: derived from Dietrich Feist’s PyPak (GPL), trimmed and modernized.
# License: GPLv2+ (inherits the original PyPak license terms).
#
# Protocol notes:
# - Hello (PakCtrl):         HiProtoCode = 0x00
# - BMP5 GetTableDefs 0x16:  HiProtoCode = 0x01  → response 0x17
# - BMP5 CollectData 0x09:   HiProtoCode = 0x01  → response 0x89
# - BMP5 FileUpload 0x0F:    HiProtoCode = 0x05
#
# This module implements:
#   - Framing/quoting/signature (0xBD/0xBC)
#   - Hello
#   - BMP5 GetTableDefs + parser
#   - BMP5 CollectData (builder + exact parser using TableDefs)
#   - FileUpload (optional)
#   - Time helpers (CR basic epoch 1990-01-01 with (sec,nsec))
#   - Convenience wrappers for common tasks
#
# All high-level calls accept RouterPhyAddr to route through an intermediary (e.g., CR800).

from __future__ import annotations

import calendar
import logging
import math
import socket
import struct
import errno
import time
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

log = logging.getLogger("pakbus.utils")

BytesLike = Union[bytes, bytearray, memoryview]

# ----------------------------- Public types ----------------------------------

Header = Dict[str, int]
Message = Dict[str, Any]

# ----------------------------- Data type map ---------------------------------
# Campbell type codes (subset used by CR2xx/CR8xx).  Names here are "logical" and
# map to a decode format. We keep both big-endian (B) and little-endian (L) IEEE
# where necessary; most CR systems use IEEE4B for floats in tables.

datatype: Dict[str, Dict[str, Any]] = {
    "Byte":   {"code": 1,  "fmt": "B",   "size": 1},
    "UInt2":  {"code": 2,  "fmt": ">H",  "size": 2},
    "UInt4":  {"code": 3,  "fmt": ">L",  "size": 4},
    "Int1":   {"code": 4,  "fmt": "b",   "size": 1},
    "Int2":   {"code": 5,  "fmt": ">h",  "size": 2},
    "Int4":   {"code": 6,  "fmt": ">l",  "size": 4},

    # Campbell packed FP types (rare in modern tables but still supported)
    "FP2":    {"code": 7,  "fmt": ">H",  "size": 2},       # special decode
    "FP3":    {"code": 15, "fmt": "3s",  "size": 3},       # raw bytes (not decoded here)
    "FP4":    {"code": 8,  "fmt": "4s",  "size": 4},       # raw bytes (not decoded here)

    "IEEE4B": {"code": 9,  "fmt": ">f",  "size": 4},       # 32-bit float, big-endian
    "IEEE8B": {"code": 18, "fmt": ">d",  "size": 8},       # 64-bit float, big-endian

    "Bool":   {"code": 10, "fmt": "B",   "size": 1},
    "Bool8":  {"code": 17, "fmt": "B",   "size": 1},
    "Bool2":  {"code": 27, "fmt": ">H",  "size": 2},
    "Bool4":  {"code": 28, "fmt": ">L",  "size": 4},

    # Time encodings
    "Sec":    {"code": 12, "fmt": ">l",  "size": 4},       # seconds (signed)
    "USec":   {"code": 13, "fmt": "6s",  "size": 6},       # rarely used here
    "NSec":   {"code": 14, "fmt": ">2l", "size": 8},       # (secs, nsecs)

    # Strings
    "ASCII":  {"code": 11, "fmt": "s",   "size": None},    # fixed length provided by caller
    "ASCIIZ": {"code": 16, "fmt": "s",   "size": None},    # NUL-terminated

    # Unused legacy little-endian aliases (kept for tabledef completeness)
    "Short":  {"code": 19, "fmt": "<h",  "size": 2},
    "Long":   {"code": 20, "fmt": "<l",  "size": 4},
    "UShort": {"code": 21, "fmt": "<H",  "size": 2},
    "ULong":  {"code": 22, "fmt": "<L",  "size": 4},
    "IEEE4L": {"code": 24, "fmt": "<f",  "size": 4},
    "IEEE8L": {"code": 25, "fmt": "<d",  "size": 8},
    "SecNano":{"code": 23, "fmt": "<2l", "size": 8},       # not used by CR2xx/CR8xx BMP5
}

# Reverse lookup from code → logical name
code_to_type: Dict[int, str] = {spec["code"]: name for name, spec in datatype.items()}

# ----------------------- Transaction numbering (8-bit) -----------------------

_transact: int = 0
def new_tran_nbr() -> int:
    global _transact
    _transact = (_transact + 1) & 0xFF
    return _transact

# ---------------------- Framing, quoting, signatures -------------------------

FRAME = 0xBD
QUOT  = 0xBC
Q_BC  = bytes([QUOT, 0xDC])  # quote for 0xBC
Q_BD  = bytes([QUOT, 0xDD])  # quote for 0xBD

def calc_sig_for(buff: BytesLike, seed: int = 0xAAAA) -> int:
    sig = seed
    for x in bytes(buff):
        j = sig
        sig = (sig << 1) & 0x1FF
        if sig >= 0x100:
            sig += 1
        low = (sig + (j >> 8) + x) & 0xFF
        sig = (low | (j << 8)) & 0xFFFF
    return sig

def calc_sig_nullifier(sig: int) -> bytes:
    nulb = bytearray()
    for _ in (1, 2):
        sig = calc_sig_for(nulb, sig)
        sig2 = (sig << 1) & 0x1FF
        if sig2 >= 0x100:
            sig2 += 1
        nulb.append((0x100 - (sig2 + (sig >> 8))) & 0xFF)
    return bytes(nulb)

def quote(pkt: BytesLike) -> bytes:
    pkt_b = bytes(pkt)
    return pkt_b.replace(b"\xBC", Q_BC).replace(b"\xBD", Q_BD)

def unquote(pkt: BytesLike) -> bytes:
    pkt_b = bytes(pkt)
    return pkt_b.replace(Q_BD, b"\xBD").replace(Q_BC, b"\xBC")

def send(sock: socket.socket, pkt: BytesLike) -> None:
    body = bytes(pkt)
    frame = quote(body + calc_sig_nullifier(calc_sig_for(body)))
    sock.sendall(b"\xBD" + frame + b"\xBD")

def _recv_byte(sock: socket.socket) -> int:
    """
    Receive exactly one byte and return it as an int (0..255).
    Raises TimeoutError on socket timeout; raises RuntimeError on EOF.
    """
    try:
        b = sock.recv(1)
    except socket.timeout:
        raise TimeoutError("socket recv timeout")
    if not b:
        raise RuntimeError("socket closed while reading")
    return b[0]

def recv(sock: socket.socket) -> Optional[bytes]:
    """
    Receive one framed PakBus packet (bytes) or None if signature check fails.
    Uses _recv_byte() which returns ints, making FRAME comparisons correct.
    """
    # sync to first FRAME
    byte: Optional[int] = None
    while byte != FRAME:
        byte = _recv_byte(sock)
    # skip repeated FRAMEs
    while True:
        byte = _recv_byte(sock)
        if byte != FRAME:
            break
    buf = bytearray()
    while byte != FRAME:
        buf.append(byte)
        byte = _recv_byte(sock)
    raw = unquote(buf)
    if calc_sig_for(raw) != 0:
        return None
    return raw[:-2]  # strip signature nullifier

# ------------------------------ Packet header --------------------------------

def pakbus_hdr(
        DstNodeId: int,
        SrcNodeId: int,
        HiProtoCode: int,
        ExpMoreCode: int = 0x0,
        LinkState: int = 0x0,
        Priority: int = 0x0,
        HopCnt: int = 0x0,
        DstPhyAddr: Optional[int] = None,
        SrcPhyAddr: Optional[int] = None,
) -> bytes:
    # Default physical == logical if not specified
    if DstPhyAddr is None:
        DstPhyAddr = DstNodeId
    if SrcPhyAddr is None:
        SrcPhyAddr = SrcNodeId
    return struct.pack(
        ">4H",
        ((LinkState & 0xF) << 12) | (DstPhyAddr & 0xFFF),
        ((ExpMoreCode & 0x3) << 14) | ((Priority & 0x3) << 12) | (SrcPhyAddr & 0xFFF),
        ((HiProtoCode & 0xF) << 12) | (DstNodeId & 0xFFF),
        ((HopCnt & 0xF) << 12) | (SrcNodeId & 0xFFF),
        )

# ----------------------------- Encode / decode -------------------------------

def decode_bin(Types: Sequence[str], buff: BytesLike, length: int = 1) -> Tuple[List[Any], int]:
    """
    Decode a sequence of BMP5 field types from a bytes-like buffer.
    """
    data = bytes(buff)
    data_len = len(data)
    offset = 0
    values: List[Any] = []

    def _require(n: int) -> None:
        if offset + n > data_len:
            raise ValueError(
                f"decode_bin: need {n} more bytes at {offset}, only {data_len - offset} remain"
            )

    for Type in Types:
        spec = datatype[Type]
        fmt: str = spec["fmt"]
        size = spec["size"]

        if Type == "ASCIIZ":
            nul = data.find(b"\x00", offset)
            if nul < 0:
                raise ValueError("ASCIIZ: missing NUL terminator")
            value: Any = data[offset:nul]
            used = (nul - offset) + 1

        elif Type == "ASCII":
            if length <= 0:
                raise ValueError("ASCII: non-positive length")
            _require(int(length))
            used = int(length)
            value = data[offset:offset + used]

        elif Type == "FP2":
            _require(2); used = 2
            (raw,) = struct.unpack_from(fmt, data, offset)
            mant = raw & 0x1FFF
            exp = (raw >> 13) & 0x3
            sign = (raw >> 15) & 0x1
            value = (-1.0 if sign else 1.0) * float(mant) / (10 ** exp)

        elif Type == "NSec":
            _require(8); used = 8
            s1, s2 = struct.unpack_from(fmt, data, offset)
            value = (int(s1), int(s2))

        else:
            if size is None:
                raise ValueError(f"Unhandled variable-sized type {Type}")
            _require(int(size))
            used = int(size)
            if "s" in fmt:
                (value,) = struct.unpack_from(f"{used}s", data, offset)
            else:
                tup = struct.unpack_from(fmt, data, offset)
                value = tup[0] if len(tup) == 1 else tup

        values.append(value)
        offset += used

    return values, offset

def encode_bin(Types: Sequence[str], Values: Sequence[Any]) -> bytes:
    if len(Types) != len(Values):
        raise ValueError("Types and Values length mismatch")
    out = bytearray()
    for Type, value in zip(Types, Values):
        spec = datatype[Type]
        fmt: str = spec["fmt"]
        size = spec["size"]

        if Type == "ASCIIZ":
            if isinstance(value, str):
                value = value.encode("ascii", "ignore")
            out += bytes(value) + b"\x00"
        elif Type == "ASCII":
            if isinstance(value, str):
                value = value.encode("ascii", "ignore")
            out += bytes(value)
        elif Type == "NSec":
            s1, s2 = value  # type: ignore[misc]
            out += struct.pack(fmt, int(s1), int(s2))
        elif Type == "FP2":
            raise NotImplementedError("Encoding FP2 not required here")
        else:
            if isinstance(value, (bytes, bytearray, memoryview)) and size and "s" in fmt:
                out += struct.pack(f"{int(size)}s", bytes(value))
            else:
                out += struct.pack(fmt, value)
    return bytes(out)

# ------------------------------- Packet decode -------------------------------

def decode_pkt(pkt: BytesLike) -> Tuple[Header, Message]:
    hdr: Header = {}
    msg: Message = {}
    try:
        data = bytes(pkt)
        rawhdr = struct.unpack(">4H", data[0:8])
        hdr = {
            "LinkState": rawhdr[0] >> 12,
            "DstPhyAddr": rawhdr[0] & 0x0FFF,
            "ExpMoreCode": (rawhdr[1] & 0xC000) >> 14,
            "Priority": (rawhdr[1] & 0x3000) >> 12,
            "SrcPhyAddr": rawhdr[1] & 0x0FFF,
            "HiProtoCode": rawhdr[2] >> 12,
            "DstNodeId": rawhdr[2] & 0x0FFF,
            "HopCnt": rawhdr[3] >> 12,
            "SrcNodeId": rawhdr[3] & 0x0FFF,
        }
        raw = data[8:]
        (msg_type, tran), _ = decode_bin(("Byte", "Byte"), raw[:2])
        msg = {"MsgType": int(msg_type), "TranNbr": int(tran), "raw": raw}
    except (struct.error, IndexError, ValueError):
        return hdr, msg
    return hdr, msg

# ----------------------------- PakCtrl: Hello --------------------------------

def pkt_hello_cmd(
        DstNodeId: int,
        SrcNodeId: int,
        IsRouter: int = 0x00,
        HopMetric: int = 0x02,
        VerifyIntv: int = 1800,
) -> Tuple[bytes, int]:
    tn = new_tran_nbr()
    hdr = pakbus_hdr(DstNodeId, SrcNodeId, 0x0)  # PakCtrl
    msg = encode_bin(["Byte", "Byte", "Byte", "Byte", "UInt2"], [0x09, tn, IsRouter, HopMetric, VerifyIntv])
    return hdr + msg, tn

def msg_hello(msg: Message) -> Message:
    vals, _ = decode_bin(["Byte", "Byte", "UInt2"], msg["raw"][2:])  # type: ignore[index]
    msg["IsRouter"], msg["HopMetric"], msg["VerifyIntv"] = int(vals[0]), int(vals[1]), int(vals[2])
    return msg

def wait_pkt(
        s: socket.socket,
        DstNodeId: int,
        SrcNodeId: int,
        TranNbr: int,
        timeout: float = 5.0,
) -> Tuple[Header, Message]:
    """
    Wait for a PakBus packet matching the expected Src/Dst/TranNbr.
    Returns (hdr, msg) upon match; if the deadline expires, returns the last
    decoded (hdr, msg) seen (which may be empty dicts) so callers can decide
    how to proceed.

    Notes:
    - Uses a monotonic deadline (robust to system clock changes).
    - Applies a short per-iteration socket timeout slice so recv() never
      blocks for the full timeout in one call.
    """
    total_timeout = max(0.1, float(timeout))
    slice_timeout = 1.0  # seconds per recv slice; adjusted down near deadline

    deadline = time.monotonic() + total_timeout
    last_hdr: Header = {}
    last_msg: Message = {}

    try:
        s.settimeout(min(slice_timeout, total_timeout))
    except Exception:
        pass

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return last_hdr, last_msg

        try:
            s.settimeout(max(0.05, min(slice_timeout, remaining)))
        except Exception:
            pass

        try:
            rcv = recv(s)  # framed packet or None
        except (socket.timeout, TimeoutError):
            continue
        except InterruptedError:
            continue
        except OSError as e:
            if getattr(e, "errno", None) in (errno.EINTR, errno.EAGAIN, errno.EWOULDBLOCK):
                continue
            raise

        if not rcv:
            continue

        try:
            hdr, msg = decode_pkt(rcv)
        except Exception:
            continue

        last_hdr, last_msg = hdr, msg

        if (
            hdr.get("DstNodeId") == DstNodeId
            and hdr.get("SrcNodeId") == SrcNodeId
            and msg.get("TranNbr") == TranNbr
        ):
            return hdr, msg

def ping_node(
        s: socket.socket,
        *,
        DstNodeId: int,
        SrcNodeId: int,
        RouterPhyAddr: Optional[int] = None,
        timeout: float = 5.0,
) -> Message:
    pkt, tn = pkt_hello_cmd(DstNodeId, SrcNodeId)
    if RouterPhyAddr is not None:
        hdr_bytes = pakbus_hdr(
            DstNodeId=DstNodeId, SrcNodeId=SrcNodeId, HiProtoCode=0x0,
            DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId,
            ExpMoreCode=0, LinkState=0, Priority=0, HopCnt=0,
        )
        pkt = hdr_bytes + pkt[8:]
    send(s, pkt)
    _hdr, msg = wait_pkt(s, DstNodeId=SrcNodeId, SrcNodeId=DstNodeId, TranNbr=tn, timeout=timeout)
    return msg

# ---------------------- BMP5: Collect Data & Table Defs ----------------------

# CollectData modes (byte after SecurityCode):
#   0x04  MostRecent (P1 = N records)
#   0x05  RecNoRange  (P1 = begin_rec)
#   0x06  RecNoRange  (P1 = begin_rec, P2 = end_rec)
#   0x07  DateRange   (P1 = (sec,nsec) begin, P2 = (sec,nsec) end)
#   0x08  DateToNewest(P1 = begin (sec,nsec), P2 ignored or (sec,nsec) per fw)

def pkt_collectdata_cmd(
        DstNodeId: int,
        SrcNodeId: int,
        TableNbr: int,
        TableDefSig: int,
        FieldNbr: Sequence[int] = (),
        CollectMode: int = 0x04,
        P1: int | Tuple[int, int] = 1,
        P2: int | Tuple[int, int] = 0,
        SecurityCode: int = 0x0000,
) -> Tuple[bytes, int]:
    tn = new_tran_nbr()

    # BMP5 header
    hdr_bytes = pakbus_hdr(DstNodeId, SrcNodeId, 0x1)

    # 0x09 request
    msg = encode_bin(["Byte", "Byte", "UInt2", "Byte"], [0x09, tn, SecurityCode, CollectMode])
    msg += encode_bin(["UInt2", "UInt2"], [TableNbr, TableDefSig & 0xFFFF])

    # Mode parameters
    if CollectMode == 0x04:
        msg += encode_bin(["UInt4"], [int(P1)])  # most recent N
    elif CollectMode == 0x05:
        msg += encode_bin(["UInt4"], [int(P1)])  # from record#
    elif CollectMode == 0x06:
        msg += encode_bin(["UInt4", "UInt4"], [int(P1), int(P2)])  # rec range
    elif CollectMode == 0x07:
        p1 = P1 if isinstance(P1, tuple) else (0, 0)
        p2 = P2 if isinstance(P2, tuple) else (0, 0)
        msg += encode_bin(["NSec", "NSec"], [p1, p2])  # time range
    elif CollectMode == 0x08:
        p1 = P1 if isinstance(P1, tuple) else (0, 0)
        msg += encode_bin(["NSec"], [p1])              # date → newest
    else:
        pass

    # Field list (UInt2 numbers 1..N, terminated by 0)
    fieldlist = list(FieldNbr) + [0]
    msg += encode_bin(["UInt2"] * len(fieldlist), fieldlist)

    return hdr_bytes + msg, tn

def msg_collectdata_response(msg: Message) -> Message:
    raw = msg.get("raw", b"")
    if not raw or len(raw) < 3:
        msg["RespCode"] = 0x0E
        msg["RecData"]  = b""
        return msg
    (resp,), used = decode_bin(["Byte"], raw[2:])
    msg["RespCode"] = int(resp)
    if msg["RespCode"] != 0:
        log.warning("CollectData RC=%02x (%d bytes payload)", msg["RespCode"], len(msg.get("RecData", b"")))
    msg["RecData"]  = raw[2 + used:]
    return msg

def parse_tabledef(raw: BytesLike) -> List[Dict[str, Any]]:
    """
    Parse Table Definitions blob from 0x17 response into a structured list.
    """
    out: List[Dict[str, Any]] = []
    data = bytes(raw)
    offset = 0

    # The blob starts with a flags/version byte we can ignore (consume)
    _, used = decode_bin(["Byte"], data[offset:])
    offset += used

    while offset < len(data):
        tblhdr: Dict[str, Any] = {}
        fields: List[Dict[str, Any]] = []
        start = offset

        vals, used = decode_bin(["ASCIIZ", "UInt4", "Byte", "NSec", "NSec"], data[offset:])
        offset += used
        tblhdr["TableName"], tblhdr["TableSize"], tblhdr["TimeType"], tblhdr["TblTimeInto"], tblhdr["TblInterval"] = vals

        # Fields loop
        while True:
            (fieldtype_byte,), used = decode_bin(["Byte"], data[offset:])
            offset += used
            if fieldtype_byte == 0:
                break

            fld: Dict[str, Any] = {"ReadOnly": int(fieldtype_byte) >> 7}
            ft_code = int(fieldtype_byte) & 0x7F
            fld["FieldType"] = code_to_type.get(ft_code, ft_code)

            (fname,), used = decode_bin(["ASCIIZ"], data[offset:])
            offset += used
            fld["FieldName"] = fname

            aliases: List[bytes] = []
            while True:
                (alias,), used = decode_bin(["ASCIIZ"], data[offset:])
                offset += used
                if alias == b"":
                    break
                aliases.append(alias)
            fld["AliasName"] = aliases

            vals2, used = decode_bin(["ASCIIZ", "ASCIIZ", "ASCIIZ", "UInt4", "UInt4"], data[offset:])
            offset += used
            fld["Processing"], fld["Units"], fld["Description"], fld["BegIdx"], fld["Dimension"] = vals2

            subdims: List[int] = []
            while True:
                (sd,), used = decode_bin(["UInt4"], data[offset:])
                offset += used
                if sd == 0:
                    break
                subdims.append(int(sd))
            fld["SubDim"] = subdims

            fields.append(fld)

        # 16-bit signature across this table block
        tblsig = calc_sig_for(data[start:offset]) & 0xFFFF
        out.append({"Header": tblhdr, "Fields": fields, "Signature": int(tblsig)})

    return out

def parse_collectdata(raw: BytesLike, tabledef: List[Dict[str, Any]], FieldNbr: Sequence[int] = ()) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Parse the payload from a 0x89 response using the given table definition.
    """
    data = bytes(raw)
    offset = 0
    recdata: List[Dict[str, Any]] = []

    while offset < len(data) - 1:
        frag: Dict[str, Any] = {}

        vals, used = decode_bin(["UInt2", "UInt4"], data[offset:])
        offset += used
        table_nbr, beg_rec = int(vals[0]), int(vals[1])
        frag["TableNbr"] = table_nbr
        frag["BegRecNbr"] = beg_rec

        # Resolve table name
        tname = tabledef[table_nbr - 1]["Header"]["TableName"]
        if isinstance(tname, (bytes, bytearray)):
            tname = tname.decode("ascii", "ignore")
        frag["TableName"] = tname

        # Offset flag + embedded info
        (isoffset_byte,), used = decode_bin(["Byte"], data[offset:])
        offset += used
        isoffset = int(isoffset_byte) >> 7
        frag["IsOffset"] = isoffset

        if isoffset:
            (byteoffset,), used = decode_bin(["UInt4"], data[offset:])
            offset += used
            frag["ByteOffset"] = int(byteoffset) & 0x7FFFFFFF
            frag["NbrOfRecs"] = None
            frag["RecFrag"] = data[offset:-1]  # raw remainder
            offset = len(data) - 1
        else:
            (nbrecs,), used = decode_bin(["UInt2"], data[offset:])
            offset += used
            frag["NbrOfRecs"] = int(nbrecs) & 0x7FFF

            # If table has a fixed interval, server may send base time once
            interval = tabledef[table_nbr - 1]["Header"]["TblInterval"]
            has_fixed = (interval != (0, 0))
            base_time: Optional[Tuple[int, int]] = None

            if has_fixed:
                (base_time,), used = decode_bin(["NSec"], data[offset:])
                offset += used

            # Field list to decode
            fields = list(FieldNbr) if FieldNbr else list(range(1, len(tabledef[table_nbr - 1]["Fields"]) + 1))

            frag_rows: List[Dict[str, Any]] = []
            for n in range(frag["NbrOfRecs"]):  # type: ignore[arg-type]
                rec: Dict[str, Any] = {"RecNbr": beg_rec + n}

                if has_fixed and base_time is not None:
                    (bsec, bnsec) = base_time
                    (isec, insec) = interval
                    sec = bsec + n * int(isec)
                    nsec = bnsec + n * int(insec)
                    if nsec >= 1_000_000_000:
                        carry = nsec // 1_000_000_000
                        sec += carry
                        nsec -= carry * 1_000_000_000
                    rec["TimeOfRec"] = (sec, nsec)
                else:
                    (tor,), used = decode_bin(["NSec"], data[offset:])
                    offset += used
                    rec["TimeOfRec"] = tor

                values_map: Dict[str, Any] = {}
                for fn in fields:
                    meta = tabledef[table_nbr - 1]["Fields"][fn - 1]
                    fname_raw = meta["FieldName"]
                    fname = fname_raw.decode("ascii", "ignore") if isinstance(fname_raw, (bytes, bytearray)) else str(fname_raw)
                    ftype = meta["FieldType"]
                    dim = int(meta["Dimension"])

                    if ftype == "ASCII":
                        (val,), used = decode_bin([ftype], data[offset:], length=dim)
                        try:
                            val = val.decode("ascii", "ignore")
                        except Exception:
                            pass
                    else:
                        vals2, used = decode_bin([ftype] * dim, data[offset:])
                        val = vals2 if dim > 1 else vals2[0]

                    offset += used
                    values_map[fname] = val

                rec["Fields"] = values_map
                frag_rows.append(rec)

            frag["RecFrag"] = frag_rows

        recdata.append(frag)

    (more,), _ = decode_bin(["Bool"], data[offset:])
    return recdata, bool(more)

# ------------------------------- Time helpers --------------------------------

nsec_base = calendar.timegm((1990, 1, 1, 0, 0, 0))
nsec_tick = 1e-9

def nsec_to_time(nsec: Tuple[int, int], epoch: int = nsec_base, tick: float = nsec_tick) -> float:
    return float(epoch + int(nsec[0])) + int(nsec[1]) * tick

def time_to_nsec(timestamp: float, epoch: int = nsec_base, tick: float = nsec_tick) -> Tuple[int, int]:
    fp, ip = math.modf(timestamp)
    return int(ip - epoch), int(fp / tick)

# ------------------------------ High-level utils -----------------------------

def open_socket(host: str, *, Port: int = 6785, Timeout: float = 20.0) -> Optional[socket.socket]:
    """
    Open a TCP socket to the CR800 (router) host:Port.
    Tries all address families returned by getaddrinfo and returns the first
    connected socket with Timeout applied. Returns None if all attempts fail.
    """
    try:
        # family, type, proto, canonname, sockaddr
        infos = socket.getaddrinfo(host, Port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except Exception:
        infos = []

    for fam, typ, proto, _canon, sa in infos:
        sock: Optional[socket.socket] = None
        try:
            sock = socket.socket(fam, typ, proto)
            sock.settimeout(float(Timeout))
            sock.connect(sa)
            return sock  # success
        except BaseException:
            try:
                if sock is not None:
                    sock.close()
            except Exception:
                pass
            continue

    return None

# ---- BMP5 GetTableDefs (0x16 -> 0x17) ----

def get_tabledefs_bmp5(
    s: socket.socket,
    *,
    DstNodeId: int,
    SrcNodeId: int,
    RouterPhyAddr: Optional[int] = None,
    timeout: float = 10.0,
) -> bytes:
    try:
        ping_node(s, DstNodeId=DstNodeId, SrcNodeId=SrcNodeId,
                  RouterPhyAddr=RouterPhyAddr, timeout=5.0)
    except Exception:
        pass

    def _try_once(hdr_opt: Dict[str, Any], flag_byte: int) -> Optional[bytes]:
        tn = new_tran_nbr()
        body = encode_bin(["Byte", "Byte", "Byte"], [0x16, tn, flag_byte])
        hdr_bytes = pakbus_hdr(DstNodeId=DstNodeId, SrcNodeId=SrcNodeId,
                               HiProtoCode=0x01, **hdr_opt)
        send(s, hdr_bytes + body)
        _hdr, msg = wait_pkt(s, DstNodeId=SrcNodeId, SrcNodeId=DstNodeId,
                             TranNbr=tn, timeout=timeout)
        if not msg or msg.get("MsgType") != 0x17:
            return None
        raw = msg.get("raw", b"")
        if len(raw) < 3:
            return None
        resp = raw[2]
        blob_bytes = raw[3:]
        return blob_bytes if resp == 0 and blob_bytes else None

    tries: List[Tuple[Dict[str, Any], int]] = []
    if RouterPhyAddr is not None:
        hdr_A = dict(ExpMoreCode=0x1, LinkState=0x9, Priority=0x1, HopCnt=0x0,
                     DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId)
        hdr_B = dict(ExpMoreCode=0x2, LinkState=0xA, Priority=0x1, HopCnt=0x0,
                     DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId)
        for hdr_opt in (hdr_A, hdr_B):
            for flag_byte in (0x00, 0x01):
                tries.append((hdr_opt, flag_byte))
    for flag_byte in (0x00, 0x01):
        tries.append(({}, flag_byte))

    last_try: Optional[Tuple[Dict[str, Any], int]] = None
    for hdr_opt, flag_byte in tries:
        last_try = (hdr_opt, flag_byte)
        blob = _try_once(hdr_opt, flag_byte)
        if blob:
            return blob

    raise RuntimeError(f"GetTableDefs failed; last_try={last_try!r}")

# ---- Convenience: get & parse defs, collect, flatten ------------------------

def ensure_tabledefs(
        s: socket.socket,
        *,
        DstNodeId: int,
        SrcNodeId: int,
        RouterPhyAddr: Optional[int] = None,
        timeout: float = 10.0,
) -> List[Dict[str, Any]]:
    raw = get_tabledefs_bmp5(s, DstNodeId=DstNodeId, SrcNodeId=SrcNodeId, RouterPhyAddr=RouterPhyAddr, timeout=timeout)
    return parse_tabledef(raw)

def _get_table_number(tabledef: List[Dict[str, Any]], table_name: str) -> Optional[int]:
    for i, t in enumerate(tabledef, start=1):
        name = t.get("Header", {}).get("TableName")
        if isinstance(name, (bytes, bytearray)):
            name = name.decode("ascii", "ignore")
        if str(name) == table_name:
            return i
    return None

def collect_data(
        s: socket.socket,
        *,
        DstNodeId: int,
        SrcNodeId: int,
        TableDef: List[Dict[str, Any]],
        TableName: str,
        FieldNames: Sequence[str] = (),
        CollectMode: int = 0x04,
        P1: Any = 1,
        P2: Any = 0,
        SecurityCode: int = 0x0000,
        RouterPhyAddr: Optional[int] = None,
        timeout: float = 20.0,
        TableDefSigOverride: Optional[int] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Send CollectData and parse response into record fragments using TableDef.

    Returns:
        (records, more) where `records` is a list of parsed record fragments and
        `more` is the device's "more data available" flag.

    Raises:
        RuntimeError on protocol / device errors (e.g., RC != 0), with rich context.
    """
    # --- Resolve table number and signature ---
    tablenbr = _get_table_number(TableDef, TableName)
    if tablenbr is None:
        raise RuntimeError(f"table {TableName!r} not found in table definition")

    try:
        if TableDefSigOverride is not None:
            tabledefsig = int(TableDefSigOverride) & 0xFFFF
        else:
            tabledefsig = int(TableDef[tablenbr - 1]["Signature"]) & 0xFFFF
    except Exception as e:
        raise RuntimeError(
            f"Unable to resolve table signature for table {TableName!r} (tablenbr={tablenbr}): {e}"
        ) from e

    # --- Optional field selection (map names -> field numbers) ---
    fieldnbr: List[int] = []
    if FieldNames:
        wanted = list(FieldNames)
        fields = TableDef[tablenbr - 1].get("Fields", [])
        for fn in range(1, len(fields) + 1):
            fieldname = fields[fn - 1].get("FieldName")
            if isinstance(fieldname, (bytes, bytearray)):
                try:
                    fieldname = fieldname.decode("ascii", "ignore")
                except Exception:
                    fieldname = repr(fieldname)
            if fieldname in wanted:
                fieldnbr.append(fn)
                try:
                    wanted.remove(fieldname)
                except ValueError:
                    pass
                if not wanted:
                    break
        if wanted:  # names requested but not found
            raise RuntimeError(
                f"Some requested FieldNames not found in {TableName!r}: {wanted}"
            )

    # --- Build CollectData command packet ---
    pkt, tran_nbr = pkt_collectdata_cmd(
        DstNodeId, SrcNodeId, tablenbr, tabledefsig,
        FieldNbr=fieldnbr,
        CollectMode=CollectMode, P1=P1, P2=P2, SecurityCode=SecurityCode
    )

    # --- Wrap in link header if routing through CR800 router ---
    if RouterPhyAddr is not None:
        hdr_bytes = pakbus_hdr(
            DstNodeId=DstNodeId, SrcNodeId=SrcNodeId, HiProtoCode=0x01,
            DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId,
            ExpMoreCode=0, LinkState=0, Priority=0, HopCnt=0,
        )
        # Replace the link-layer header section with our routed header
        pkt = hdr_bytes + pkt[8:]

    # --- Send and await response ---
    send(s, pkt)
    _hdr, raw_msg = wait_pkt(
        s,
        DstNodeId=SrcNodeId,    # responses swap src/dst
        SrcNodeId=DstNodeId,
        TranNbr=tran_nbr,
        timeout=timeout
    )
    if not raw_msg:
        # No response at all
        raise RuntimeError(
            f"CollectData timeout/no response (dst={DstNodeId}, src={SrcNodeId}, "
            f"router={RouterPhyAddr}, table={TableName!r}, mode=0x{CollectMode:02X}, "
            f"P1={P1}, P2={P2}, sig=0x{tabledefsig:04X})"
        )

    # --- Parse high-level CollectData response (extract RC, RecData, More, etc.) ---
    msg = msg_collectdata_response(raw_msg)
    rc = int(msg.get("RespCode", 0)) & 0xFF  # FIX: use RespCode

    # If device signaled an error, raise a clear, contextual exception
    if rc != 0:
        raise RuntimeError(
            f"CollectData RC=0x{rc:02X} (dst={DstNodeId}, src={SrcNodeId}, router={RouterPhyAddr}, "
            f"table={TableName!r}#{tablenbr}, sig=0x{tabledefsig:04X}, mode=0x{CollectMode:02X}, "
            f"P1={P1}, P2={P2})"
        )

    # --- Extract record payload and "more" flag ---
    recdata: bytes = msg.get("RecData", b"") or b""
    more_flag = bool(msg.get("More", False))

    # It's valid to have zero rows (e.g., empty window) with RC==0; return empty set cleanly.
    if len(recdata) == 0:
        return [], more_flag

    # --- Decode records safely, with rich error context on failure ---
    try:
        recs, more_from_payload = parse_collectdata(recdata, TableDef, FieldNbr=fieldnbr)
        # Prefer the explicit "More" flag if present; fall back to parser's.
        more = bool(more_flag or more_from_payload)
        return recs, more
    except Exception as e:
        raise RuntimeError(
            f"Failed to decode CollectData payload (len={len(recdata)} bytes) for "
            f"table={TableName!r}#{tablenbr}, sig=0x{tabledefsig:04X}, "
            f"mode=0x{CollectMode:02X}, P1={P1}, P2={P2}. Error: {e}"
        ) from e

def collect_by_time(
        s: socket.socket,
        *,
        DstNodeId: int,
        SrcNodeId: int,
        TableDef: List[Dict[str, Any]],
        TableName: str,
        BeginUnixUTC: float,
        EndUnixUTC: float,
        FieldNames: Sequence[str] = (),
        RouterPhyAddr: Optional[int] = None,
        timeout: float = 20.0,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Convenience: CollectData by time range (inclusive start, exclusive end).
    """
    p1 = time_to_nsec(BeginUnixUTC)
    p2 = time_to_nsec(EndUnixUTC)
    return collect_data(
        s,
        DstNodeId=DstNodeId, SrcNodeId=SrcNodeId,
        TableDef=TableDef, TableName=TableName,
        FieldNames=FieldNames,
        CollectMode=0x07, P1=p1, P2=p2,
        RouterPhyAddr=RouterPhyAddr, timeout=timeout,
    )

def collect_most_recent(
        s: socket.socket,
        *,
        DstNodeId: int,
        SrcNodeId: int,
        TableDef: List[Dict[str, Any]],
        TableName: str,
        Count: int = 1,
        FieldNames: Sequence[str] = (),
        RouterPhyAddr: Optional[int] = None,
        timeout: float = 20.0,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Convenience: most recent Count records from TableName.
    """
    return collect_data(
        s,
        DstNodeId=DstNodeId, SrcNodeId=SrcNodeId,
        TableDef=TableDef, TableName=TableName,
        FieldNames=FieldNames,
        CollectMode=0x04, P1=int(Count),
        RouterPhyAddr=RouterPhyAddr, timeout=timeout,
    )

def flatten_records(rec_frags: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten parsed fragments into a simple list of row dicts:
      { "TableName": ..., "RecNbr": ..., "TimeOfRec": (sec,nsec), **fields }
    """
    out: List[Dict[str, Any]] = []
    for frag in rec_frags:
        tname = frag.get("TableName")
        rows = frag.get("RecFrag", [])
        if isinstance(rows, list):
            for r in rows:
                flat = {
                    "TableName": tname,
                    "RecNbr": r.get("RecNbr"),
                    "TimeOfRec": r.get("TimeOfRec"),
                }
                flds = r.get("Fields", {})
                if isinstance(flds, dict):
                    flat.update(flds)
                out.append(flat)
    return out

# ---- File Upload (optional) ----

def msg_fileupload_response(msg: Message) -> Message:
    # [MsgType, TranNbr, RespCode, FileOffset(4), FileData...]
    raw = msg.get("raw", b"")
    if len(raw) < 7:
        msg["RespCode"] = 0x0E
        msg["RecData"] = b""
        return msg
    resp, _fileoff = raw[2], struct.unpack(">L", raw[3:7])[0]
    msg["RespCode"] = int(resp)
    msg["RecData"] = raw[7:]
    return msg

def fileupload(
    s: socket.socket,
    *,
    DstNodeId: int,
    SrcNodeId: int,
    FileName: str,
    SecurityCode: int = 0x0000,
    RouterPhyAddr: Optional[int] = None,
    timeout: float = 12.0,
) -> Tuple[bytes, int]:
    def _hdr_variants():
        if RouterPhyAddr is not None:
            yield dict(ExpMoreCode=0x1, LinkState=0x9, Priority=0x1, HopCnt=0x0,
                       DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId)
            yield dict(ExpMoreCode=0x2, LinkState=0xA, Priority=0x1, HopCnt=0x0,
                       DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId)
            yield dict(ExpMoreCode=0x0, LinkState=0x0, Priority=0x0, HopCnt=0x0,
                       DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId)
        yield dict(ExpMoreCode=0x0, LinkState=0x0, Priority=0x0, HopCnt=0x0)

    filenames: List[str] = [FileName]
    if FileName.upper().startswith("CPU:"):
        base = FileName.split(":", 1)[1]
        if base not in filenames:
            filenames.append(base)
    else:
        prefixed = f"CPU:{FileName}"
        if prefixed not in filenames:
            filenames.insert(0, prefixed)

    def _try_one(req_name: str, hdr_opt: Dict[str, Any]) -> Tuple[bytes, int]:
        file_data = bytearray()
        file_offset = 0
        while True:
            tn = new_tran_nbr()
            body = encode_bin(
                ["Byte", "Byte", "UInt2", "ASCIIZ", "UInt4", "Byte"],
                [0x0F, tn, SecurityCode, req_name.encode("ascii", "ignore"),
                 file_offset, 0x00]
            )
            hdr_bytes = pakbus_hdr(DstNodeId=DstNodeId, SrcNodeId=SrcNodeId,
                                   HiProtoCode=0x05, **hdr_opt)
            try:
                send(s, hdr_bytes + body)
                _hdr, msg = wait_pkt(s, DstNodeId=SrcNodeId, SrcNodeId=DstNodeId,
                                     TranNbr=tn, timeout=timeout)
            except (ConnectionResetError, TimeoutError):
                return bytes(file_data), 0x0E

            if not msg:
                return bytes(file_data), 0x0E

            msg = msg_fileupload_response(msg)
            resp_code = int(msg.get("RespCode", 0x0E))
            chunk = msg.get("RecData", b"")
            if resp_code == 0 and chunk:
                file_data += chunk
                file_offset += len(chunk)
                continue
            return bytes(file_data), resp_code

    last_resp = 0x0E
    for candidate in filenames:
        for hdr_opt in _hdr_variants():
            data, resp_code = _try_one(candidate, hdr_opt)
            if resp_code == 0 and data:
                return data, 0
            last_resp = resp_code
    return b"", last_resp

# alias for compatibility with old code
pkt_hello_response = msg_hello

__all__ = [
    # core framing
    "send", "recv", "pakbus_hdr", "decode_pkt", "encode_bin", "decode_bin", "wait_pkt",
    # hello + routing
    "pkt_hello_cmd", "ping_node",
    # table/collect
    "get_tabledefs_bmp5", "parse_tabledef",
    "pkt_collectdata_cmd", "msg_collectdata_response", "parse_collectdata",
    "ensure_tabledefs", "collect_data", "collect_by_time", "collect_most_recent", "flatten_records",
    # time helpers
    "nsec_to_time", "time_to_nsec", "nsec_base", "nsec_tick",
    # sockets
    "open_socket",
    # file upload
    "fileupload",
]