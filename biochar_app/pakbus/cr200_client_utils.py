# biochar_app/pakbus/cr200_client_utils.py
# Minimal, Python 3.13-safe PakBus helpers for CR200/CR800 workflows.
#
# Origin: derived from Dietrich Feist’s PyPak (GPL), trimmed and modernized.
# License: GPLv2+ (inherits the original PyPak license terms).
#
# Notes:
# - Provides framing/signature, header encode/decode, Hello, Collect Data,
#   TableDef parsing, BMP5 GetTableDefs (0x16/0x17), FileUpload, and socket helper.
# - All high-level calls accept RouterPhyAddr (int) to force routing via CR800.

from __future__ import annotations

import calendar
import logging
import math
import socket
import struct
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

log = logging.getLogger("pakbus.utils")

BytesLike = Union[bytes, bytearray, memoryview]

# ----------------------------- Public types ----------------------------------

Header = Dict[str, int]
Message = Dict[str, Any]

# ----------------------------- Data type map ---------------------------------

datatype: Dict[str, Dict[str, Any]] = {
    "Byte":   {"code": 1,  "fmt": "B",   "size": 1},
    "UInt2":  {"code": 2,  "fmt": ">H",  "size": 2},
    "UInt4":  {"code": 3,  "fmt": ">L",  "size": 4},
    "Int1":   {"code": 4,  "fmt": "b",   "size": 1},
    "Int2":   {"code": 5,  "fmt": ">h",  "size": 2},
    "Int4":   {"code": 6,  "fmt": ">l",  "size": 4},
    "FP2":    {"code": 7,  "fmt": ">H",  "size": 2},
    "FP3":    {"code": 15, "fmt": "3s",  "size": 3},
    "FP4":    {"code": 8,  "fmt": "4s",  "size": 4},
    "IEEE4B": {"code": 9,  "fmt": ">f",  "size": 4},
    "IEEE8B": {"code": 18, "fmt": ">d",  "size": 8},
    "Bool8":  {"code": 17, "fmt": "B",   "size": 1},
    "Bool":   {"code": 10, "fmt": "B",   "size": 1},
    "Bool2":  {"code": 27, "fmt": ">H",  "size": 2},
    "Bool4":  {"code": 28, "fmt": ">L",  "size": 4},
    "Sec":    {"code": 12, "fmt": ">l",  "size": 4},
    "USec":   {"code": 13, "fmt": "6s",  "size": 6},
    "NSec":   {"code": 14, "fmt": ">2l", "size": 8},
    "ASCII":  {"code": 11, "fmt": "s",   "size": None},
    "ASCIIZ": {"code": 16, "fmt": "s",   "size": None},
    "Short":  {"code": 19, "fmt": "<h",  "size": 2},
    "Long":   {"code": 20, "fmt": "<l",  "size": 4},
    "UShort": {"code": 21, "fmt": "<H",  "size": 2},
    "ULong":  {"code": 22, "fmt": "<L",  "size": 4},
    "IEEE4L": {"code": 24, "fmt": "<f",  "size": 4},
    "IEEE8L": {"code": 25, "fmt": "<d",  "size": 8},
    "SecNano":{"code": 23, "fmt": "<2l", "size": 8},
}

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
    b = sock.recv(1)
    if not b:
        raise TimeoutError("Socket closed while reading.")
    return b[0]

def recv(sock: socket.socket) -> Optional[bytes]:
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
    offset = 0
    values: List[Any] = []
    data = bytes(buff)

    for Type in Types:
        spec = datatype[Type]
        fmt: str = spec["fmt"]
        size = spec["size"]

        if Type == "ASCIIZ":
            nul = data.find(b"\x00", offset)
            if nul < 0:
                raise ValueError("ASCIIZ: missing terminator")
            value: Any = data[offset:nul]
            used = (nul - offset) + 1
        elif Type == "ASCII":
            used = int(length)
            value = data[offset:offset + used]
        elif Type == "FP2":
            used = 2
            (raw,) = struct.unpack_from(fmt, data, offset)
            mant = raw & 0x1FFF
            exp = (raw >> 13) & 0x3
            sign = (raw >> 15) & 0x1
            value = (-1) ** sign * float(mant) / (10 ** exp)
        elif Type == "NSec":
            used = 8
            s1, s2 = struct.unpack_from(fmt, data, offset)
            value = (int(s1), int(s2))
        else:
            if size is None:
                raise ValueError(f"Unhandled variable-sized type {Type}")
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
            raise NotImplementedError("Encoding FP2 not required for our use")
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
    hdr = pakbus_hdr(DstNodeId, SrcNodeId, 0x0)
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
    end_by = time.time() + max(0.1, timeout)
    last_hdr: Header = {}
    last_msg: Message = {}
    s.settimeout(max(0.1, timeout))
    while time.time() < end_by:
        try:
            rcv = recv(s)
        except socket.timeout:
            continue
        if not rcv:
            continue
        hdr, msg = decode_pkt(rcv)
        last_hdr, last_msg = hdr, msg
        # match the directed pair (Dst<-Src) and transaction
        if hdr.get("DstNodeId") == DstNodeId and hdr.get("SrcNodeId") == SrcNodeId and msg.get("TranNbr") == TranNbr:
            return hdr, msg
    return last_hdr, last_msg

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
        # replace header to route via CR800 (DstPhy=router)
        hdr = pakbus_hdr(
            DstNodeId=DstNodeId, SrcNodeId=SrcNodeId, HiProtoCode=0x0,
            DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId,
            ExpMoreCode=0, LinkState=0, Priority=0, HopCnt=0,
        )
        pkt = hdr + pkt[8:]
    send(s, pkt)
    _hdr, msg = wait_pkt(s, DstNodeId=SrcNodeId, SrcNodeId=DstNodeId, TranNbr=tn, timeout=timeout)
    return msg

# ---------------------- BMP5: Collect Data & Table Defs ----------------------

def pkt_collectdata_cmd(
    DstNodeId: int,
    SrcNodeId: int,
    TableNbr: int,
    TableDefSig: int,
    FieldNbr: Sequence[int] = (),
    CollectMode: int = 0x05,
    P1: int | Tuple[int, int] = 0,
    P2: int | Tuple[int, int] = 0,
    SecurityCode: int = 0x0000,
) -> Tuple[bytes, int]:
    tn = new_tran_nbr()
    hdr = pakbus_hdr(DstNodeId, SrcNodeId, 0x1)
    msg = encode_bin(["Byte", "Byte", "UInt2", "Byte"], [0x09, tn, SecurityCode, CollectMode])
    msg += encode_bin(["UInt2", "UInt2"], [TableNbr, TableDefSig])

    if CollectMode in (0x04, 0x05):
        msg += encode_bin(["UInt4"], [int(P1)])  # last N or from record
    elif CollectMode in (0x06, 0x08):
        msg += encode_bin(["UInt4", "UInt4"], [int(P1), int(P2)])
    elif CollectMode == 0x07:
        # time range
        p1 = P1 if isinstance(P1, tuple) else (0, 0)
        p2 = P2 if isinstance(P2, tuple) else (0, 0)
        msg += encode_bin(["NSec", "NSec"], [p1, p2])

    fieldlist = list(FieldNbr) + [0]
    msg += encode_bin(["UInt2"] * len(fieldlist), fieldlist)
    return hdr + msg, tn

def msg_collectdata_response(msg: Message) -> Message:
    off = 2
    (resp,), used = decode_bin(["Byte"], msg["raw"][off:])  # type: ignore[index]
    off += used
    msg["RespCode"] = int(resp)
    msg["RecData"] = msg["raw"][off:]  # type: ignore[index]
    return msg

def parse_tabledef(raw: BytesLike) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    data = bytes(raw)
    offset = 0
    _, used = decode_bin(["Byte"], data[offset:])
    offset += used

    while offset < len(data):
        tblhdr: Dict[str, Any] = {}
        fields: List[Dict[str, Any]] = []
        start = offset

        vals, used = decode_bin(["ASCIIZ", "UInt4", "Byte", "NSec", "NSec"], data[offset:])
        offset += used
        tblhdr["TableName"], tblhdr["TableSize"], tblhdr["TimeType"], tblhdr["TblTimeInto"], tblhdr["TblInterval"] = vals

        while True:
            (fieldtype,), used = decode_bin(["Byte"], data[offset:])
            offset += used
            if fieldtype == 0:
                break

            fld: Dict[str, Any] = {"ReadOnly": int(fieldtype) >> 7}
            ft_code = int(fieldtype) & 0x7F
            ft_name: Any = next((name for name, spec in datatype.items() if spec["code"] == ft_code), ft_code)
            fld["FieldType"] = ft_name

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

            vals, used = decode_bin(["ASCIIZ", "ASCIIZ", "ASCIIZ", "UInt4", "UInt4"], data[offset:])
            offset += used
            fld["Processing"], fld["Units"], fld["Description"], fld["BegIdx"], fld["Dimension"] = vals

            subdims: List[int] = []
            while True:
                (sd,), used = decode_bin(["UInt4"], data[offset:])
                offset += used
                if sd == 0:
                    break
                subdims.append(int(sd))
            fld["SubDim"] = subdims

            fields.append(fld)

        tblsig = calc_sig_for(data[start:offset])
        out.append({"Header": tblhdr, "Fields": fields, "Signature": int(tblsig)})

    return out

def parse_collectdata(raw: BytesLike, tabledef: List[Dict[str, Any]], FieldNbr: Sequence[int] = ()) -> Tuple[List[Dict[str, Any]], bool]:
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
        frag["TableName"] = tabledef[table_nbr - 1]["Header"]["TableName"]

        (isoffset,), used = decode_bin(["Byte"], data[offset:])
        frag["IsOffset"] = int(isoffset) >> 7

        if frag["IsOffset"]:
            (byteoffset,), used = decode_bin(["UInt4"], data[offset:])
            offset += used
            frag["ByteOffset"] = int(byteoffset) & 0x7FFFFFFF
            frag["NbrOfRecs"] = None
            frag["RecFrag"] = data[offset:-1]
            offset = len(data) - 1
        else:
            (nbrecs,), used = decode_bin(["UInt2"], data[offset:])
            offset += used
            frag["NbrOfRecs"] = int(nbrecs) & 0x7FFF
            frag["ByteOffset"] = None

            interval = tabledef[table_nbr - 1]["Header"]["TblInterval"]
            if interval == (0, 0):
                timeofrec: Optional[Tuple[int, int]] = None
            else:
                (timeofrec,), used = decode_bin(["NSec"], data[offset:])
                offset += used

            frag["RecFrag"] = []
            fields = list(FieldNbr) if FieldNbr else list(range(1, len(tabledef[table_nbr - 1]["Fields"]) + 1))

            for n in range(frag["NbrOfRecs"]):  # type: ignore[arg-type]
                record: Dict[str, Any] = {"RecNbr": beg_rec + n}

                if timeofrec:
                    interval_s, interval_n = interval
                    record["TimeOfRec"] = (timeofrec[0] + n * int(interval_s), timeofrec[1] + n * int(interval_n))
                else:
                    (tor,), used = decode_bin(["NSec"], data[offset:])
                    offset += used
                    record["TimeOfRec"] = tor

                values_map: Dict[Any, Any] = {}
                for field in fields:
                    meta = tabledef[table_nbr - 1]["Fields"][field - 1]
                    fname = meta["FieldName"].decode("ascii", "ignore") if isinstance(meta["FieldName"], (bytes, bytearray)) else meta["FieldName"]
                    ftype = meta["FieldType"]
                    dim = int(meta["Dimension"])
                    if ftype == "ASCII":
                        (val,), used = decode_bin([ftype], data[offset:], length=dim)
                    else:
                        vals2, used = decode_bin([ftype] * dim, data[offset:])
                        val = vals2 if dim > 1 else vals2[0]
                    offset += used
                    values_map[fname] = val
                record["Fields"] = values_map
                frag["RecFrag"].append(record)

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

def open_socket(host: str, Port: int = 6785, Timeout: float = 30.0) -> Optional[socket.socket]:
    try:
        infos = socket.getaddrinfo(host, Port, socket.AF_UNSPEC, socket.SOCK_STREAM)
    except socket.gaierror:
        return None

    for af, socktype, proto, _canon, sa in infos:
        s = None
        try:
            s = socket.socket(af, socktype, proto)
            s.settimeout(Timeout)
            s.connect(sa)
            return s
        except Exception:
            if s:
                try:
                    s.close()
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
    """
    BMP5 'GetTableDefs' for CR200/CR800 networks.

    Handshake-first strategy:
      1) If RouterPhyAddr is given, send a routed Hello (leaf <-> us) to
         ensure the CR800 has an open/known route for the leaf.
      2) Try two header flavors (the ones proven by your Hello tests),
         and both flags (0x00, 0x01) for the 0x16 request.
      3) Expect MsgType 0x17 response: [0x17, TranNbr, RespCode, <blob...>].
    """
    # --- Step 0: polite Hello first (routed when router phy is provided) ---
    try:
        # ping_node already supports RouterPhyAddr; it builds the routed header for Hello.
        _hello = ping_node(
            s,
            DstNodeId=DstNodeId,
            SrcNodeId=SrcNodeId,
            RouterPhyAddr=RouterPhyAddr,
        )
        # no strict check; some firmwares respond minimally
    except Exception:
        # If Hello fails we still try GetTableDefs; some devices are quirky
        pass

    def _try_once(hdr_kwargs: Dict[str, Any], flags: int) -> Optional[bytes]:
        tn = new_tran_nbr()
        # MsgType 0x16 = GetTableDefs
        payload = encode_bin(["Byte", "Byte", "Byte"], [0x16, tn, flags])
        hdr = pakbus_hdr(
            DstNodeId=DstNodeId,
            SrcNodeId=SrcNodeId,
            HiProtoCode=0x00,  # PakCtrl / BMP shell
            **hdr_kwargs,
        )
        send(s, hdr + payload)
        _hdr, msg = wait_pkt(
            s,
            DstNodeId=SrcNodeId,
            SrcNodeId=DstNodeId,
            TranNbr=tn,
            timeout=timeout,
        )
        if not msg:
            return None
        if msg.get("MsgType") != 0x17:
            return None
        raw = msg.get("raw", b"")
        if len(raw) < 3:
            return None
        resp_code = raw[2]
        blob = raw[3:]
        if resp_code == 0 and blob:
            return blob
        return None

    tries: List[Tuple[Dict[str, Any], int]] = []

    # Routed header flavors (these matched your Hello traces)
    if RouterPhyAddr is not None:
        hdr_B = dict(
            ExpMoreCode=0x1,
            LinkState=0x9,
            Priority=0x1,
            HopCnt=0x0,
            DstPhyAddr=RouterPhyAddr,
            SrcPhyAddr=SrcNodeId,
        )
        hdr_C = dict(
            ExpMoreCode=0x2,
            LinkState=0xA,
            Priority=0x1,
            HopCnt=0x0,
            DstPhyAddr=RouterPhyAddr,
            SrcPhyAddr=SrcNodeId,
        )
        for hdr in (hdr_B, hdr_C):
            for flg in (0x00, 0x01):
                tries.append((hdr, flg))

    # Fallback: direct physical addressing (some setups allow this)
    hdr_direct: Dict[str, Any] = {}
    for flg in (0x00, 0x01):
        tries.append((hdr_direct, flg))

    last = None
    for hdr_kwargs, flags in tries:
        last = (hdr_kwargs, flags)
        try:
            blob = _try_once(hdr_kwargs, flags)
        except ConnectionResetError:
            # Peer reset; bail out early (caller can reopen socket)
            raise
        if blob:
            return blob

    raise RuntimeError(f"GetTableDefs failed: last_try={last!r}")

# ---- Collect data convenience (routed) ----

def _get_table_number(tabledef: List[Dict[str, Any]], table_name: str) -> Optional[int]:
    for i, t in enumerate(tabledef, start=1):
        name = t.get("Header", {}).get("TableName")
        if isinstance(name, (bytes, bytearray)):
            name = name.decode("ascii", "ignore")
        if name == table_name:
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
    CollectMode: int = 0x05,
    P1: Any = 1,
    P2: Any = 0,
    SecurityCode: int = 0x0000,
    RouterPhyAddr: Optional[int] = None,
    timeout: float = 20.0,
) -> Tuple[List[Dict[str, Any]], bool]:
    tablenbr = _get_table_number(TableDef, TableName)
    if tablenbr is None:
        raise RuntimeError(f"table {TableName!r} not found in table definition")
    tabledefsig = int(TableDef[tablenbr - 1]["Signature"])

    # map names -> field numbers (optional)
    fieldnbr: List[int] = []
    if FieldNames:
        wanted = list(FieldNames)
        for fn in range(1, len(TableDef[tablenbr - 1]["Fields"]) + 1):
            fieldname = TableDef[tablenbr - 1]["Fields"][fn - 1]["FieldName"]
            if isinstance(fieldname, (bytes, bytearray)):
                fieldname = fieldname.decode("ascii", "ignore")
            try:
                idx = wanted.index(fieldname)
            except ValueError:
                continue
            fieldnbr.append(fn)
            del wanted[idx]
            if not wanted:
                break

    pkt, tn = pkt_collectdata_cmd(
        DstNodeId, SrcNodeId, tablenbr, tabledefsig,
        FieldNbr=fieldnbr, CollectMode=CollectMode, P1=P1, P2=P2, SecurityCode=SecurityCode
    )

    # Route via router if provided (swap header only)
    if RouterPhyAddr is not None:
        hdr = pakbus_hdr(
            DstNodeId=DstNodeId, SrcNodeId=SrcNodeId, HiProtoCode=0x01,
            DstPhyAddr=RouterPhyAddr, SrcPhyAddr=SrcNodeId,
            ExpMoreCode=0, LinkState=0, Priority=0, HopCnt=0,
        )
        pkt = hdr + pkt[8:]

    send(s, pkt)
    _hdr, msg = wait_pkt(s, DstNodeId=SrcNodeId, SrcNodeId=DstNodeId, TranNbr=tn, timeout=timeout)
    if not msg:
        return [], False
    msg = msg_collectdata_response(msg)
    recs, more = parse_collectdata(msg["RecData"], TableDef, FieldNbr=fieldnbr)
    return recs, more

# ---- File Upload (routed) ----

def msg_fileupload_response(msg: Message) -> Message:
    # [MsgType, TranNbr, RespCode, FileOffset(4), FileData...]
    raw = msg.get("raw", b"")
    if len(raw) < 7:
        msg["RespCode"] = 0x0E  # general error
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
    timeout: float = 10.0,
) -> Tuple[bytes, int]:
    """
    Read a whole file from the logger. Returns (data, RespCode).
    """
    file_data = bytearray()
    file_offset = 0
    tn: Optional[int] = None

    while True:
        # Build BMP5 FileUpload request (0x0F)
        if tn is None:
            tn = new_tran_nbr()
        else:
            tn = new_tran_nbr()

        name_b = FileName.encode("ascii", "ignore")
        body = encode_bin(
            ["Byte", "Byte", "UInt2", "ASCIIZ", "UInt4", "Byte"],
            [0x0F, tn, SecurityCode, name_b, file_offset, 0x00],  # keep open
        )
        hdr = pakbus_hdr(
            DstNodeId=DstNodeId, SrcNodeId=SrcNodeId, HiProtoCode=0x05,
            DstPhyAddr=(RouterPhyAddr if RouterPhyAddr is not None else DstNodeId),
            SrcPhyAddr=SrcNodeId,
            ExpMoreCode=0, LinkState=0, Priority=0, HopCnt=0,
        )
        send(s, hdr + body)

        _hdr, msg = wait_pkt(s, DstNodeId=SrcNodeId, SrcNodeId=DstNodeId, TranNbr=tn, timeout=timeout)
        if not msg:
            return bytes(file_data), 0x0E
        msg = msg_fileupload_response(msg)
        rc = int(msg.get("RespCode", 0x0E))
        chunk = msg.get("RecData", b"")
        if rc == 0 and chunk:
            file_data += chunk
            file_offset += len(chunk)
            continue
        return bytes(file_data), rc

# alias for compatibility with old code
pkt_hello_response = msg_hello

__all__ = [
    # core framing
    "send", "recv", "pakbus_hdr", "decode_pkt", "encode_bin", "decode_bin", "wait_pkt",
    # hello + routing
    "pkt_hello_cmd", "ping_node",
    # table/collect
    "pkt_collectdata_cmd", "msg_collectdata_response", "parse_tabledef", "parse_collectdata",
    # time helpers
    "nsec_to_time", "time_to_nsec", "nsec_base", "nsec_tick",
    # sockets
    "open_socket",
    # new BMP5 GetTableDefs + routed collect/file
    "get_tabledefs_bmp5", "collect_data", "fileupload",
]