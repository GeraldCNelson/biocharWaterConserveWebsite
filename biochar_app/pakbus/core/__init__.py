# biochar_app/pakbus/__init__.py
"""
PakBus helpers package.

Exports:
- cd200_client_utils: Python 3.13-safe helpers for CR200/CR800
- (re-exports of commonly used symbols for convenience)
"""

# Make the submodule discoverable to "from biochar_app.pakbus import cd200_client_utils"
from . import cr200_client_utils

# Optional: re-export frequently used helpers so callers can also do
# "from biochar_app.pakbus import open_socket, collect_data, ..."
from .cr200_client_utils import (
    open_socket,
    ping_node,
    fileupload,
    collect_data,
    parse_tabledef,
    parse_collectdata,
    nsec_to_time,
    time_to_nsec,
    nsec_base,
    nsec_tick,
    send,
    recv,
    pakbus_hdr,
    decode_pkt,
    encode_bin,
    decode_bin,
    wait_pkt,
    pkt_hello_cmd,
    pkt_hello_response,
    msg_hello,
    pkt_collectdata_cmd,
    msg_collectdata_response,
)

__all__ = [
    "cr200_client_utils",
    "open_socket",
    "ping_node",
    "fileupload",
    "collect_data",
    "parse_tabledef",
    "parse_collectdata",
    "nsec_to_time",
    "time_to_nsec",
    "nsec_base",
    "nsec_tick",
    "send",
    "recv",
    "pakbus_hdr",
    "decode_pkt",
    "encode_bin",
    "decode_bin",
    "wait_pkt",
    "pkt_hello_cmd",
    "pkt_hello_response",
    "msg_hello",
    "pkt_collectdata_cmd",
    "msg_collectdata_response",
]