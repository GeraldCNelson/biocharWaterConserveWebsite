import socket, struct, time, binascii

HOST = "2605:59C0:30F3:2500:2D0:2CFF:FE02:1DDD"
PORT = 6785

def frame(payload: bytes) -> bytes:
    return b"\xBD" + payload + b"\xBD"

def build_inner(dst=1, src=15, tran=0x42, table=6, pbytes=b""):
    # Minimal PakBus frame header (no authentication, no link layer extras):
    # | Dest(2) | Src(2) | Prot(1=0xDB?) | Datalen(1) | Port(1=0) | Spare(1=0) |  Payload...
    # In your capture, the “inner” already had 8 bytes of header before 0x09.
    # We'll mirror that structure: 2+2+1+1+1+1 = 8
    hdr = struct.pack(">HHBBBB", dst, src, 0xDB, 0, 0, 0)  # datalen will be ignored by many loggers
    payload = bytes([0x09, tran, 0x00]) + struct.pack(">H", table) + pbytes
    # Some loggers ignore the length field; keeping it 0 is fine given your replay worked without fixing it.
    return hdr + payload

def send_recv(inner: bytes, wait_s=4.0):
    s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    s.settimeout(10)
    s.connect((HOST, PORT, 0, 0))
    s.sendall(frame(inner))
    s.settimeout(0.5)
    end = time.time() + wait_s
    buf = bytearray()
    while time.time() < end:
        try:
            chunk = s.recv(4096)
            if not chunk: break
            buf.extend(chunk)
        except socket.timeout:
            pass
    s.close()
    return bytes(buf)

def deframe_all(buf: bytes):
    frames, cur, in_frame = [], bytearray(), False
    for b in buf:
        if b == 0xBD:
            if in_frame and cur: frames.append(bytes(cur))
            cur.clear(); in_frame = not in_frame
            continue
        if in_frame: cur.append(b)
    return frames

def main():
    # “latest 1” guess for P:
    P = bytes.fromhex("00 02 2C 79  00 00 FF FF  00 00 FF FF  00 00 00 01")
    inner = build_inner(dst=1, src=15, tran=0x55, table=6, pbytes=P)
    rx = send_recv(inner, wait_s=6.0)
    frames = deframe_all(rx)
    print("received bytes:", len(rx), "framed messages:", len(frames))
    # find a 0x89 with matching tran=0x55
    best = None
    for fr in frames:
        payload = fr[8:] if len(fr) > 8 else b""
        if not payload or payload[0] != 0x89:
            continue
        tran = payload[1] if len(payload) > 1 else None
        rc   = payload[2] if len(payload) > 2 else None
        data = payload[3:] if len(payload) > 3 else b""
        if tran == 0x55:
            best = (rc, data); break
        if best is None:
            best = (rc, data)
    if not best:
        print("No 0x89 response found")
        return
    rc, data = best
    print("rc=", rc, "data_len=", len(data))
    if rc != 0 or not data:
        print("router cache returned empty/nonzero rc")
        return
    print("data head:", binascii.hexlify(data[:64]).decode())

if __name__ == "__main__":
    main()