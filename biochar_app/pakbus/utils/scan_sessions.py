from pathlib import Path
import re, binascii

ROOT = Path("biochar_app/pakbus/pakbus_data")
OUT  = ROOT / "hello_candidates.txt"   # save inside pakbus_data

def ascii_hex_to_bytes(text: str) -> bytes:
    s = re.sub(r"[^0-9A-Fa-f]", "", text)
    if len(s) % 2: s = s[:-1]
    return bytes(int(s[i:i+2], 16) for i in range(0, len(s), 2))

def load_raw_frames(raw_path: Path) -> list[bytes]:
    """BD-deframe a *_raw.hex capture into a list of payloads."""
    raw = ascii_hex_to_bytes(raw_path.read_text())
    frames, buf, in_frame = [], bytearray(), False
    for b in raw:
        if b == 0xBD:
            if in_frame and buf: frames.append(bytes(buf))
            buf.clear(); in_frame = not in_frame
        elif in_frame:
            buf.append(b)
    return frames

def load_frames_folder(dir_path: Path) -> list[bytes]:
    """Treat each frame_*.hex file as one deframed frame (payload bytes)."""
    frames = []
    for f in sorted(dir_path.glob("frame_*.hex")):
        frames.append(ascii_hex_to_bytes(f.read_text()))
    return frames

def find_pre_hellos(frames: list[bytes]) -> list[bytes]:
    """Return the contiguous frames immediately before the first 0x09."""
    for i, fr in enumerate(frames):
        if fr and fr[0] == 0x09:
            # walk backwards collecting AF/16/0F etc.
            j, picked = i - 1, []
            while j >= 0 and frames[j] and frames[j][0] in (0xAF, 0x16, 0x0F):
                picked.insert(0, frames[j]); j -= 1
            return picked
    return []

def opset(frames: list[bytes]): return {fr[0] for fr in frames if fr}

def hexstr(b: bytes) -> str: return binascii.hexlify(b).decode()

def main():
    goods, maybes = [], []
    lines = []

    # 1) Scan *_raw.hex
    for f in ROOT.rglob("*_raw.hex"):
        try:
            frames = load_raw_frames(f)
            if not frames: continue
            ops = opset(frames)
            if 0x09 in ops and 0x89 in ops:
                goods.append((f, len(frames)))
                pre = find_pre_hellos(frames)
                if pre:
                    for idx, fr in enumerate(pre):
                        lines.append(f"{f} pre#{idx} op=0x{fr[0]:02X} hex={hexstr(fr)}")
                else:
                    # fallback: first two AFs if present
                    afs = [fr for fr in frames if fr and fr[0] == 0xAF][:2]
                    for idx, fr in enumerate(afs):
                        lines.append(f"{f} pre#{idx} op=0x{fr[0]:02X} hex={hexstr(fr)}")
            elif len(frames) > 3:
                maybes.append((f, len(frames)))
        except Exception as e:
            print(f"ERROR(raw) {f}: {e}")

    # 2) Scan *_frames folders
    for d in ROOT.rglob("*_frames"):
        if not d.is_dir(): continue
        try:
            frames = load_frames_folder(d)
            if not frames: continue
            ops = opset(frames)
            if 0x09 in ops and 0x89 in ops:
                goods.append((d, len(frames)))
                pre = find_pre_hellos(frames)
                if pre:
                    # pick the last one before 0x09 (usually the actual hello)
                    fr = pre[-1]
                    lines.append(f"{d}/frames pre op=0x{fr[0]:02X} hex={hexstr(fr)}")
                else:
                    # fallback: take first frame as a candidate
                    fr = frames[0]
                    lines.append(f"{d}/frames pre(op?) op=0x{fr[0]:02X} hex={hexstr(fr)}")
            elif len(frames) > 3:
                maybes.append((d, len(frames)))
        except Exception as e:
            print(f"ERROR(frames) {d}: {e}")

    OUT.write_text("\n".join(lines), encoding="utf-8")

    print("--- Summary ---")
    print(f"GOOD sessions : {len(goods)}")
    for p, n in goods[:12]:  # print first few
        print(f"  GOOD  {p}  ({n} frames)")
    print(f"MAYBE sessions: {len(maybes)}")
    print(f"Candidates saved to: {OUT}")
    if lines:
        # Also print the first candidate hex so you can copy it quickly
        first = lines[0].split("hex=", 1)[1]
        print("\nFirst candidate hex:\n" + first)

if __name__ == "__main__":
    main()