#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
공통 유틸리티 모듈
- S3 client / decode
- 문자열·경로 헬퍼
- 시간창 폴더 선택 / 빈 디렉터리 정리
- 팔레트 헬퍼
- 헤더 파싱
- 폰트 캐시 / PNG 원자적 저장
"""

import os, re, json, io, zipfile, gzip, tarfile, subprocess, shutil, importlib, tempfile, multiprocessing
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

import boto3
from botocore.config import Config
from PIL import ImageFont
from tqdm import tqdm


# =================== S3 ===================

def create_s3_client(cfg):
    """PipelineConfig / BucketBConfig 둘 다 사용 가능."""
    return boto3.session.Session().client(
        "s3",
        region_name=cfg.region_name or None,
        aws_access_key_id=cfg.aws_access_key_id or None,
        aws_secret_access_key=cfg.aws_secret_access_key or None,
        endpoint_url=cfg.endpoint_url or None,
        config=Config(
            max_pool_connections=cfg.max_pool_connections,
            retries={'max_attempts': 8, 'mode': 'adaptive'},
        ),
        use_ssl=False,
    )


def decode_best_effort(b: bytes) -> str:
    for enc in ("utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return b.decode(enc)
        except:
            pass
    return b.decode("utf-8", errors="ignore")


# =================== String / Path helpers ===================

def split_key_and_inner(name: str):
    s = str(name or "")
    return s.split("::", 1)[0]


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "", (s or "NA").strip()) or "NA"


def safe_prefix(*parts: str) -> str:
    return "/".join(re.sub(r"[^A-Za-z0-9._-]+", "", str(p)) for p in parts if str(p).strip())


# =================== Time window / Cleanup ===================

def select_folders_by_window(folders, a, b):
    """top-level 폴더명(YYMMDD/YYYYMMDD)을 시간창에 해당하는 날짜만 선택"""
    if b < a:
        a, b = b, a
    want = {(a + timedelta(days=i)).strftime("%y%m%d") for i in range((b.date() - a.date()).days + 1)}
    want |= {w if len(w) == 8 else "20" + w for w in want}
    return [f for f in folders if f.rstrip("/").split("/")[-1] in want]


def cleanup_empty_p2_and_dates(base_root: str, p1_set: set) -> int:
    removed = 0
    for p1 in sorted(p1_set):
        p1_dir = os.path.join(base_root, safe_prefix(p1))
        if not os.path.isdir(p1_dir):
            continue
        for p2 in sorted(os.listdir(p1_dir)):
            p2_dir = os.path.join(p1_dir, p2)
            if not os.path.isdir(p2_dir):
                continue
            for day in sorted(os.listdir(p2_dir)):
                day_dir = os.path.join(p2_dir, day)
                if os.path.isdir(day_dir):
                    try:
                        if not os.listdir(day_dir):
                            os.rmdir(day_dir)
                            removed += 1
                    except:
                        pass
            try:
                if not os.listdir(p2_dir):
                    os.rmdir(p2_dir)
                    removed += 1
            except:
                pass
    return removed


# =================== Palette helpers ===================

def hex_to_rgb(hex_code: str):
    s = (hex_code or "").strip()
    if not (s.startswith("#") and len(s) == 7):
        return [0, 0, 0]
    return [int(s[1:3], 16), int(s[3:5], 16), int(s[5:7], 16)]


def flatten_palette_by_keys(color_map, index_to_key, total_colors=32):
    rgb = []
    for key in index_to_key:
        rgb.extend(hex_to_rgb(color_map.get(key, "#000000")))
    need = total_colors * 3 - len(rgb)
    if need > 0:
        rgb.extend([0] * need)
    elif need < 0:
        raise ValueError("palette key overflow (PALETTE_INDEX_TO_KEY > total_colors)")
    return rgb


def build_palette(color_json_path):
    """color-legends.json → 팔레트 전역 데이터 빌드."""
    with open(color_json_path, 'r', encoding='utf-8') as f:
        _cd = json.load(f)

    _d   = _cd['default']
    _top = _d['top']
    _btm = _d.get('bottom', {})

    palette_hex_map = {
        "chip0": _top.get("Grade0", "#FFFFFF"),
        "chip1": _top.get("Grade1", "#9B9B9B"),
        "chip2": _top.get("Grade2", "#009619"),
        "chip3": _top.get("Grade3", "#0000FF"),
        "chip4": _top.get("Grade4", "#D91DFF"),
        "chip5": _top.get("Grade5", "#FFFF00"),
        "chip6": _top.get("Grade6", "#FF0000"),
        "chip7": _top.get("Grade7", "#000000"),
        "bg":   _d.get("background", "#FEFEFE"),
        "text": _d.get("text", "#000001"),
        "border":     _btm.get("Normal",  "#BEBEBE"),
        "border_inv": _btm.get("Invalid", "#FF9900"),
        "border_b285": _btm.get("B285", "#0099FF"),
        "border_b286": _btm.get("B286", "#FF714F"),
        "border_b287": _btm.get("B287", "#66FFCC"),
        "border_b288": _btm.get("B288", "#DA26CD"),
        "border_b290": _btm.get("B290", "#FFD700"),
        "border_b291": _btm.get("B291", "#32CD32"),
        "border_b300": _btm.get("B300", "#AAAAAA"),
        "border_b385": _btm.get("B385", "#00C8FF"),
        "border_b386": _btm.get("B386", "#FF00C8"),
        "border_b388": _btm.get("B388", "#00FF66"),
        "border_b389": _btm.get("B389", "#FF6666"),
        "border_b390": _btm.get("B390", "#6666FF"),
    }

    palette_index_to_key = [
        "chip0","chip1","chip2","chip3","chip4","chip5","chip6","chip7",
        "bg","text",
        "border","border_inv",
        "border_b285","border_b286","border_b287","border_b288","border_b290","border_b291",
        "border_b300","border_b385","border_b386","border_b388","border_b389","border_b390",
    ]

    key_to_index = {k: i for i, k in enumerate(palette_index_to_key)}

    idx_invalid_fill = 31
    idx_bg         = key_to_index["bg"]
    idx_text       = key_to_index["text"]
    idx_border     = key_to_index["border"]
    idx_border_inv = key_to_index["border_inv"]

    idx_b_def_00p = {
        "285": key_to_index["border_b285"],
        "286": key_to_index["border_b286"],
        "287": key_to_index["border_b287"],
        "288": key_to_index["border_b288"],
        "290": key_to_index["border_b290"],
        "291": key_to_index["border_b291"],
    }
    idx_b_def_00c = {
        "300": key_to_index["border_b300"],
        "385": key_to_index["border_b385"],
        "386": key_to_index["border_b386"],
        "388": key_to_index["border_b388"],
        "389": key_to_index["border_b389"],
        "390": key_to_index["border_b390"],
    }

    palette_32 = flatten_palette_by_keys(palette_hex_map, palette_index_to_key, 32)
    palette_32[idx_invalid_fill * 3:idx_invalid_fill * 3 + 3] = hex_to_rgb("#FFFFFF")

    return (palette_hex_map, palette_index_to_key, key_to_index,
            idx_invalid_fill, idx_bg, idx_text, idx_border, idx_border_inv,
            idx_b_def_00p, idx_b_def_00c, palette_32)


# =================== Header parsing ===================

def hval(lines, key, max_lines=200):
    for ln in lines[:max_lines]:
        if ln.startswith(key):
            return ln.split("=", 1)[1].strip()
    return ""


def parse_stime(lines):
    """10번째 줄 :STIME=YYYY/MM/DD HH:MM:SS → YYYYMMDD_HHMMSS"""
    stime = "NA"
    if len(lines) > 9 and lines[9].startswith(':STIME='):
        raw = lines[9].split('=', 1)[1].strip()
        m = re.match(r'(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', raw)
        stime = (f"{m.group(1)}{m.group(2)}{m.group(3)}_{m.group(4)}{m.group(5)}{m.group(6)}"
                 if m else raw.replace('/', '').replace(':', '').replace(' ', '_'))
    return stime


def parse_stime_dt(st):
    if not st:
        return None
    if not re.match(r'^(\d{8})_(\d{6})$', st):
        return None
    try:
        return datetime.strptime(st, "%Y%m%d_%H%M%S")
    except:
        return None


# =================== File parsing ===================

def find_initial_values_from_lines(lines, file_name):
    """파일 포맷에서 x/y size, 데이터 시작 라인 등을 찾아 타일 크기를 결정"""
    wfid = lines[1].split('=')[1].strip()
    base = os.path.basename((file_name or "").split("::")[-1])
    root = (base.split('-', 1)[0] if '-' in base else base).upper()
    step, wafer = wfid.split('-',1)[1].split('.',1)

    xsize = int(lines[11].split('=')[1])
    ysize = int(lines[12].split('=')[1])

    rot = 5
    if len(lines) > 8 and '=' in lines[8]:
        try:
            rot = int(lines[8].split('=',1)[1].strip())
        except:
            rot = 5

    start, line_offset = None, 1
    for i in range(28, 40):
        if i < len(lines) and lines[i].startswith('X='):
            start = i
            if i+1 < len(lines) and lines[i+1].startswith('mft'):
                line_offset = 2
            break
    if start is None:
        raise ValueError("X= start not found")

    for i in range(start, min(start+10, len(lines))):
        if lines[i].startswith('#'):
            if i + xsize < len(lines) and lines[i+xsize].startswith('X='):
                xsize, ysize = ysize, xsize
            else:
                xsize = int((len(lines[i]) - 1)//2)
                start = i - line_offset
                for j in range(i, min(i+1000, len(lines))):
                    if lines[j].startswith('X='):
                        ysize = j - i
                        break
            break

    return xsize, ysize, start, root, step, wafer, rot


def process_file_content(args):
    """워커에서 실행: 텍스트 1개를 파싱해 chip sample list를 반환."""
    file_name, file_content = args
    if not file_content:
        return []
    lines = file_content.splitlines()
    if not lines:
        return []

    stime  = parse_stime(lines)
    partid = hval(lines, ":PARTID=")
    tester = hval(lines, ":TESTER=")
    device = hval(lines, ":DEVICE=")
    pgm    = hval(lines, ":PGM=")
    netd_raw = hval(lines, ":NETD=")
    try:
        netd = int(re.sub(r'\D', '', netd_raw)) if netd_raw else 0
    except:
        netd = 0

    try:
        xsize, ysize, start, root, step, wafer, rot = find_initial_values_from_lines(lines, file_name)
    except:
        return []

    convert_hex_values = get_cython_convert_hex()
    last_by_xy = {}

    i = start
    while i < len(lines):
        if not lines[i].startswith('X='):
            i += 1
            continue

        m = dict(re.findall(r'([XYbB])\s*=\s*([-\w]+)', lines[i].strip()))
        try:
            cx = int(m.get('X', '0')); cy = int(m.get('Y', '0'))
        except:
            parts = lines[i].split()
            cx = int(parts[1]) if len(parts) > 1 else 0
            cy = int(parts[3]) if len(parts) > 3 else 0

        cb = (m.get('b') or m.get('B') or '').strip()

        j = i + 1
        if j < len(lines) and lines[j].startswith('mft'):
            j += 1

        hex_block = ""
        if j < len(lines) and lines[j].startswith('#'):
            try:
                hex_block = convert_hex_values(lines, j, xsize, ysize)
            except:
                hex_block = ""

        orig_key = split_key_and_inner(file_name)
        last_by_xy[(cx, cy)] = {
            "root": root, "step": step, "wafer": wafer,
            "x": cx, "y": cy, "b": cb,
            "transformed_values": hex_block,
            "stime": stime,
            "rot": rot,
            "partid": partid, "tester": tester, "device": device, "pgm": pgm,
            "netd": netd, "orig_key": orig_key
        }
        i += 1

    return list(last_by_xy.values())


# =================== Token → pair ===================

def choose_pair_by_device(token, device_value, token2pps):
    # LOT ID '1' 시작: :DEVICE= 끝에서 3번째,2번째 글자 → p1, "P1"+p1 → p2
    if token == "_1LOT":
        dev = (device_value or "").strip()
        if len(dev) >= 3:
            p1 = dev[-3:-1]
            return (p1, f"P1{p1}")
        return ("NA", "NA")

    pairs = token2pps.get(str(token), [])
    if not pairs:
        return ("NA", "NA")
    devU = (device_value or "").upper()
    for (p1, p2) in pairs:
        if p1 and (p1.upper() in devU):
            return (p1, p2)
    return ("NA", "NA")


# =================== Environment ===================

def setup_environment():
    cores = multiprocessing.cpu_count()
    for k in ("NUMEXPR_MAX_THREADS", "NUMEXPR_NUM_THREADS", "OMP_NUM_THREADS",
              "OPENBLAS_NUM_THREADS", "MKL_NUM_THREADS", "VECLIB_MAXIMUM_THREADS"):
        os.environ[k] = str(cores)
    print(f"[env] Threads={cores}")


# =================== Cython (with pure-Python fallback) ===================

_HEXMAP = {ord('0'): '0', ord('9'): '1',
           ord('A'): '2', ord('a'): '2', ord('B'): '3', ord('b'): '3',
           ord('C'): '4', ord('c'): '4', ord('D'): '5', ord('d'): '5',
           ord('E'): '6', ord('e'): '6', ord('F'): '7', ord('f'): '7'}

def _py_transform_line(line, xsize):
    b = line.encode('ascii', 'ignore') if isinstance(line, str) else line
    if len(b) < xsize * 2:
        return ""
    return "".join(_HEXMAP.get(b[1 + i * 2], '0') for i in range(xsize))

def _py_convert_hex_values(lines, current_position, xsize, ysize):
    if current_position >= len(lines):
        return ""
    parts = [_py_transform_line(lines[current_position][1:], xsize)]
    for i in range(1, ysize):
        if current_position + i < len(lines):
            parts.append(_py_transform_line(lines[current_position + i][1:], xsize))
    return ",".join(parts)

_CYTHON_FN = None

def get_cython_convert_hex():
    """worker에서 cython 함수 1회 import 캐시. .so 없으면 Python fallback."""
    global _CYTHON_FN
    if _CYTHON_FN is None:
        try:
            importlib.invalidate_caches()
            import cython_functions
            _CYTHON_FN = cython_functions.convert_hex_values_cython
        except ImportError:
            print("[warn] cython_functions not found, using Python fallback (slower)")
            _CYTHON_FN = _py_convert_hex_values
    return _CYTHON_FN


# =================== Font cache / PNG save ===================

_FONT_CACHE = {}


def ttf_cached(w, h, text):
    key = (w, h, len(text))
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    sz = max(8, min(w, h))
    for name in ("DejaVuSans.ttf", "Arial.ttf", "LiberationSans-Regular.ttf"):
        try:
            f = ImageFont.truetype(name, sz)
            _FONT_CACHE[key] = f
            return f
        except:
            pass
    f = ImageFont.load_default()
    _FONT_CACHE[key] = f
    return f


def save_indexed32_png(img, path):
    """원자적 저장: tmp 파일에 저장 후 os.replace"""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".tmp_", dir=str(p.parent))
    os.close(fd)
    try:
        img.save(tmp, format="PNG", optimize=False, compress_level=1)
        if not os.path.exists(tmp) or os.path.getsize(tmp) <= 0:
            raise IOError("empty output")
        os.replace(tmp, path)
    finally:
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except:
            pass


# =================== S3 Manager (Bucket A) ===================

class S3Manager:
    def __init__(self, cfg):
        self.cfg = cfg
        self.client = create_s3_client(cfg)

    def get_top_level_folders(self):
        p = self.client.get_paginator('list_objects_v2')
        folders = []
        for page in p.paginate(Bucket=self.cfg.bucket_name, Delimiter='/'):
            folders.extend(cp['Prefix'] for cp in page.get('CommonPrefixes', []))
        return sorted(folders)

    def get_compressed_files_meta(self, folders, file_pattern='.Z'):
        from collections import deque
        p = self.client.get_paginator('list_objects_v2')

        def list_all(prefix):
            out = []
            st = deque([prefix])
            while st:
                cur = st.pop()
                for page in p.paginate(Bucket=self.cfg.bucket_name, Prefix=cur, Delimiter='/'):
                    for obj in page.get('Contents', []) or []:
                        key = obj['Key']
                        if key.endswith(file_pattern):
                            out.append((key, obj.get('LastModified')))
                    for cp in page.get('CommonPrefixes', []) or []:
                        st.append(cp.get('Prefix'))
            return out

        all_meta = []
        with ThreadPoolExecutor(max_workers=8) as ex:
            for files in tqdm(ex.map(list_all, folders), total=len(folders), desc="List files (meta)"):
                all_meta.extend(files)
        return all_meta

    def prefilter_keys_by_filename(self, folders, token2pps, middle_map, start_dt, end_dt):
        """00P/00C 둘 다 파일명 필터 + kind까지 리턴. LOT ID가 '1'로 시작하는 키도 수집."""
        from datetime import datetime as _dt
        all_meta = self.get_compressed_files_meta(folders, '.Z')
        keys = [k for k, _ in all_meta]

        rx_map = {}
        for tok in token2pps.keys():
            for kind, middle in middle_map.items():
                pat = rf'^\d{{2}}_{re.escape(str(tok))}.*{re.escape(middle)}.*?(?P<d>\d{{8}})[_-]?(?P<t>\d{{6}})'
                rx_map[(str(tok), str(kind))] = re.compile(pat)

        # LOT ID '1' 시작 매칭용: 시간 추출 패턴
        rx_1lot_time = re.compile(r'(?P<d>\d{8})[_-]?(?P<t>\d{6})')

        stats = dict(scanned=len(keys), token_hit=0, time_hit=0, kept=0)
        key_to_info = {}
        window_on = (start_dt is not None and end_dt is not None and (start_dt != end_dt))

        for key in keys:
            bn = os.path.basename(key)
            hit_tok = None
            hit_kind = None
            name_dt = None

            for (tok, kind), rx in rx_map.items():
                m = rx.search(bn)
                if not m:
                    continue
                hit_tok = tok
                hit_kind = kind
                if window_on:
                    try:
                        name_dt = _dt.strptime(f"{m.group('d')}_{m.group('t')}", "%Y%m%d_%H%M%S")
                    except:
                        name_dt = None
                break

            # LOT ID가 '1'로 시작 + 00P/00C 조건: ex) 07_1AB382-00P_N_20260311_230051.Z
            if not hit_tok and re.match(r'^\d{2}_1', bn):
                for kind, middle in middle_map.items():
                    if middle in bn:
                        hit_tok = "_1LOT"
                        hit_kind = kind
                        m_time = rx_1lot_time.search(bn)
                        if window_on and m_time:
                            try:
                                name_dt = _dt.strptime(f"{m_time.group('d')}_{m_time.group('t')}", "%Y%m%d_%H%M%S")
                            except:
                                name_dt = None
                        break

            if not hit_tok:
                continue
            stats['token_hit'] += 1

            if window_on:
                if name_dt is None:
                    continue
                if not (start_dt <= name_dt <= end_dt):
                    continue
                stats['time_hit'] += 1

            key_to_info[key] = (hit_tok, hit_kind)
            stats['kept'] += 1

        return key_to_info, stats

    def download_and_decompress_parallel(self, keys):
        if not keys:
            return []
        try:
            from unlzw3 import unlzw
        except:
            unlzw = None
        try:
            import py7zr
        except:
            py7zr = None
        sevenz = shutil.which("7z") or shutil.which("7za") or shutil.which("7zr")

        _decode = decode_best_effort

        def _sig_7z(b): return len(b) >= 6 and b[:6] == b"7z\xBC\xAF\x27\x1C"
        def _sig_Z(b):  return len(b) >= 2 and b[:2] == b"\x1f\x9d"
        def _sig_gz(b): return len(b) >= 2 and b[:2] == b"\x1f\x8b"
        def _is_zip(b):
            try:
                return zipfile.is_zipfile(io.BytesIO(b))
            except:
                return False

        def _extract_zip(data, tag):
            out = []
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for n in zf.namelist():
                    if n.endswith("/"):
                        continue
                    out.append((f"{tag}::{n}", zf.read(n)))
            return out

        def _extract_py7zr(data, tag):
            if not py7zr:
                return []
            out = []
            try:
                with py7zr.SevenZipFile(io.BytesIO(data)) as ar:
                    for n, fobj in ar.readall().items():
                        out.append((f"{tag}::{n}", fobj.read()))
            except:
                pass
            return out

        def _extract_7z_cli(data, tag):
            out = []
            if not sevenz:
                return out
            with tempfile.TemporaryDirectory() as td:
                inpath = os.path.join(td, "in.bin")
                with open(inpath, "wb") as f:
                    f.write(data)
                cmd = [sevenz, "x", inpath, f"-o{td}", "-y", "-bd", "-bso0", "-bsp0"]
                try:
                    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                except:
                    return out
                for root, _, files in os.walk(td):
                    for fn in files:
                        p = os.path.join(root, fn)
                        rel = os.path.relpath(p, td).replace("\\", "/")
                        with open(p, "rb") as f:
                            out.append((f"{tag}::{rel}", f.read()))
            return out

        def _extract_tar_like(data, tag):
            out = []
            bio = io.BytesIO(data)
            try:
                with tarfile.open(fileobj=bio, mode="r:*") as tf:
                    for m in tf.getmembers():
                        if not m.isfile():
                            continue
                        f = tf.extractfile(m)
                        if f:
                            out.append((f"{tag}::{m.name}", f.read()))
            except:
                pass
            return out

        def _expand(name, data, depth=0):
            if depth > 6:
                return [(name, _decode(data))]

            if _is_zip(data):
                try:
                    pairs = _extract_zip(data, name)
                except:
                    pairs = _extract_py7zr(data, name) or _extract_7z_cli(data, name)
                if pairs:
                    out = []
                    for n, b in pairs:
                        out.extend(_expand(n, b, depth + 1))
                    return out

            if _sig_7z(data):
                pairs = _extract_py7zr(data, name) or _extract_7z_cli(data, name)
                if pairs:
                    out = []
                    for n, b in pairs:
                        out.extend(_expand(n, b, depth + 1))
                    return out

            if _sig_gz(data):
                try:
                    u = gzip.decompress(data)
                    return _expand(name.rsplit(".gz", 1)[0], u, depth + 1)
                except:
                    pass

            if name.lower().endswith(".z"):
                if unlzw is not None and _sig_Z(data):
                    try:
                        u = unlzw(data)
                        base = name.rsplit(".Z", 1)[0]
                        return _expand(base, u, depth + 1)
                    except:
                        pass
                pairs = _extract_py7zr(data, name) or _extract_7z_cli(data, name)
                if not pairs and _is_zip(data):
                    try:
                        pairs = _extract_zip(data, name)
                    except:
                        pairs = _extract_py7zr(data, name) or _extract_7z_cli(data, name)
                if not pairs:
                    pairs = _extract_tar_like(data, name)
                if pairs:
                    out = []
                    for n, b in pairs:
                        out.extend(_expand(n, b, depth + 1))
                    return out

            pairs = _extract_tar_like(data, name) or _extract_py7zr(data, name) or _extract_7z_cli(data, name)
            if pairs:
                out = []
                for n, b in pairs:
                    out.extend(_expand(n, b, depth + 1))
                return out

            return [(name, _decode(data))]

        def _one(key):
            try:
                body = self.client.get_object(Bucket=self.cfg.bucket_name, Key=key)['Body'].read()
            except Exception as e:
                print(f"[s3] Error {key}: {e}")
                return []
            try:
                flat = _expand(key, body, 0)
                return [(n, t) for n, t in flat if isinstance(t, str) and t.strip()]
            except Exception as e:
                print(f"[extract] Error {key}: {e}")
                return []

        allc = []
        with ThreadPoolExecutor(max_workers=self.cfg.download_threads) as ex:
            for part in tqdm(ex.map(_one, keys), total=len(keys), desc="Download+Decompress"):
                allc.extend(part)
        return allc
