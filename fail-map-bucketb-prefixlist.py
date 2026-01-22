#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, re, sys, time, json, io, zipfile, tempfile, shutil, gzip, tarfile, subprocess, importlib, multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path

import boto3
import pandas as pd
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from tqdm import tqdm
from botocore.config import Config

# =================== Config ===================

@dataclass
class PipelineConfig:
    bucket_name: str = 'eds-ec-memory.fbm-data'
    region_name: str = ''
    aws_access_key_id: str = 'ho.choi-LakeS3-F6B0U6'            # 요청: 자동 치환 금지
    aws_secret_access_key: str = 'iYb7zYDVzitt4QVkUcR2'         # 요청: 자동 치환 금지
    endpoint_url: str = 'http://lakes3.dataplatform.samsungds.net:9020'
    max_pool_connections: int = 256
    download_threads: int = 128
    cpu_processes: int = min(multiprocessing.cpu_count(), 24)
    chunk_size: int = 300

    border_thickness: int = 1
    defect_border_thickness: int = 2
    default_tile_size: tuple = (24, 24)

    draw_empty_chip_text: bool = True
    empty_chip_text_field: str = "b"

    palette_colors: int = 32
    color_json: str = "/appdata/appuser/l3tracker-main/logs/color-legends.json"
    folder_filter_middle: str = "-00P_"

    hours_back_start: int = 0
    hours_back_end:   int = 2

    df_path: str = "/appdata/appuser/project/device_info.txt"
    df_positions: tuple = (4, 3, 1)  # (token, prefix1, prefix2) 1-based

    base_root: str = "/appdata/appuser/images"
    positions_root: str = "/appdata/appuser/positions"

CFG = PipelineConfig()

# =================== Bucket B (Secondary) Config ===================

@dataclass
class BucketBConfig:
    """
    Bucket B 파일 매칭 규칙:
      A: YYYYMMDD/WW_LOTBASE-00SUFFIX_X_YYYYMMDD_HHMMSS.Z
      B: YYYYMMDD/LOTBASESUFFIX_WWW_YYYYMMDD_HHMMSS.gz
    시간 오프셋: -10~+10초
    """
    bucket_name: str = 'eds.m-eds-map-raw'
    region_name: str = ''
    aws_access_key_id: str = 'ho.choi-LakeS3-F6B0U6'            # 요청: 자동 치환 금지
    aws_secret_access_key: str = 'iYb7zYDVzitt4QVkUcR2'         # 요청: 자동 치환 금지
    endpoint_url: str = 'http://lakes3.dataplatform.samsungds.net:9020'
    max_pool_connections: int = 256
    enabled: bool = True
    time_offset_range: tuple = (-10, 10)
    file_ext: str = ".gz"
    list_max_workers: int = 32
    firstline_max_workers: int = 32
    firstline_max_bytes: int = 65536
    debug: bool = False

CFG_B = BucketBConfig()

# =================== Bucket B Matching Helpers (Prefix-List) ===================

_RX_A_BASENAME = re.compile(
    r'^(?P<wafer>\d{2})_(?P<lotbase>[A-Z0-9]+)-00(?P<suffix>[A-Z])_(?P<middle>[A-Z])_(?P<date>\d{8})_(?P<time>\d{6})\.Z$',
    re.IGNORECASE
)

def _split_key_and_inner(name: str):
    s = str(name or "")
    return s.split("::", 1)[0]

def parse_bucket_a_key(key: str):
    """
    예)
      A: 20260122/01_3BC170H3-00P_N_20260122_022718.Z
    반환:
      dict or None
    """
    k = _split_key_and_inner(key)
    parts = k.split("/", 1)
    if len(parts) != 2:
        return None
    folder = parts[0]
    basename = parts[1]
    m = _RX_A_BASENAME.match(os.path.basename(basename))
    if not m:
        return None
    wafer_num = m.group("wafer")
    lot_id = f"{m.group('lotbase')}{m.group('suffix')}"  # -00 제거 + 접미 1글자 결합
    date = m.group("date")
    time_hhmmss = m.group("time")
    return {
        "a_key": k,
        "folder": folder,
        "wafer_num": wafer_num,
        "wafer_w": f"W{wafer_num}",
        "lot_id": lot_id,
        "date": date,
        "time": time_hhmmss,
    }

def iter_offsets_by_closeness(offset_range):
    """
    offset_range 내에서 0에 가까운 순서로 오프셋을 생성.
    예) (-2, 3) -> 0, +1, -1, +2, -2, +3
    """
    lo, hi = int(offset_range[0]), int(offset_range[1])
    if hi < lo:
        lo, hi = hi, lo
    max_abs = max(abs(lo), abs(hi))
    for d in range(0, max_abs + 1):
        if d == 0:
            offs = [0]
        else:
            # tie-break: +d 우선, 그 다음 -d
            offs = [d, -d]
        for off in offs:
            if lo <= off <= hi:
                yield off


def generate_bucket_b_candidate_keys(info: dict, offset_range=(-10, 10)):
    dt0 = datetime.strptime(f"{info['date']}_{info['time']}", "%Y%m%d_%H%M%S")
    for off in iter_offsets_by_closeness(offset_range):
        dt = dt0 + timedelta(seconds=int(off))
        folder_a = info.get("folder") or dt.strftime("%Y%m%d")
        day = dt.strftime("%Y%m%d")
        hhmmss = dt.strftime("%H%M%S")
        # 기본: A 폴더(대부분 케이스). 단, 자정 넘김으로 dt의 날짜가 바뀌면 dt 날짜 폴더도 함께 시도
        key_b1 = f"{folder_a}/{info['lot_id']}_{info['wafer_w']}_{day}_{hhmmss}{CFG_B.file_ext}"
        yield key_b1, int(off)
        folder_dt = day
        if folder_dt != folder_a:
            key_b2 = f"{folder_dt}/{info['lot_id']}_{info['wafer_w']}_{day}_{hhmmss}{CFG_B.file_ext}"
            yield key_b2, int(off)

def generate_bucket_b_prefixes(info: dict, offset_range=(-10, 10)):
    """
    list_objects_v2 를 아주 좁은 prefix로 호출하기 위한 prefix 생성.
    - 자정 넘김을 고려해 offset 시작/끝으로 day 2개까지 생성
    """
    folder_a = info.get("folder") or info.get("date")
    if not folder_a:
        return
    days = {str(folder_a)}
    # 자정 넘김 대응: dt0+lo/hi에서 날짜가 바뀌면 그 날짜 폴더도 list 대상으로 포함
    try:
        dt0 = datetime.strptime(f"{info['date']}_{info['time']}", "%Y%m%d_%H%M%S")
        lo, hi = int(offset_range[0]), int(offset_range[1])
        if hi < lo:
            lo, hi = hi, lo
        days.add((dt0 + timedelta(seconds=lo)).strftime("%Y%m%d"))
        days.add((dt0 + timedelta(seconds=hi)).strftime("%Y%m%d"))
    except:
        pass
    # 내부 date(YYYYMMDD)는 자정 넘어가면 변할 수 있으므로 prefix는 lot+wafer까지만 사용
    for d in sorted(days):
        yield f"{d}/{info['lot_id']}_{info['wafer_w']}_"

def _decode_best_effort(b: bytes) -> str:
    for enc in ("utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return b.decode(enc)
        except:
            pass
    return b.decode("utf-8", errors="ignore")

class S3ManagerB:
    def __init__(self, cfg: BucketBConfig):
        self.cfg = cfg
        self.client = boto3.session.Session().client(
            "s3",
            region_name=cfg.region_name or None,
            aws_access_key_id=cfg.aws_access_key_id or None,
            aws_secret_access_key=cfg.aws_secret_access_key or None,
            endpoint_url=cfg.endpoint_url or None,
            config=Config(max_pool_connections=cfg.max_pool_connections, retries={'max_attempts': 8, 'mode': 'adaptive'}),
            use_ssl=False,
        )

    def list_keys_with_prefix(self, prefix: str):
        out = []
        token = None
        while True:
            # Delimiter='/' 를 주면 prefix 아래의 \"폴더\" 경계로 끊어서 조회 (지금 구조에선 결과는 동일하지만 의도 명확)
            kw = dict(Bucket=self.cfg.bucket_name, Prefix=prefix, Delimiter='/', MaxKeys=1000)
            if token:
                kw["ContinuationToken"] = token
            resp = self.client.list_objects_v2(**kw)
            for obj in resp.get("Contents", []) or []:
                out.append(obj["Key"])
            if resp.get("IsTruncated"):
                token = resp.get("NextContinuationToken")
                if not token:
                    break
            else:
                break
        return out

    def read_gz_first_line(self, key: str, max_bytes: int = 65536) -> str:
        try:
            obj = self.client.get_object(Bucket=self.cfg.bucket_name, Key=key)
            body = obj["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                line = gz.readline(max_bytes)
            if not line:
                return ""
            return _decode_best_effort(line).rstrip("\r\n")
        except:
            return ""

def build_bucket_b_match_map_prefixlist(part_keys_a, s3b: S3ManagerB, cfg_b: BucketBConfig, chunk_label: str = ""):
    """
    returns: dict[a_key] -> match_meta
    """
    t0 = time.time()
    infos = {}
    for ka in part_keys_a:
        info = parse_bucket_a_key(ka)
        if info:
            infos[ka] = info

    # 1) prefix 수집
    prefixes = set()
    for info in infos.values():
        for pfx in generate_bucket_b_prefixes(info, cfg_b.time_offset_range):
            prefixes.add(pfx)

    # 2) prefix 별 list
    all_b_keys = set()
    if prefixes:
        with ThreadPoolExecutor(max_workers=cfg_b.list_max_workers) as ex:
            for keys in ex.map(s3b.list_keys_with_prefix, sorted(prefixes)):
                for k in keys:
                    all_b_keys.add(k)

    # 3) A -> B 매칭
    match_map = {}
    matched = 0
    for ka in part_keys_a:
        info = infos.get(ka)
        if not info:
            match_map[ka] = {
                "matched": False,
                "bucket": cfg_b.bucket_name,
                "method": "prefix_list",
                "reason": "parse_failed",
                "time_offset_range": list(cfg_b.time_offset_range),
            }
            continue
        hit_key = None
        hit_off = None
        for kb, off in generate_bucket_b_candidate_keys(info, cfg_b.time_offset_range):
            if kb in all_b_keys:
                hit_key = kb
                hit_off = int(off)
                break
        if hit_key:
            matched += 1
            match_map[ka] = {
                "matched": True,
                "bucket": cfg_b.bucket_name,
                "method": "prefix_list",
                "a_key": ka,
                "key": hit_key,
                "offset_sec": hit_off,
                "time_offset_range": list(cfg_b.time_offset_range),
                "first_line": "",
                "first_line_ok": False,
            }
        else:
            match_map[ka] = {
                "matched": False,
                "bucket": cfg_b.bucket_name,
                "method": "prefix_list",
                "a_key": ka,
                "time_offset_range": list(cfg_b.time_offset_range),
            }

    # 4) first line read (matched only)
    matched_keys = [m["key"] for m in match_map.values() if m.get("matched") and m.get("key")]
    firstline_by_key = {}
    if matched_keys:
        def _read_one(k):
            return k, s3b.read_gz_first_line(k, max_bytes=cfg_b.firstline_max_bytes)
        with ThreadPoolExecutor(max_workers=cfg_b.firstline_max_workers) as ex:
            for k, line in ex.map(_read_one, matched_keys):
                firstline_by_key[k] = line
        for meta in match_map.values():
            if meta.get("matched") and meta.get("key"):
                line = firstline_by_key.get(meta["key"], "")
                meta["first_line"] = line
                meta["first_line_ok"] = bool(line)

    parse_failed = sum(1 for m in match_map.values() if m.get("reason") == "parse_failed")
    firstline_ok = sum(1 for m in match_map.values() if m.get("matched") and m.get("first_line_ok"))
    lbl = f" {chunk_label}" if chunk_label else ""
    read_part = (f" read_ok={firstline_ok}/{matched}" if matched else " read_ok=0")
    print(
        f"[bucketB(prefix-list){lbl}] match성공={matched}/{len(part_keys_a)}"
        f"{read_part} parse_failed={parse_failed} prefixes={len(prefixes)} listed_keys={len(all_b_keys)}"
        f" in {time.time()-t0:.2f}s"
    )
    return match_map

# =================== Env / Cython(import-only) ===================

def setup_environment():
    cores = multiprocessing.cpu_count()
    for k in ("NUMEXPR_MAX_THREADS","NUMEXPR_NUM_THREADS","OMP_NUM_THREADS",
              "OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
        os.environ[k] = str(cores)
    print(f"[env] Threads={cores}")

def get_cython_convert_hex():
    importlib.invalidate_caches()
    import cython_functions
    return cython_functions.convert_hex_values_cython

# =================== Utils ===================

def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "", (s or "NA").strip()) or "NA"

def _safe_prefix(*parts: str) -> str:
    return "/".join(re.sub(r"[^A-Za-z0-9._-]+", "", str(p)) for p in parts if str(p).strip())

def select_folders_by_window(folders, a, b):
    if b < a: a, b = b, a
    want = {(a + timedelta(days=i)).strftime("%y%m%d") for i in range((b.date()-a.date()).days+1)}
    want |= {w if len(w)==8 else "20"+w for w in want}  # YYMMDD -> 20YYMMDD
    return [f for f in folders if f.rstrip("/").split("/")[-1] in want]

def _cleanup_empty_p2_and_dates(base_root: str, p1_set: set) -> int:
    removed = 0
    for p1 in sorted(p1_set):
        p1_dir = os.path.join(base_root, _safe_prefix(p1))
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

# =================== Palette ===================

def _hex_to_rgb(hex_code: str):
    s = (hex_code or "").strip()
    if not (s.startswith("#") and len(s) == 7):
        return [0, 0, 0]
    return [int(s[1:3],16), int(s[3:5],16), int(s[5:7],16)]

def _flatten_palette_by_keys(color_map, index_to_key, total_colors=32):
    rgb = []
    for key in index_to_key:
        rgb.extend(_hex_to_rgb(color_map.get(key, "#000000")))
    need = total_colors * 3 - len(rgb)
    if need > 0:
        rgb.extend([0] * need)
    elif need < 0:
        raise ValueError("palette key overflow")
    return rgb

with open(CFG.color_json, 'r', encoding='utf-8') as f:
    _cd = json.load(f)
_d   = _cd['default']
_top = _d['top']
_btm = _d.get('bottom', {})

PALETTE_HEX_MAP = {
    "chip0": _top.get("Grade0", "#FFFFFF"),
    "chip1": _top.get("Grade1", "#9B9B9B"),
    "chip2": _top.get("Grade2", "#009619"),
    "chip3": _top.get("Grade3", "#0000FF"),
    "chip4": _top.get("Grade4", "#D91DFF"),
    "chip5": _top.get("Grade5", "#FFFF00"),
    "chip6": _top.get("Grade6", "#FF0000"),
    "chip7": _top.get("Grade7", "#000000"),
    "border":      _btm.get("border",  "#BEBEBE"),
    "border_inv":  _btm.get("Invalid", "#FF9900"),
    "border_b285": _btm.get("B285",    "#0099FF"),
    "border_b286": _btm.get("B286",    "#FF714F"),
    "border_b287": _btm.get("B287",    "#66FFCC"),
    "border_b288": _btm.get("B288",    "#DA26CD"),
    "bg":          _d.get("background", "#FEFEFE"),
    "text":        _d.get("text", "#000001"),
}

PALETTE_INDEX_TO_KEY = [
    "chip0","chip1","chip2","chip3","chip4","chip5","chip6","chip7",   # 0..7
    "border","border_inv","border_b285","border_b286","border_b287","border_b288",  # 8..13
    "bg","text"  # 14..15
]
KEY_TO_INDEX = {k:i for i,k in enumerate(PALETTE_INDEX_TO_KEY)}

IDX_INVALID_FILL = 31
IDX_BG         = KEY_TO_INDEX["bg"]          # 14
IDX_BORDER     = KEY_TO_INDEX["border"]      # 8
IDX_BORDER_INV = KEY_TO_INDEX["border_inv"]  # 9
IDX_TEXT       = KEY_TO_INDEX["text"]        # 15
IDX_B_DEF = {
    "285": KEY_TO_INDEX["border_b285"],
    "286": KEY_TO_INDEX["border_b286"],
    "287": KEY_TO_INDEX["border_b287"],
    "288": KEY_TO_INDEX["border_b288"],
}

PALETTE_32 = _flatten_palette_by_keys(PALETTE_HEX_MAP, PALETTE_INDEX_TO_KEY, 32)
PALETTE_32[IDX_INVALID_FILL*3:IDX_INVALID_FILL*3+3] = _hex_to_rgb("#FFFFFF")

# =================== Header parsing helpers ===================

def _hval(lines, key, max_lines=200):
    for ln in lines[:max_lines]:
        if ln.startswith(key):
            return ln.split("=",1)[1].strip()
    return ""

def _parse_stime(lines):
    stime = "NA"
    if len(lines) > 9 and lines[9].startswith(':STIME='):
        raw = lines[9].split('=', 1)[1].strip()
        m = re.match(r'(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):(\d{2})', raw)
        stime = (f"{m.group(1)}{m.group(2)}{m.group(3)}_{m.group(4)}{m.group(5)}{m.group(6)}"
                 if m else raw.replace('/', '').replace(':', '').replace(' ', '_'))
    return stime

def _parse_stime_dt(st):
    if not st: return None
    if not re.match(r'^(\d{8})_(\d{6})$', st): return None
    try:
        return datetime.strptime(st, "%Y%m%d_%H%M%S")
    except:
        return None

# =================== Token→pair 결정 (DEVICE 기반, p1만) ===================

def choose_pair_by_device(token, device_value, token2pps):
    pairs = token2pps.get(str(token), [])
    if not pairs:
        return ("NA", "NA")
    devU = (device_value or "").upper()
    for (p1, p2) in pairs:
        if p1 and (p1.upper() in devU):
            return (p1, p2)
    return ("NA", "NA")

# =================== Parsing ===================

def find_initial_values_from_lines(lines, file_name):
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
    file_name, file_content = args
    if not file_content:
        return []
    lines = file_content.splitlines()
    if not lines:
        return []

    # header values
    stime  = _parse_stime(lines)
    partid = _hval(lines, ":PARTID=")
    tester = _hval(lines, ":TESTER=")
    device = _hval(lines, ":DEVICE=")
    pgm    = _hval(lines, ":PGM=")

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

        orig_key = _split_key_and_inner(file_name)
        last_by_xy[(cx, cy)] = {
            "root": root, "step": step, "wafer": wafer,
            "x": cx, "y": cy, "b": cb,
            "transformed_values": hex_block,
            "stime": stime,
            "rot": rot,
            "partid": partid, "tester": tester, "device": device, "pgm": pgm,
            "orig_key": orig_key
        }
        i += 1

    return list(last_by_xy.values())

# =================== Image generation ===================

_FONT_CACHE = {}
def _ttf_cached(w, h, text):
    key = (w, h, len(text))
    if key in _FONT_CACHE:
        return _FONT_CACHE[key]
    sz = max(8, min(w, h))
    for name in ("DejaVuSans.ttf","Arial.ttf","LiberationSans-Regular.ttf"):
        try:
            f = ImageFont.truetype(name, sz)
            _FONT_CACHE[key] = f
            return f
        except:
            pass
    f = ImageFont.load_default()
    _FONT_CACHE[key] = f
    return f

def map_tile_after_rotation(i0, j0, rot_code, tilesW_after, tilesH_after):
    if rot_code == 7:   # 90 CCW
        return (j0, tilesH_after - 1 - i0)
    elif rot_code == 3: # 270 CCW
        return (tilesW_after - 1 - j0, i0)
    elif rot_code == 0: # 180
        return (tilesW_after - 1 - i0, tilesH_after - 1 - j0)
    else:
        return (i0, j0)

def centerize_col(i, W):
    return i - (W//2 - 1) if (W % 2 == 0) else i - (W//2)

def centerize_row(j, H):
    return j - (H//2)

def _save_indexed32_png(img, path):
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

def create_sample_image_func(args):
    (samples, output_path, border_thin, rot, default_tile_size,
     draw_empty_text, empty_text_field, defect_border_thickness, positions_root) = args

    if not samples:
        return None

    xs = [s['x'] for s in samples]; ys = [s['y'] for s in samples]
    x_min, x_max, y_min, y_max = min(xs), max(xs), min(ys), max(ys)
    tiles_w = x_max - x_min + 1
    tiles_h = y_max - y_min + 1

    first_valid = next((s for s in samples if s.get('transformed_values')), None)
    if first_valid and first_valid.get('transformed_values'):
        sr = first_valid['transformed_values'].split(',')
        sh = len(sr); sw = len(sr[0]) if sh > 0 else 0
        if sh == 0 or sw == 0:
            sh, sw = default_tile_size
    else:
        sh, sw = default_tile_size

    H0, W0 = tiles_h * sh, tiles_w * sw
    idx0 = np.full((H0, W0), IDX_BG, dtype=np.uint8)

    vmap, bmap, have = {}, {}, set()

    def _tile_ok(s):
        rs = s.get('transformed_values') or ""
        rows = rs.split(',') if rs else []
        return (len(rows) == sh) and (sh > 0) and all(len(r) == sw for r in rows)

    for s in samples:
        i, j = s['x'] - x_min, s['y'] - y_min
        have.add((i, j))
        ok = _tile_ok(s)
        vmap[(i, j)] = ok
        bmap[(i, j)] = (s.get('b') or "").strip()
        y0, y1 = j * sh, (j + 1) * sh
        x0, x1 = i * sw, (i + 1) * sw
        if ok:
            rows = (s.get('transformed_values') or "").split(',')
            vals = np.frombuffer(''.join(rows).encode('ascii'), dtype=np.uint8) - ord('0')
            idx0[y0:y1, x0:x1] = vals.reshape(sh, sw)
        else:
            idx0[y0:y1, x0:x1] = IDX_INVALID_FILL

    rot_code = int(rot) if rot is not None else 5
    if rot_code == 7:
        idxR = np.transpose(idx0, (1, 0))[::-1, :]
        tilesW_after, tilesH_after = tiles_h, tiles_w
    elif rot_code == 3:
        idxR = np.transpose(idx0, (1, 0))[:, ::-1]
        tilesW_after, tilesH_after = tiles_h, tiles_w
    elif rot_code == 0:
        idxR = idx0[::-1, ::-1]
        tilesW_after, tilesH_after = tiles_w, tiles_h
    else:
        idxR = idx0
        tilesW_after, tilesH_after = tiles_w, tiles_h
    del idx0

    imgP = Image.fromarray(idxR, mode='P')
    imgP.putpalette(PALETTE_32)

    wR, hR = imgP.size
    if wR == hR:
        imgS = imgP
        sx = sy = 1.0
    elif wR < hR:
        S = wR
        imgS = imgP.resize((S, S), resample=Image.NEAREST)
        sx, sy = 1.0, (wR / hR)
    else:
        S = hR
        imgS = imgP.resize((S, S), resample=Image.NEAREST)
        sx, sy = (hR / wR), 1.0

    W, H = imgS.size
    arr = np.array(imgS, dtype=np.uint8, copy=True)
    arr.setflags(write=1)

    xs_pix = [int(round(k * W / tilesW_after)) for k in range(tilesW_after + 1)]
    ys_pix = [int(round(k * H / tilesH_after)) for k in range(tilesH_after + 1)]

    b = int(max(1, border_thin))
    for (ii0, jj0) in have:
        ii, jj = map_tile_after_rotation(ii0, jj0, rot_code, tilesW_after, tilesH_after)
        x0, x1 = xs_pix[ii], xs_pix[ii + 1]
        y0, y1 = ys_pix[jj], ys_pix[jj + 1]
        arr[y0:y0+b, x0:x1] = IDX_BORDER
        arr[y1-b:y1, x0:x1] = IDX_BORDER
        arr[y0:y1, x0:x0+b] = IDX_BORDER
        arr[y0:y1, x1-b:x1] = IDX_BORDER

    for (ii0, jj0) in have:
        if vmap.get((ii0, jj0), False):
            continue
        ii, jj = map_tile_after_rotation(ii0, jj0, rot_code, tilesW_after, tilesH_after)
        x0, x1 = xs_pix[ii], xs_pix[ii + 1]
        y0, y1 = ys_pix[jj], ys_pix[jj + 1]
        arr[y0:y1, x0:x1] = IDX_INVALID_FILL

    base_img = Image.fromarray(arr, mode='P')
    base_img.putpalette(PALETTE_32)

    draw = ImageDraw.Draw(base_img)
    d = int(max(1, defect_border_thickness))
    TEXT_FILL_RATIO = 0.35

    for s in samples:
        x_abs = int(s['x']); y_abs = int(s['y'])
        ii0, jj0 = x_abs - x_min, y_abs - y_min
        ii, jj = map_tile_after_rotation(ii0, jj0, rot_code, tilesW_after, tilesH_after)
        x0, x1 = xs_pix[ii], xs_pix[ii + 1]
        y0, y1 = ys_pix[jj], ys_pix[jj + 1]

        ok = vmap.get((ii0, jj0), False)
        bval = bmap.get((ii0, jj0), "")
        mnum = re.search(r'(\d{3})', bval or "")
        num_key = (mnum.group(1) if mnum else None)

        if not ok:
            cidx = IDX_BORDER_INV
        elif num_key in IDX_B_DEF:
            cidx = IDX_B_DEF[num_key]
        else:
            cidx = None

        if cidx is not None:
            base_img.paste(cidx, (x0, y0, x1, y0 + d))
            base_img.paste(cidx, (x0, y1 - d, x1, y1))
            base_img.paste(cidx, (x0, y0, x0 + d, y1))
            base_img.paste(cidx, (x1 - d, y0, x1, y1))

        if draw_empty_text and (not ok):
            rawb = str(s.get(empty_text_field) or s.get('b') or "").strip()
            if rawb:
                rawb = rawb[1:4] if len(rawb) >= 4 else rawb[-3:]
                inner_w = max(1, int(round((x1 - x0) * TEXT_FILL_RATIO)))
                inner_h = max(1, int(round((y1 - y0) * TEXT_FILL_RATIO)))
                font = _ttf_cached(inner_w, inner_h, rawb)
                try:
                    tw = int(draw.textlength(rawb, font=font))
                    th = font.size
                except:
                    tw, th = inner_w, inner_h
                cx, cy = (x0 + x1) // 2, (y0 + y1) // 2
                draw.text((cx - tw // 2, cy - th // 2), rawb, fill=IDX_TEXT, font=font)

    _save_indexed32_png(base_img, output_path)

    # positions json
    meta0 = samples[0]
    p1 = meta0.get("p1","NA"); p2 = meta0.get("p2","NA")
    stime = str(meta0.get("stime",""))
    day = (stime.split('_')[0] if (stime and '_' in stime) else "NA")

    Ws, Hs = base_img.size
    tiles_w_rot = tilesW_after
    tiles_h_rot = tilesH_after

    xs_edges = [int(round(k * Ws / tiles_w_rot)) for k in range(tiles_w_rot + 1)]
    ys_edges = [int(round(k * Hs / tiles_h_rot)) for k in range(tiles_h_rot + 1)]

    chips_json = []
    for s in samples:
        x_abs = int(s['x']); y_abs = int(s['y'])
        i0 = x_abs - x_min; j0 = y_abs - y_min
        i, j = map_tile_after_rotation(i0, j0, rot_code, tiles_w_rot, tiles_h_rot)
        x_cal = centerize_col(i, tiles_w_rot)
        y_cal = centerize_row(j, tiles_h_rot)
        x0, x1 = xs_edges[i], xs_edges[i+1]
        y0, y1 = ys_edges[j], ys_edges[j+1]
        rawb = (str(s.get('b') or "").lstrip('0') or '0')
        chips_json.append({
            "x_abs": x_abs, "y_abs": y_abs, "b": rawb,
            "x_cal": int(x_cal), "y_cal": int(y_cal),
            "rect": {
                "x0": int(x0), "y0": int(y0), "x1": int(x1), "y1": int(y1),
                "quad": [[int(x0),int(y0)],[int(x1),int(y0)],[int(x1),int(y1)],[int(x0),int(y1)]]
            }
        })

    # ===== Bucket B match summary (첫 key로 노출) =====
    _bm = meta0.get("bucket_b_match")
    _match = "match실패"
    _b_key = ""
    _b_off = None
    _b_first = ""
    if isinstance(_bm, dict) and _bm.get("matched"):
        _match = "match성공"
        _b_key = str(_bm.get("key") or "")
        _b_off = _bm.get("offset_sec")
        _b_first = str(_bm.get("first_line") or "")

    json_obj = {
        "match": _match,
        "bucket_b_key": _b_key,
        "bucket_b_offset_sec": _b_off,
        "bucket_b_first_line": _b_first,
        "image_path": output_path,
        "root": meta0.get("root",""),
        "step": meta0.get("step",""),
        "wafer": meta0.get("wafer",""),
        "stime": stime,
        "day": day,
        "partid": meta0.get("partid",""),
        "tester": meta0.get("tester",""),
        "device": meta0.get("device",""),
        "pgm": meta0.get("pgm",""),
        "coord": {
            "rot_code": int(rot_code),
            "x_min_abs": int(x_min),
            "y_min_abs": int(y_min),
            "x_max_abs": int(x_max),
            "y_max_abs": int(y_max),
            "tiles_w_rot": int(tiles_w_rot),
            "tiles_h_rot": int(tiles_h_rot),
            "grid_edges": {"xs": xs_edges, "ys": ys_edges},
            "canvas": {"width": int(Ws), "height": int(Hs)},
            "scale": {"sx": float(sx), "sy": float(sy)},
            "border": int(border_thin),
            "defect_border": int(defect_border_thickness),
            "center_rule": {"even_x_zero": "left", "even_y_zero": "down"}
        },
        "chips": chips_json
    }

    # Bucket B 매칭 상세(디버그용)
    if _bm is not None:
        json_obj["bucket_b_match"] = _bm

    json_dir = os.path.join(positions_root, _safe_prefix(p1), _safe_prefix(p2), day)
    os.makedirs(json_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(output_path))[0]
    json_path = os.path.join(json_dir, base_name + ".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_obj, f, ensure_ascii=False, indent=2)

    return output_path

# =================== S3 / Decompress ===================

class S3Manager:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.client = boto3.session.Session().client(
            "s3",
            region_name=cfg.region_name or None,
            aws_access_key_id=cfg.aws_access_key_id or None,
            aws_secret_access_key=cfg.aws_secret_access_key or None,
            endpoint_url=cfg.endpoint_url or None,
            config=Config(max_pool_connections=cfg.max_pool_connections, retries={'max_attempts': 8, 'mode': 'adaptive'}),
            use_ssl=False,
        )

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

    def prefilter_keys_by_filename(self, folders, token2pps, middle, start_dt, end_dt):
        all_meta = self.get_compressed_files_meta(folders, '.Z')
        keys = [k for k, _ in all_meta]

        rx_map = {}
        for tok in token2pps.keys():
            pat = rf'^\d{{2}}_{re.escape(str(tok))}.*{re.escape(middle)}.*?(?P<d>\d{{8}})[_-]?(?P<t>\d{{6}})'
            rx_map[str(tok)] = re.compile(pat)

        stats = dict(scanned=len(keys), token_hit=0, time_hit=0, kept=0)
        key_to_token = {}
        window_on = (start_dt is not None and end_dt is not None and (start_dt != end_dt))

        for key in keys:
            bn = os.path.basename(key)
            hit_tok = None
            name_dt = None
            for tok, rx in rx_map.items():
                m = rx.search(bn)
                if not m:
                    continue
                hit_tok = tok
                if window_on:
                    try:
                        name_dt = datetime.strptime(f"{m.group('d')}_{m.group('t')}", "%Y%m%d_%H%M%S")
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

            key_to_token[key] = hit_tok
            stats['kept'] += 1

        return key_to_token, stats

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

        def _decode(b):
            for enc in ("utf-8", "cp949", "euc-kr", "latin1"):
                try:
                    return b.decode(enc)
                except:
                    pass
            return b.decode("utf-8", errors="ignore")

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

# =================== Processor / Generator ===================

class DataProcessor:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.executor = ProcessPoolExecutor(max_workers=cfg.cpu_processes,
                                            mp_context=multiprocessing.get_context("spawn"))
    def close(self):
        self.executor.shutdown(wait=True)

    def process_files_parallel_tagged(self, tagged_pairs):
        if not tagged_pairs:
            return []
        file_contents = [(name, text) for _, _, _, name, text in tagged_pairs]
        results = list(tqdm(self.executor.map(
            process_file_content, file_contents,
            chunksize=max(1, len(file_contents)//(self.cfg.cpu_processes*4) or 1)
        ), total=len(file_contents), desc="Processing (Cython)"))

        out = []
        for idx, fr in enumerate(results):
            tok, p1, p2 = tagged_pairs[idx][0], tagged_pairs[idx][1], tagged_pairs[idx][2]
            for r in fr:
                r["token"] = tok
                r["p1"] = p1
                r["p2"] = p2
                out.append(r)
        return out

class ImageGenerator:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.executor = ProcessPoolExecutor(max_workers=cfg.cpu_processes,
                                            mp_context=multiprocessing.get_context("spawn"))
    def close(self):
        self.executor.shutdown(wait=True)

    def generate_images_mixed(self, dataset_all, base_root, positions_root):
        from collections import defaultdict, Counter
        groups = defaultdict(list)
        for s in dataset_all:
            key = (s.get('token','NA'), s.get('p1','NA'), s.get('p2','NA'),
                   s.get('root',''), s.get('step',''), s.get('wafer',''), s.get('stime','NA'))
            groups[key].append(s)

        tasks, task_keys = [], []
        for (tok, p1, p2, root, step, wafer, stime), samples in groups.items():
            if not samples:
                continue

            # rot majority
            rots = [int(x.get('rot',5) or 5) for x in samples]
            rot = Counter(rots).most_common(1)[0][0] if rots else 5
            for s in samples:
                s["rot"] = rot

            day = (stime.split('_')[0] if (stime and '_' in stime) else "NA")
            out_dir = os.path.join(base_root, _safe_prefix(p1), _safe_prefix(p2), day)
            os.makedirs(out_dir, exist_ok=True)
            wafer_for_name = wafer[1:] if str(wafer).startswith("W") else wafer
            out_path = os.path.join(
                out_dir,
                f"{_safe_name(root)}_{_safe_name(step)}_{_safe_name(wafer_for_name)}_{_safe_name(stime)}.png"
            )

            tasks.append((
                samples, out_path,
                self.cfg.border_thickness,
                rot,
                self.cfg.default_tile_size,
                self.cfg.draw_empty_chip_text,
                self.cfg.empty_chip_text_field,
                self.cfg.defect_border_thickness,
                positions_root
            ))
            task_keys.append((tok, p1, p2))

        if not tasks:
            return [], {}

        results = list(tqdm(self.executor.map(
            create_sample_image_func, tasks,
            chunksize=max(1, len(tasks)//(self.cfg.cpu_processes*4) or 1)
        ), total=len(tasks), desc="Generating images"))

        ok_by_key = Counter()
        for key, r in zip(task_keys, results):
            if r:
                ok_by_key[key] += 1

        return [r for r in results if r], dict(ok_by_key)

# =================== Orchestration ===================

def load_df(path: str) -> pd.DataFrame:
    i, j, k = (p-1 for p in CFG.df_positions)
    df = pd.read_csv(path, sep=None, engine='python', header=0, index_col=0).iloc[:, [i, j, k]].copy()
    df.columns = ["_token","_p1","_p2"]
    return df

def run_pipeline_for_dataframe(df: pd.DataFrame):
    if df is None or len(df) == 0:
        return {}

    token2pps = {}
    for tok, p1, p2 in df[["_token","_p1","_p2"]].itertuples(index=False, name=None):
        token2pps.setdefault(str(tok), []).append((str(p1), str(p2)))

    s3 = S3Manager(CFG)
    s3b = S3ManagerB(CFG_B) if CFG_B.enabled else None
    proc = DataProcessor(CFG)
    img = ImageGenerator(CFG)

    h0, h1 = CFG.hours_back_start, CFG.hours_back_end
    if h1 < h0:
        h0, h1 = h1, h0

    now_local = datetime.now()
    start_ts = now_local - timedelta(hours=h1)
    end_ts   = now_local - timedelta(hours=h0)

    print(f"🚀 Start {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"[window(name)] {start_ts:%Y-%m-%d %H:%M:%S} ~ {end_ts:%Y-%m-%d %H:%M:%S}")
    print(f"[tokens] n={len(token2pps)}")

    t0 = time.time()
    results = {}
    bucketb_mismatched_keys = set()
    mismatch_out_path = None
    try:
        folders = s3.get_top_level_folders()
        print(f"[folders] total={len(folders)}")
        if not folders:
            print("No folders.")
            return results

        selected = select_folders_by_window(folders, start_ts, end_ts)
        print(f"[folders] selected={selected}")

        # bucketB mismatch log: chunk마다 append (실행 폴더에 1개 파일로 계속 저장)
        window_start_s = start_ts.strftime("%Y-%m-%d %H:%M:%S")
        window_end_s = end_ts.strftime("%Y-%m-%d %H:%M:%S")
        if s3b:
            mismatch_out_path = os.path.join(
                str(Path(__file__).resolve().parent),
                f"bucketb_mismatch_{Path(__file__).stem}_{datetime.now():%Y%m%d_%H%M%S}.txt",
            )
            with open(mismatch_out_path, "w", encoding="utf-8", newline="\n") as f:
                f.write("window_start\twindow_end\tchunk_idx\tchunk_total\tchunk_keys\tchunk_fail\ta_key\treason\n")
            print(f"[bucketB] mismatch_log={mismatch_out_path}")

        # 1차 필터: 파일명(basename)에서 token+시간 뽑아 시간창으로 key 선정
        key_to_token, pf_stats = s3.prefilter_keys_by_filename(
            selected, token2pps, CFG.folder_filter_middle, start_ts, end_ts,
        )
        print(f"[prefilter(filename)] scanned={pf_stats.get('scanned',0)}  token_hit={pf_stats.get('token_hit',0)}  "
              f"time_hit={pf_stats.get('time_hit',0)}  kept={pf_stats.get('kept',0)}")

        matched_keys = list(key_to_token.keys())
        if not matched_keys:
            print("[prefilter] no keys; nothing to do.")
            return results

        chunk_size = CFG.chunk_size
        total_chunks = (len(matched_keys) + chunk_size - 1) // chunk_size
        print(f"[chunks] global chunks={total_chunks}, chunk_size={chunk_size}")

        for idx, off in enumerate(range(0, len(matched_keys), chunk_size), 1):
            part_keys = matched_keys[off:off+chunk_size]
            print(f"\n🔥 Global Chunk {idx}/{total_chunks} size={len(part_keys)} (now={datetime.now():%Y-%m-%d %H:%M:%S})")
            t_chunk = time.time()

            # Bucket B 매칭(먼저): A key -> bucket_b_match meta
            bucket_b_match_map = {}
            if s3b:
                bucket_b_match_map = build_bucket_b_match_map_prefixlist(part_keys, s3b, CFG_B, chunk_label=f"{idx}/{total_chunks}")

            contents = s3.download_and_decompress_parallel(part_keys)
            if not contents:
                print("  -> empty chunk")
                continue

            # token은 key_to_token에서, pair는 :DEVICE=로 결정
            tagged_pairs = []
            for name, text in contents:
                orig_key = name.split("::", 1)[0]
                tok = key_to_token.get(orig_key)
                if tok is None:
                    continue
                lines = text.splitlines()
                device_val = _hval(lines, ":DEVICE=", max_lines=200)
                p1, p2 = choose_pair_by_device(tok, device_val, token2pps)
                tagged_pairs.append((tok, p1, p2, name, text))
            del contents

            dataset_all = proc.process_files_parallel_tagged(tagged_pairs)

            # bucket_b_match 주입 (positions json에서 성공/실패 확인용)
            if bucket_b_match_map:
                for s in dataset_all:
                    akey = s.get("orig_key") or ""
                    meta = bucket_b_match_map.get(akey)
                    if meta is not None:
                        s["bucket_b_match"] = meta

            # 2차 필터: 본문 STIME 기반 시간창 필터
            before = len(dataset_all)
            if (h0 != 0 or h1 != 0):
                kept = []
                for s in dataset_all:
                    dt = _parse_stime_dt(s.get('stime'))
                    if dt is not None:
                        kept.append(s)
                dataset_all = kept
            after = len(dataset_all)
            print(f"[stime-filter] kept {after}/{before}")
            if not dataset_all:
                print("  -> no dataset in window")
                continue

            imgs, img_ok_by_key = img.generate_images_mixed(dataset_all, base_root=CFG.base_root, positions_root=CFG.positions_root)

            # simple accumulate
            from collections import Counter, defaultdict
            ds_count_by_key = Counter((s.get('token','NA'), s.get('p1','NA'), s.get('p2','NA')) for s in dataset_all)
            for (tok, p1, p2), n in ds_count_by_key.items():
                k = (f"{p1}/{p2}", str(tok), p1, p2)
                results.setdefault(k, {"dataset_size": 0, "image_count": 0})
                results[k]["dataset_size"] += n
                results[k]["image_count"]  += img_ok_by_key.get((tok, p1, p2), 0)

            # chunk 종료 시 bucketB 매칭 성공/실패 요약 출력
            if bucket_b_match_map:
                _succ = sum(1 for v in bucket_b_match_map.values() if v.get("matched"))
                _fail = len(bucket_b_match_map) - _succ
                _read_ok = sum(1 for v in bucket_b_match_map.values() if v.get("matched") and v.get("first_line_ok"))

                # mismatch: chunk마다 파일에 append 저장 (원본 A key만)
                mismatch_this = []
                for _ka, _meta in bucket_b_match_map.items():
                    if not (_meta or {}).get("matched"):
                        mismatch_this.append(_ka)
                        bucketb_mismatched_keys.add(_ka)

                if mismatch_out_path and mismatch_this:
                    lines = []
                    for _ka in mismatch_this:
                        _meta = bucket_b_match_map.get(_ka) or {}
                        _reason = _meta.get("reason") or "not_found"
                        lines.append(
                            f"{window_start_s}\t{window_end_s}\t{idx}\t{total_chunks}\t{len(part_keys)}\t{_fail}\t{_ka}\t{_reason}\n"
                        )
                    with open(mismatch_out_path, "a", encoding="utf-8", newline="\n") as f:
                        f.write("".join(lines))

                print(
                    f"  -> chunk done in {round(time.time()-t_chunk, 2)}s  [bucketB] 성공={_succ} 실패={_fail} read_ok={_read_ok}/{_succ if _succ else 0}"
                    + (f"  mismatch_append={len(mismatch_this)} total_saved={len(bucketb_mismatched_keys)}" if mismatch_out_path else "")
                )
            else:
                print(f"  -> chunk done in {round(time.time()-t_chunk, 2)}s  [bucketB] disabled_or_empty")

        total_secs = round(time.time()-t0, 2)

        # cleanup 복구
        p1_set = set()
        for pairs in token2pps.values():
            for (p1, p2) in pairs:
                if p1 and p1 != "NA":
                    p1_set.add(p1)
        removed = _cleanup_empty_p2_and_dates(CFG.base_root, p1_set)
        if removed:
            print(f"[cleanup] removed {removed} empty dirs")

        # mismatch 파일은 chunk마다 append 저장되며, 여기서는 요약만 출력
        if s3b and mismatch_out_path:
            print(f"[bucketB] mismatch_keys_total={len(bucketb_mismatched_keys)} saved={mismatch_out_path}")

        print(f"\n✅ Global done in {total_secs}s")
        print("\n🎯 Results by (prefix, token, p1, p2)")
        for k, v in results.items():
            print(" ", k, "->", v)

        return results

    finally:
        try:
            proc.close()
        except:
            pass
        try:
            img.close()
        except:
            pass

# =================== Main ===================

if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass
    setup_environment()
    df = load_df(CFG.df_path)
    if df is not None and len(df) > 0:
        run_pipeline_for_dataframe(df)
    else:
        print("No tokens found in DataFrame; nothing to do.")