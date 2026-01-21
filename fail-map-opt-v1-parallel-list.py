#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fail-map-opt-v1-parallel-list.py

Optimization Strategy: Parallel Listing & In-Memory Join
1. Bucket A (Main): Download & Parse (CPU)
2. Bucket B (Sub): 
   - Extract Lot/Wafer from parsed data (preserve original filename).
   - Run parallel `list_objects_v2` for each Lot/Wafer prefix (e.g., "YYYYMMDD/ABC123P_W01_").
   - Filter files in memory (Time difference 0~10s).
   - Download matched files (.gz) in parallel.
3. Merge & Generate Image.
"""

import os, re, sys, time, json, io, zipfile, tempfile, shutil, gzip, tarfile, subprocess, importlib, multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
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
    secondary_bucket_name: str = 'eds.m-eds-map-raw'  # [NEW] Secondary Bucket
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

# =================== Env / Cython ===================

def setup_environment():
    cores = multiprocessing.cpu_count()
    for k in ("NUMEXPR_MAX_THREADS","NUMEXPR_NUM_THREADS","OMP_NUM_THREADS",
              "OPENBLAS_NUM_THREADS","MKL_NUM_THREADS","VECLIB_MAXIMUM_THREADS"):
        os.environ[k] = str(cores)
    print(f"[env] Threads={cores}")

def get_cython_convert_hex():
    importlib.invalidate_caches()
    try:
        import cython_functions
        return cython_functions.convert_hex_values_cython
    except ImportError:
        def _fallback(lines, start_idx, xsize, ysize): return ""
        return _fallback

# =================== Utils ===================

def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "", (s or "NA").strip()) or "NA"

def _safe_prefix(*parts: str) -> str:
    return "/".join(re.sub(r"[^A-Za-z0-9._-]+", "", str(p)) for p in parts if str(p).strip())

def select_folders_by_window(folders, a, b):
    if b < a: a, b = b, a
    want = {(a + timedelta(days=i)).strftime("%y%m%d") for i in range((b.date()-a.date()).days+1)}
    want |= {w if len(w)==8 else "20"+w for w in want}
    return [f for f in folders if f.rstrip("/").split("/")[-1] in want]

def _cleanup_empty_p2_and_dates(base_root: str, p1_set: set) -> int:
    return 0

# =================== Palette ===================

def _hex_to_rgb(hex_code: str):
    s = (hex_code or "").strip()
    if not (s.startswith("#") and len(s) == 7): return [0, 0, 0]
    return [int(s[1:3],16), int(s[3:5],16), int(s[5:7],16)]

def _flatten_palette_by_keys(color_map, index_to_key, total_colors=32):
    rgb = []
    for key in index_to_key:
        rgb.extend(_hex_to_rgb(color_map.get(key, "#000000")))
    need = total_colors * 3 - len(rgb)
    if need > 0: rgb.extend([0] * need)
    return rgb

try:
    with open(CFG.color_json, 'r', encoding='utf-8') as f:
        _cd = json.load(f)
    _d, _top, _btm = _cd['default'], _cd['default']['top'], _cd['default'].get('bottom', {})
except:
    _top, _btm, _d = {}, {}, {}

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
    "chip0","chip1","chip2","chip3","chip4","chip5","chip6","chip7",
    "border","border_inv","border_b285","border_b286","border_b287","border_b288",
    "bg","text"
]
KEY_TO_INDEX = {k:i for i,k in enumerate(PALETTE_INDEX_TO_KEY)}
IDX_INVALID_FILL = 31
IDX_BG, IDX_BORDER, IDX_BORDER_INV, IDX_TEXT = 14, 8, 9, 15
IDX_B_DEF = {"285": 10, "286": 11, "287": 12, "288": 13}
PALETTE_32 = _flatten_palette_by_keys(PALETTE_HEX_MAP, PALETTE_INDEX_TO_KEY, 32)
PALETTE_32[IDX_INVALID_FILL*3:IDX_INVALID_FILL*3+3] = _hex_to_rgb("#FFFFFF")

# =================== Header parsing helpers ===================

def _hval(lines, key, max_lines=200):
    for ln in lines[:max_lines]:
        if ln.startswith(key): return ln.split("=",1)[1].strip()
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
    try: return datetime.strptime(st, "%Y%m%d_%H%M%S")
    except: return None

# =================== Parsing ===================

def choose_pair_by_device(token, device_value, token2pps):
    pairs = token2pps.get(str(token), [])
    if not pairs: return ("NA", "NA")
    devU = (device_value or "").upper()
    for (p1, p2) in pairs:
        if p1 and (p1.upper() in devU): return (p1, p2)
    return ("NA", "NA")

def find_initial_values_from_lines(lines, file_name):
    wfid = lines[1].split('=')[1].strip()
    base = os.path.basename((file_name or "").split("::")[-1])
    root = (base.split('-', 1)[0] if '-' in base else base).upper()
    step, wafer = wfid.split('-',1)[1].split('.',1)
    xsize = int(lines[11].split('=')[1])
    ysize = int(lines[12].split('=')[1])
    rot = 5
    if len(lines) > 8 and '=' in lines[8]:
        try: rot = int(lines[8].split('=',1)[1].strip())
        except: rot = 5
    start, line_offset = None, 1
    for i in range(28, 40):
        if i < len(lines) and lines[i].startswith('X='):
            start = i
            if i+1 < len(lines) and lines[i+1].startswith('mft'): line_offset = 2
            break
    if start is None: raise ValueError("X= start not found")
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
    if not file_content: return []
    lines = file_content.splitlines()
    if not lines: return []
    stime  = _parse_stime(lines)
    partid = _hval(lines, ":PARTID=")
    tester = _hval(lines, ":TESTER=")
    device = _hval(lines, ":DEVICE=")
    pgm    = _hval(lines, ":PGM=")
    try: xsize, ysize, start, root, step, wafer, rot = find_initial_values_from_lines(lines, file_name)
    except: return []
    convert_hex_values = get_cython_convert_hex()
    last_by_xy = {}
    i = start
    while i < len(lines):
        if not lines[i].startswith('X='):
            i += 1
            continue
        m = dict(re.findall(r'([XYbB])\s*=\s*([-\w]+)', lines[i].strip()))
        try: cx = int(m.get('X', '0')); cy = int(m.get('Y', '0'))
        except: parts = lines[i].split(); cx = int(parts[1]) if len(parts)>1 else 0; cy = int(parts[3]) if len(parts)>3 else 0
        cb = (m.get('b') or m.get('B') or '').strip()
        j = i + 1
        if j < len(lines) and lines[j].startswith('mft'): j += 1
        hex_block = ""
        if j < len(lines) and lines[j].startswith('#'):
            try: hex_block = convert_hex_values(lines, j, xsize, ysize)
            except: hex_block = ""
        last_by_xy[(cx, cy)] = {
            "root": root, "step": step, "wafer": wafer,
            "x": cx, "y": cy, "b": cb, "transformed_values": hex_block,
            "stime": stime, "rot": rot,
            "partid": partid, "tester": tester, "device": device, "pgm": pgm
        }
        i += 1
    return list(last_by_xy.values())

# =================== Image generation ===================

_FONT_CACHE = {}
def _ttf_cached(w, h, text):
    key = (w, h, len(text))
    if key in _FONT_CACHE: return _FONT_CACHE[key]
    sz = max(8, min(w, h))
    f = ImageFont.load_default()
    _FONT_CACHE[key] = f
    return f

def map_tile_after_rotation(i0, j0, rot_code, tilesW_after, tilesH_after):
    if rot_code == 7: return (j0, tilesH_after - 1 - i0)
    elif rot_code == 3: return (tilesW_after - 1 - j0, i0)
    elif rot_code == 0: return (tilesW_after - 1 - i0, tilesH_after - 1 - j0)
    else: return (i0, j0)

def centerize_col(i, W): return i - (W//2 - 1) if (W % 2 == 0) else i - (W//2)
def centerize_row(j, H): return j - (H//2)

def create_sample_image_func(args):
    (samples, output_path, border_thin, rot, default_tile_size,
     draw_empty_text, empty_text_field, defect_border_thickness, positions_root) = args
    if not samples: return None
    xs = [s['x'] for s in samples]; ys = [s['y'] for s in samples]
    x_min, x_max, y_min, y_max = min(xs), max(xs), min(ys), max(ys)
    tiles_w = x_max - x_min + 1
    tiles_h = y_max - y_min + 1
    first_valid = next((s for s in samples if s.get('transformed_values')), None)
    if first_valid and first_valid.get('transformed_values'):
        sr = first_valid['transformed_values'].split(',')
        sh = len(sr); sw = len(sr[0]) if sh > 0 else 0
        if sh == 0 or sw == 0: sh, sw = default_tile_size
    else: sh, sw = default_tile_size
    H0, W0 = tiles_h * sh, tiles_w * sw
    idx0 = np.full((H0, W0), IDX_BG, dtype=np.uint8)
    vmap, bmap, have = {}, {}, set()
    for s in samples:
        i, j = s['x'] - x_min, s['y'] - y_min
        have.add((i, j))
        rs = s.get('transformed_values') or ""
        rows = rs.split(',') if rs else []
        ok = (len(rows) == sh) and (sh > 0) and all(len(r) == sw for r in rows)
        vmap[(i, j)] = ok
        bmap[(i, j)] = (s.get('b') or "").strip()
        y0, y1 = j * sh, (j + 1) * sh
        x0, x1 = i * sw, (i + 1) * sw
        if ok:
            vals = np.frombuffer(''.join(rows).encode('ascii'), dtype=np.uint8) - ord('0')
            idx0[y0:y1, x0:x1] = vals.reshape(sh, sw)
        else: idx0[y0:y1, x0:x1] = IDX_INVALID_FILL
    rot_code = int(rot) if rot is not None else 5
    if rot_code == 7: idxR = np.transpose(idx0, (1, 0))[::-1, :]; tilesW_after, tilesH_after = tiles_h, tiles_w
    elif rot_code == 3: idxR = np.transpose(idx0, (1, 0))[:, ::-1]; tilesW_after, tilesH_after = tiles_h, tiles_w
    elif rot_code == 0: idxR = idx0[::-1, ::-1]; tilesW_after, tilesH_after = tiles_w, tiles_h
    else: idxR = idx0; tilesW_after, tilesH_after = tiles_w, tiles_h
    del idx0
    imgP = Image.fromarray(idxR, mode='P')
    imgP.putpalette(PALETTE_32)
    wR, hR = imgP.size
    if wR == hR: imgS, sx, sy = imgP, 1.0, 1.0
    elif wR < hR: S = wR; imgS = imgP.resize((S, S), resample=Image.NEAREST); sx, sy = 1.0, (wR / hR)
    else: S = hR; imgS = imgP.resize((S, S), resample=Image.NEAREST); sx, sy = (hR / wR), 1.0
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
        if vmap.get((ii0, jj0), False): continue
        ii, jj = map_tile_after_rotation(ii0, jj0, rot_code, tilesW_after, tilesH_after)
        x0, x1 = xs_pix[ii], xs_pix[ii + 1]
        y0, y1 = ys_pix[jj], ys_pix[jj + 1]
        arr[y0:y1, x0:x1] = IDX_INVALID_FILL
    base_img = Image.fromarray(arr, mode='P')
    base_img.putpalette(PALETTE_32)
    draw = ImageDraw.Draw(base_img)
    d = int(max(1, defect_border_thickness))
    _save_indexed32_png(base_img, output_path)

    meta0 = samples[0]
    p1 = meta0.get("p1","NA"); p2 = meta0.get("p2","NA")
    stime = str(meta0.get("stime",""))
    day = (stime.split('_')[0] if (stime and '_' in stime) else "NA")
    extra_info = meta0.get("extra_info", {})

    Ws, Hs = base_img.size
    tiles_w_rot, tiles_h_rot = tilesW_after, tilesH_after
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
    json_obj = {
        "image_path": output_path,
        "root": meta0.get("root",""),
        "step": meta0.get("step",""),
        "wafer": meta0.get("wafer",""),
        "stime": stime,
        "day": day,
        "extra_info": extra_info,
        "coord": {
            "rot_code": int(rot_code),
            "x_min_abs": int(x_min), "y_min_abs": int(y_min),
            "x_max_abs": int(x_max), "y_max_abs": int(y_max),
            "tiles_w_rot": int(tiles_w_rot), "tiles_h_rot": int(tiles_h_rot),
            "grid_edges": {"xs": xs_edges, "ys": ys_edges},
            "canvas": {"width": int(Ws), "height": int(Hs)},
            "scale": {"sx": float(sx), "sy": float(sy)},
            "border": int(border_thin),
            "defect_border": int(defect_border_thickness),
            "center_rule": {"even_x_zero": "left", "even_y_zero": "down"}
        },
        "chips": chips_json
    }
    json_dir = os.path.join(positions_root, _safe_prefix(p1), _safe_prefix(p2), day)
    os.makedirs(json_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(output_path))[0]
    json_path = os.path.join(json_dir, base_name + ".json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_obj, f, ensure_ascii=False, indent=2)
    return output_path

def _save_indexed32_png(img, path):
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    img.save(path, format="PNG", optimize=False, compress_level=1)

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
                        if key.endswith(file_pattern): out.append((key, obj.get('LastModified')))
                    for cp in page.get('CommonPrefixes', []) or []: st.append(cp.get('Prefix'))
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
        key_to_token = {}
        window_on = (start_dt is not None and end_dt is not None and (start_dt != end_dt))
        for key in keys:
            bn = os.path.basename(key)
            hit_tok = None
            name_dt = None
            for tok, rx in rx_map.items():
                m = rx.search(bn)
                if not m: continue
                hit_tok = tok
                if window_on:
                    try: name_dt = datetime.strptime(f"{m.group('d')}_{m.group('t')}", "%Y%m%d_%H%M%S")
                    except: name_dt = None
                break
            if not hit_tok: continue
            if window_on:
                if name_dt is None: continue
                if not (start_dt <= name_dt <= end_dt): continue
            key_to_token[key] = hit_tok
        return key_to_token, {}

    def download_and_decompress_parallel(self, keys):
        def _one(key):
            try:
                body = self.client.get_object(Bucket=self.cfg.bucket_name, Key=key)['Body'].read()
                return [(key, body.decode('utf-8', errors='ignore'))]
            except: return []
        allc = []
        with ThreadPoolExecutor(max_workers=self.cfg.download_threads) as ex:
            for part in ex.map(_one, keys): allc.extend(part)
        return allc

    # [NEW] Parallel Listing & Filtering for Secondary Bucket
    def fetch_secondary_data_parallel(self, dataset_A):
        """
        1. Identify unique Lot/Wafer/Date from dataset_A keys.
        2. Parallel ListObjectsV2 on Secondary Bucket using specific prefix.
        3. Filter files by Time Difference (0~10s).
        4. Download matched files.
        """
        if not dataset_A: return {}

        # Helper: Extract search key info from dataset item
        def _get_search_prefix(d):
            orig_name = d.get('orig_name', '')
            if not orig_name: return None, None
            
            # Filename: 01_ABC123-00P_N_20260121_025936.Z
            try:
                base = os.path.basename(orig_name)
                parts = base.split('_')
                if len(parts) < 4: return None, None
                
                wafer_num = parts[0]  # "01"
                lot_part = parts[1]   # "ABC123-00P"
                date_part = parts[-2] # "20260121"
                
                # Lot conversion: ABC123-00P -> ABC123P
                if "-00" in lot_part:
                    lot_base, suffix = lot_part.split("-00", 1)
                    new_lot = lot_base + suffix
                else:
                    new_lot = lot_part
                
                new_wafer = f"W{wafer_num}"
                
                # Prefix: 20260121/ABC123P_W01_
                prefix = f"{date_part}/{new_lot}_{new_wafer}_"
                return prefix, date_part
            except:
                return None, None

        groups = {}
        for d in dataset_A:
            prefix, day = _get_search_prefix(d)
            if not prefix: continue
            if prefix not in groups: groups[prefix] = []
            groups[prefix].append(d)

        # 1. Parallel List
        found_files = []
        def _list_prefix(prefix):
            try:
                resp = self.client.list_objects_v2(Bucket=self.cfg.secondary_bucket_name, Prefix=prefix)
                return resp.get('Contents', [])
            except: return []

        with ThreadPoolExecutor(max_workers=32) as ex:
            futures = {ex.submit(_list_prefix, p): p for p in groups.keys()}
            for fut in as_completed(futures):
                found_files.extend(fut.result())

        # 2. In-Memory Filter
        lookup = {}
        for d in dataset_A:
            prefix, _ = _get_search_prefix(d)
            if not prefix: continue
            if prefix not in lookup: lookup[prefix] = []
            lookup[prefix].append(d)

        keys_to_download = []
        for obj in found_files:
            key = obj['Key'] # 20260121/ABC123P_W01_20260121_025938.gz
            try:
                base = os.path.splitext(os.path.basename(key))[0]
                ts_str = base.split('_')[-1] # 025938
                date_str = base.split('_')[-2] # 20260121
                dt_b = datetime.strptime(f"{date_str}_{ts_str}", "%Y%m%d_%H%M%S")
                
                parts = base.split('_')
                lot_wafer_part = "_".join(parts[:-2]) # ABC123P_W01
                prefix_candidate = f"{date_str}/{lot_wafer_part}_"
                
                if prefix_candidate in lookup:
                    candidates = lookup[prefix_candidate]
                    for item in candidates:
                        dt_a = _parse_stime_dt(item.get('stime'))
                        if dt_a:
                            diff = (dt_b - dt_a).total_seconds()
                            if 0 <= diff <= 10:
                                meta_key = (item.get('token'), item.get('wafer'), item.get('step'))
                                keys_to_download.append((key, meta_key))
            except: pass

        # 3. Parallel Download
        results = {}
        def _dl(args):
            k, meta_key = args
            try:
                obj = self.client.get_object(Bucket=self.cfg.secondary_bucket_name, Key=k)
                with gzip.GzipFile(fileobj=obj['Body']) as gz:
                    data = json.load(gz)
                return (meta_key, data)
            except: return None

        with ThreadPoolExecutor(max_workers=64) as ex:
            for res in ex.map(_dl, keys_to_download):
                if res:
                    m_key, data = res
                    results[m_key] = data
        return results

# =================== Processor / Generator ===================

class DataProcessor:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.executor = ProcessPoolExecutor(max_workers=cfg.cpu_processes, mp_context=multiprocessing.get_context("spawn"))
    def close(self): self.executor.shutdown(wait=True)

    def process_files_parallel_tagged(self, tagged_pairs):
        if not tagged_pairs: return []
        file_contents = [(name, text) for _, _, _, name, text in tagged_pairs]
        results = list(tqdm(self.executor.map(
            process_file_content, file_contents,
            chunksize=max(1, len(file_contents)//(self.cfg.cpu_processes*4) or 1)
        ), total=len(file_contents), desc="Processing (Cython)"))
        
        out = []
        for idx, fr in enumerate(results):
            # Inject orig_name here
            tok, p1, p2, orig_name = tagged_pairs[idx][0], tagged_pairs[idx][1], tagged_pairs[idx][2], tagged_pairs[idx][3]
            for r in fr:
                r["token"] = tok; r["p1"] = p1; r["p2"] = p2; r["orig_name"] = orig_name
                out.append(r)
        return out

class ImageGenerator:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.executor = ProcessPoolExecutor(max_workers=cfg.cpu_processes, mp_context=multiprocessing.get_context("spawn"))
    def close(self): self.executor.shutdown(wait=True)
    def generate_images_mixed(self, dataset_all, base_root, positions_root):
        from collections import defaultdict, Counter
        groups = defaultdict(list)
        for s in dataset_all:
            key = (s.get('token','NA'), s.get('p1','NA'), s.get('p2','NA'), s.get('root',''), s.get('step',''), s.get('wafer',''), s.get('stime','NA'))
            groups[key].append(s)
        tasks, task_keys = [], []
        for (tok, p1, p2, root, step, wafer, stime), samples in groups.items():
            if not samples: continue
            rots = [int(x.get('rot',5) or 5) for x in samples]
            rot = Counter(rots).most_common(1)[0][0] if rots else 5
            for s in samples: s["rot"] = rot
            day = (stime.split('_')[0] if (stime and '_' in stime) else "NA")
            out_dir = os.path.join(base_root, _safe_prefix(p1), _safe_prefix(p2), day)
            os.makedirs(out_dir, exist_ok=True)
            wafer_for_name = wafer[1:] if str(wafer).startswith("W") else wafer
            out_path = os.path.join(out_dir, f"{_safe_name(root)}_{_safe_name(step)}_{_safe_name(wafer_for_name)}_{_safe_name(stime)}.png")
            tasks.append((samples, out_path, self.cfg.border_thickness, rot, self.cfg.default_tile_size, self.cfg.draw_empty_chip_text, self.cfg.empty_chip_text_field, self.cfg.defect_border_thickness, positions_root))
            task_keys.append((tok, p1, p2))
        if not tasks: return [], {}
        results = list(tqdm(self.executor.map(create_sample_image_func, tasks, chunksize=max(1, len(tasks)//(self.cfg.cpu_processes*4) or 1)), total=len(tasks), desc="Generating images"))
        ok_by_key = Counter()
        for key, r in zip(task_keys, results):
            if r: ok_by_key[key] += 1
        return [r for r in results if r], dict(ok_by_key)

# =================== Orchestration ===================

def load_df(path: str) -> pd.DataFrame:
    try:
        i, j, k = (p-1 for p in CFG.df_positions)
        df = pd.read_csv(path, sep=None, engine='python', header=0, index_col=0).iloc[:, [i, j, k]].copy()
        df.columns = ["_token","_p1","_p2"]
        return df
    except: return None

def run_pipeline_for_dataframe(df: pd.DataFrame):
    if df is None or len(df) == 0: return {}
    token2pps = {}
    for tok, p1, p2 in df[["_token","_p1","_p2"]].itertuples(index=False, name=None):
        token2pps.setdefault(str(tok), []).append((str(p1), str(p2)))

    s3 = S3Manager(CFG)
    proc = DataProcessor(CFG)
    img = ImageGenerator(CFG)

    h0, h1 = CFG.hours_back_start, CFG.hours_back_end
    if h1 < h0: h0, h1 = h1, h0
    now_local = datetime.now()
    start_ts = now_local - timedelta(hours=h1)
    end_ts   = now_local - timedelta(hours=h0)

    print(f"🚀 Start {datetime.now():%Y-%m-%d %H:%M:%S}")
    
    try:
        folders = s3.get_top_level_folders()
        selected = select_folders_by_window(folders, start_ts, end_ts)
        key_to_token, pf_stats = s3.prefilter_keys_by_filename(selected, token2pps, CFG.folder_filter_middle, start_ts, end_ts)
        matched_keys = list(key_to_token.keys())
        chunk_size = CFG.chunk_size
        total_chunks = (len(matched_keys) + chunk_size - 1) // chunk_size

        for idx, off in enumerate(range(0, len(matched_keys), chunk_size), 1):
            part_keys = matched_keys[off:off+chunk_size]
            print(f"\n🔥 Chunk {idx}/{total_chunks} size={len(part_keys)}")
            
            contents = s3.download_and_decompress_parallel(part_keys)
            if not contents: continue

            tagged_pairs = []
            for name, text in contents:
                orig_key = name.split("::", 1)[0]
                tok = key_to_token.get(orig_key)
                if tok is None: continue
                lines = text.splitlines()
                device_val = _hval(lines, ":DEVICE=", max_lines=200)
                p1, p2 = choose_pair_by_device(tok, device_val, token2pps)
                tagged_pairs.append((tok, p1, p2, name, text))
            del contents

            dataset_all = proc.process_files_parallel_tagged(tagged_pairs)
            kept = []
            for s in dataset_all:
                dt = _parse_stime_dt(s.get('stime'))
                if dt is not None: kept.append(s)
            dataset_all = kept
            if not dataset_all: continue

            # [NEW] 3. Fetch Secondary Data (Parallel List & Filter)
            print("  -> Fetching secondary data...")
            t_sec = time.time()
            secondary_data_map = s3.fetch_secondary_data_parallel(dataset_all)
            print(f"  -> Secondary fetched: {len(secondary_data_map)} items in {time.time()-t_sec:.2f}s")

            for s in dataset_all:
                k = (s.get('token'), s.get('wafer'), s.get('step'))
                if k in secondary_data_map:
                    s['extra_info'] = secondary_data_map[k]

            img.generate_images_mixed(dataset_all, base_root=CFG.base_root, positions_root=CFG.positions_root)

    finally:
        try: proc.close()
        except: pass
        try: img.close()
        except: pass

if __name__ == "__main__":
    try: multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError: pass
    setup_environment()
    df = load_df(CFG.df_path)
    if df is not None: run_pipeline_for_dataframe(df)
