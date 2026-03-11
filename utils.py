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

import os, re, sys, subprocess, importlib, tempfile, multiprocessing
from datetime import datetime, timedelta
from pathlib import Path

import boto3
from botocore.config import Config
from PIL import ImageFont


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


# =================== Token → pair ===================

def choose_pair_by_device(token, device_value, token2pps):
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


# =================== Cython auto-build ===================

def _ensure_cython_built():
    """cython_functions.pyx가 있으면 자동 빌드 (.so/.pyd 없을 때만)"""
    src_dir = Path(__file__).resolve().parent
    pyx = src_dir / "cython_functions.pyx"
    if not pyx.exists():
        return
    # 이미 import 가능하면 빌드 불필요
    try:
        import cython_functions
        return
    except ImportError:
        pass
    print("[cython] cython_functions not found, building...")
    setup_py = src_dir / "setup.py"
    if not setup_py.exists():
        return
    subprocess.run(
        [sys.executable, str(setup_py), "build_ext", "--inplace"],
        cwd=str(src_dir),
        check=True,
    )
    importlib.invalidate_caches()
    print("[cython] build done")


_CYTHON_FN = None

def get_cython_convert_hex():
    """worker에서 cython 함수 1회 import 캐시 (필요 시 자동 빌드)"""
    global _CYTHON_FN
    if _CYTHON_FN is None:
        _ensure_cython_built()
        importlib.invalidate_caches()
        import cython_functions
        _CYTHON_FN = cython_functions.convert_hex_values_cython
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
