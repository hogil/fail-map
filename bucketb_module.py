#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bucket B (Secondary) 매칭 모듈
- prefix-list + fallback(closest time) 방식
- 260122_1544_fail-map-bucketb-prefixlist-fallback.py 에서 추출
"""

import os, re, gzip, time
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

from utils import create_s3_client, decode_best_effort, split_key_and_inner


# =================== Bucket B Config ===================

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


def parse_bucket_a_key(key: str):
    """
    예)
      A: 20260122/01_3BC170H3-00P_N_20260122_022718.Z
    반환:
      dict or None
    """
    k = split_key_and_inner(key)
    parts = k.split("/", 1)
    if len(parts) != 2:
        return None
    folder = parts[0]
    basename = parts[1]
    m = _RX_A_BASENAME.match(os.path.basename(basename))
    if not m:
        return None
    wafer_num = m.group("wafer")
    lot_id = f"{m.group('lotbase')}{m.group('suffix')}"
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
        key_b1 = f"{folder_a}/{info['lot_id']}_{info['wafer_w']}_{day}_{hhmmss}{CFG_B.file_ext}"
        yield key_b1, int(off)
        folder_dt = day
        if folder_dt != folder_a:
            key_b2 = f"{folder_dt}/{info['lot_id']}_{info['wafer_w']}_{day}_{hhmmss}{CFG_B.file_ext}"
            yield key_b2, int(off)


def generate_bucket_b_prefixes(info: dict, offset_range=(-10, 10)):
    """
    list_objects_v2 를 아주 좁은 prefix로 호출하기 위한 prefix 생성.
    """
    folder_a = info.get("folder") or info.get("date")
    if not folder_a:
        return
    days = {str(folder_a)}
    try:
        dt0 = datetime.strptime(f"{info['date']}_{info['time']}", "%Y%m%d_%H%M%S")
        lo, hi = int(offset_range[0]), int(offset_range[1])
        if hi < lo:
            lo, hi = hi, lo
        days.add((dt0 + timedelta(seconds=lo)).strftime("%Y%m%d"))
        days.add((dt0 + timedelta(seconds=hi)).strftime("%Y%m%d"))
    except:
        pass
    for d in sorted(days):
        yield f"{d}/{info['lot_id']}_{info['wafer_w']}_"


# =================== Bucket B Content Parsing ===================

def parse_bucket_b_content(text):
    """
    Bucket B 파일 전체 내용 파싱.
    - FTN= 여러 줄 → ftn_keys 리스트
    - QTN= 여러 줄 → qtn_keys 리스트
    - 칩별 X= Y= 블록에서 F=, Q= 값 추출 → FTN/QTN 매핑
    """
    lines = text.splitlines()
    if not lines:
        return {"first_line": "", "ftn_keys": [], "qtn_keys": [], "chip_data": {}}

    first_line = lines[0]

    # 1) FTN= / QTN= 헤더 수집 (여러 줄 이어붙이기)
    ftn_keys = []
    qtn_keys = []
    for ln in lines:
        s = ln.strip()
        if s.startswith("FTN="):
            ftn_keys.extend(s[4:].split())
        elif s.startswith("QTN="):
            qtn_keys.extend(s[4:].split())

    # 2) 칩별 데이터: X= Y= → F= / Q=
    chip_data = {}
    i = 0
    while i < len(lines):
        m = re.match(r'X=\s*(\S+)\s+Y=\s*(\S+)', lines[i].strip())
        if m:
            x_val = str(int(m.group(1)))
            y_val = str(int(m.group(2)))
            chip_key = f"{x_val}_{y_val}"
            f_vals = []
            q_vals = []
            j = i + 1
            while j < len(lines):
                nxt = lines[j].strip()
                if re.match(r'X=\s*(\S+)\s+Y=\s*(\S+)', nxt):
                    break
                if nxt.startswith("F="):
                    f_vals = nxt[2:].split()
                elif nxt.startswith("Q="):
                    q_vals = nxt[2:].split()
                j += 1
            ftn_map = {k: (f_vals[idx] if idx < len(f_vals) else "") for idx, k in enumerate(ftn_keys)}
            qtn_map = {k: (q_vals[idx] if idx < len(q_vals) else "") for idx, k in enumerate(qtn_keys)}
            chip_data[chip_key] = {"FTN": ftn_map, "QTN": qtn_map}
        i += 1

    # 3) TM 추출 (1번째 줄), LT 추출 (5번째 줄)
    tm = ""
    if len(lines) > 0:
        tm_m = re.search(r'TM=(\S+)', lines[0])
        if tm_m:
            tm = tm_m.group(1)
    lt = ""
    if len(lines) > 4:
        lt_m = re.search(r'LT=(\S{2})', lines[4])
        if lt_m:
            lt = lt_m.group(1)

    return {
        "first_line": first_line,
        "tm": tm,
        "lt": lt,
        "ftn_keys": ftn_keys,
        "qtn_keys": qtn_keys,
        "chip_data": chip_data,
    }


# =================== S3ManagerB ===================

class S3ManagerB:
    def __init__(self, cfg: BucketBConfig):
        self.cfg = cfg
        self.client = create_s3_client(cfg)

    def list_keys_with_prefix(self, prefix: str):
        out = []
        token = None
        while True:
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

    def read_gz_full(self, key: str) -> str:
        try:
            obj = self.client.get_object(Bucket=self.cfg.bucket_name, Key=key)
            raw = gzip.decompress(obj["Body"].read())
            return decode_best_effort(raw)
        except:
            return ""

    def read_gz_first_line(self, key: str, max_bytes: int = 65536) -> str:
        try:
            obj = self.client.get_object(Bucket=self.cfg.bucket_name, Key=key)
            body = obj["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                line = gz.readline(max_bytes)
            if not line:
                return ""
            return decode_best_effort(line).rstrip("\r\n")
        except:
            return ""


# =================== Main Match Function ===================

def build_bucket_b_match_map_prefixlist(part_keys_a, s3b: S3ManagerB, cfg_b: BucketBConfig, chunk_label: str = ""):
    """
    returns: dict[a_key] -> match_meta
    """
    t0 = time.time()
    lo, hi = int(cfg_b.time_offset_range[0]), int(cfg_b.time_offset_range[1])
    if hi < lo:
        lo, hi = hi, lo
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

    # 2) prefix 별 list (prefix -> keys) + 전체 key set
    prefix_list = sorted(prefixes)
    prefix_to_keys = {p: [] for p in prefix_list}
    all_b_keys_set = set()
    all_listed = 0
    if prefix_list:
        with ThreadPoolExecutor(max_workers=cfg_b.list_max_workers) as ex:
            for p, keys in zip(prefix_list, ex.map(s3b.list_keys_with_prefix, prefix_list)):
                ks = keys or []
                prefix_to_keys[p] = ks
                all_listed += len(ks)
                for k in ks:
                    all_b_keys_set.add(k)

    # 2.5) prefix -> (sorted dts, keys) (fallback: closest time)
    prefix_to_dt_keys = {}
    for p, keys in prefix_to_keys.items():
        arr = []
        for kb in keys:
            bn = os.path.basename(str(kb))
            if not bn.lower().endswith(cfg_b.file_ext):
                continue
            base = bn[: -len(cfg_b.file_ext)]
            parts = base.split("_")
            if len(parts) < 4:
                continue
            date = parts[-2]
            hhmmss = parts[-1]
            try:
                dt_b = datetime.strptime(f"{date}_{hhmmss}", "%Y%m%d_%H%M%S")
            except:
                continue
            arr.append((dt_b, kb))
        if arr:
            arr.sort(key=lambda x: x[0])
            dts = [x[0] for x in arr]
            kbs = [x[1] for x in arr]
        else:
            dts, kbs = [], []
        prefix_to_dt_keys[p] = (dts, kbs)

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
                "time_offset_range": [lo, hi],
                "expected_key0": "",
                "expected_range": "",
            }
            continue

        expected_key0 = ""
        expected_range = ""
        try:
            dt0 = datetime.strptime(f"{info['date']}_{info['time']}", "%Y%m%d_%H%M%S")
            dt_lo = dt0 + timedelta(seconds=lo)
            dt_hi = dt0 + timedelta(seconds=hi)
            folder_a = info.get("folder") or info.get("date") or dt0.strftime("%Y%m%d")
            expected_key0 = f"{folder_a}/{info['lot_id']}_{info['wafer_w']}_{dt0:%Y%m%d}_{dt0:%H%M%S}{cfg_b.file_ext}"
            expected_range = f"{dt_lo:%Y%m%d_%H%M%S}~{dt_hi:%Y%m%d_%H%M%S}"
        except:
            pass

        # (A) 예상 시간범위 내 후보 key 확인 (빠른 경로)
        hit_key = None
        hit_off = None
        for kb, off in generate_bucket_b_candidate_keys(info, cfg_b.time_offset_range):
            if kb in all_b_keys_set:
                hit_key = kb
                hit_off = int(off)
                break

        # (B) fallback: 같은 prefix 중에서 A와 가장 가까운 시간 1개 선택
        fallback_key = None
        fallback_diff = None
        had_candidates = False
        if not hit_key:
            try:
                dt_a = datetime.strptime(f"{info['date']}_{info['time']}", "%Y%m%d_%H%M%S")
            except:
                dt_a = None
            if dt_a is not None:
                best = None
                best_key = None
                best_diff = None
                prefixes_a = list(generate_bucket_b_prefixes(info, cfg_b.time_offset_range))
                for pfx in prefixes_a:
                    dts, kbs = prefix_to_dt_keys.get(pfx, ([], []))
                    if not dts:
                        continue
                    had_candidates = True
                    i = bisect_left(dts, dt_a)
                    for j in (i - 1, i):
                        if 0 <= j < len(dts):
                            diff = int(round((dts[j] - dt_a).total_seconds()))
                            score = (abs(diff), 0 if diff >= 0 else 1)
                            if best is None or score < best:
                                best = score
                                best_key = kbs[j]
                                best_diff = diff
                fallback_key = best_key
                fallback_diff = best_diff

        if hit_key:
            matched += 1
            match_map[ka] = {
                "matched": True,
                "bucket": cfg_b.bucket_name,
                "method": "prefix_list_fallback",
                "a_key": ka,
                "key": hit_key,
                "offset_sec": hit_off,
                "time_offset_range": [lo, hi],
                "expected_key0": expected_key0,
                "expected_range": expected_range,
                "reason": "predicted_hit",
                "first_line": "",
                "first_line_ok": False,
            }
        elif fallback_key:
            matched += 1
            match_map[ka] = {
                "matched": True,
                "bucket": cfg_b.bucket_name,
                "method": "prefix_list_fallback",
                "a_key": ka,
                "key": fallback_key,
                "offset_sec": int(fallback_diff) if fallback_diff is not None else 0,
                "time_offset_range": [lo, hi],
                "expected_key0": expected_key0,
                "expected_range": expected_range,
                "reason": "closest_time",
                "first_line": "",
                "first_line_ok": False,
            }
        else:
            match_map[ka] = {
                "matched": False,
                "bucket": cfg_b.bucket_name,
                "method": "prefix_list_fallback",
                "a_key": ka,
                "time_offset_range": [lo, hi],
                "expected_key0": expected_key0,
                "expected_range": expected_range,
                "reason": ("no_listed_keys" if not had_candidates else "not_found"),
            }

    # 4) full content read + parse (matched only)
    matched_keys = [m["key"] for m in match_map.values() if m.get("matched") and m.get("key")]
    parsed_by_key = {}
    if matched_keys:
        def _read_one(k):
            return k, s3b.read_gz_full(k)
        with ThreadPoolExecutor(max_workers=cfg_b.firstline_max_workers) as ex:
            for k, content in ex.map(_read_one, matched_keys):
                parsed_by_key[k] = parse_bucket_b_content(content)
        for meta in match_map.values():
            if meta.get("matched") and meta.get("key"):
                parsed = parsed_by_key.get(meta["key"], {})
                meta["first_line"] = parsed.get("first_line", "")
                meta["first_line_ok"] = bool(parsed.get("first_line"))
                meta["bucket_b_parsed"] = parsed

    parse_failed = sum(1 for m in match_map.values() if m.get("reason") == "parse_failed")
    firstline_ok = sum(1 for m in match_map.values() if m.get("matched") and m.get("first_line_ok"))
    lbl = f" {chunk_label}" if chunk_label else ""
    read_part = (f" read_ok={firstline_ok}/{matched}" if matched else " read_ok=0")
    print(
        f"[bucketB(prefix-list){lbl}] match성공={matched}/{len(part_keys_a)}"
        f"{read_part} parse_failed={parse_failed} prefixes={len(prefixes)} listed_keys={all_listed}"
        f" in {time.time()-t0:.2f}s"
    )
    return match_map
