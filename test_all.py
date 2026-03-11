#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 기능 통합 테스트 (더미 데이터 사용, S3 불필요)
- 더미 파일: test_data/
- 결과 파일: test_output/
"""
import os, sys, json, re, shutil
from datetime import datetime, timedelta

_BASE = os.path.dirname(os.path.abspath(__file__))
_DATA = os.path.join(_BASE, "test_data")
_OUT = os.path.join(_BASE, "test_output")

# 이전 결과 정리 후 새로 생성
if os.path.exists(_OUT):
    shutil.rmtree(_OUT)
os.makedirs(_OUT, exist_ok=True)

positions_root = os.path.join(_OUT, "positions")
images_root = os.path.join(_OUT, "images")
os.makedirs(positions_root, exist_ok=True)
os.makedirs(images_root, exist_ok=True)

print(f"[test] data dir : {_DATA}")
print(f"[test] output dir: {_OUT}")


# ========== 1) utils.py 함수 테스트 ==========
print("\n=== 1) utils.py 함수 테스트 ===")

from utils import (
    decode_best_effort, split_key_and_inner, safe_name, safe_prefix,
    hex_to_rgb, flatten_palette_by_keys,
    hval, parse_stime, parse_stime_dt,
    choose_pair_by_device, setup_environment,
    get_cython_convert_hex, ttf_cached, save_indexed32_png,
    select_folders_by_window,
)

# decode
assert decode_best_effort(b"hello") == "hello"
print("  decode_best_effort: OK")

# split_key_and_inner
assert split_key_and_inner("20260122/file.Z::inner") == "20260122/file.Z"
assert split_key_and_inner("simple_key") == "simple_key"
print("  split_key_and_inner: OK")

# safe_name / safe_prefix
assert safe_name("test@#file") == "testfile"
assert safe_name("") == "NA"
assert safe_prefix("abc", "def") == "abc/def"
print("  safe_name / safe_prefix: OK")

# hex_to_rgb
assert hex_to_rgb("#FF0000") == [255, 0, 0]
assert hex_to_rgb("#00FF00") == [0, 255, 0]
assert hex_to_rgb("bad") == [0, 0, 0]
print("  hex_to_rgb: OK")

# flatten_palette_by_keys
pal = flatten_palette_by_keys({"a": "#FF0000"}, ["a"], total_colors=2)
assert pal[:3] == [255, 0, 0]
assert len(pal) == 6
print("  flatten_palette_by_keys: OK")

# hval
lines = [":WFID=TEST-00P.W01", ":PARTID=ABC", ":NETD=0437"]
assert hval(lines, ":PARTID=") == "ABC"
assert hval(lines, ":NETD=") == "0437"
assert hval(lines, ":MISSING=") == ""
print("  hval: OK")

# parse_stime
stime_lines = [""] * 9 + [":STIME=2026/01/22 03:14:55"]
assert parse_stime(stime_lines) == "20260122_031455"
print("  parse_stime: OK")

# parse_stime_dt
dt = parse_stime_dt("20260122_031455")
assert dt == datetime(2026, 1, 22, 3, 14, 55)
assert parse_stime_dt("bad") is None
print("  parse_stime_dt: OK")

# choose_pair_by_device
t2p = {"TOK1": [("FAB_A", "P2A"), ("FAB_B", "P2B")]}
assert choose_pair_by_device("TOK1", "FAB_A_DEVICE", t2p) == ("FAB_A", "P2A")
assert choose_pair_by_device("TOK1", "UNKNOWN", t2p) == ("NA", "NA")
assert choose_pair_by_device("TOK_X", "FAB_A", t2p) == ("NA", "NA")
print("  choose_pair_by_device: OK")

# select_folders_by_window
now = datetime(2026, 1, 22, 12, 0, 0)
folders = ["260121/", "260122/", "260123/", "260120/"]
sel = select_folders_by_window(folders, now - timedelta(hours=24), now)
assert "260121/" in sel and "260122/" in sel
print("  select_folders_by_window: OK")


# ========== 2) Cython / Python fallback 테스트 ==========
print("\n=== 2) Cython convert_hex 테스트 ===")

convert_fn = get_cython_convert_hex()

test_lines = ["#0A0B0F00", "#0A0B0F00"]
result = convert_fn(test_lines, 0, 4, 2)
rows = result.split(",")
assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
assert len(rows[0]) == 4, f"Expected 4 chars per row, got {len(rows[0])}"
assert rows[0] == "2370", f"Expected '2370', got '{rows[0]}'"
assert rows[1] == "2370"
print(f"  convert_hex result: {result}")
print("  get_cython_convert_hex (fallback): OK")


# ========== 3) 더미 Bucket A 파일 파싱 테스트 ==========
print("\n=== 3) 더미 Bucket A (raw) 파싱 테스트 ===")

with open(os.path.join(_DATA, "dummy_bucket_a.txt"), "r", encoding="utf-8") as f:
    raw_content = f.read()

raw_lines = raw_content.splitlines()
netd_raw = hval(raw_lines, ":NETD=")
netd = int(re.sub(r'\D', '', netd_raw)) if netd_raw else 0
assert netd == 4, f"Expected 4, got {netd}"
print(f"  NETD parsing: '{netd_raw}' → {netd}: OK")

assert hval(raw_lines, ":PARTID=") == "PART001"
print(f"  PARTID: {hval(raw_lines, ':PARTID=')}: OK")

assert hval(raw_lines, ":PGM=") == "TESTPGM"
print(f"  PGM: {hval(raw_lines, ':PGM=')}: OK")

assert parse_stime(raw_lines) == "20260122_031455"
print(f"  STIME: {parse_stime(raw_lines)}: OK")


# ========== 4) 더미 Bucket B 파일 파싱 테스트 ==========
print("\n=== 4) 더미 Bucket B 파싱 (FTN/QTN) 테스트 ===")

from bucketb_module import (
    parse_bucket_a_key, iter_offsets_by_closeness,
    generate_bucket_b_candidate_keys, generate_bucket_b_prefixes,
    parse_bucket_b_content,
)

with open(os.path.join(_DATA, "dummy_bucket_b.txt"), "r", encoding="utf-8") as f:
    bucket_b_content = f.read()

parsed_b = parse_bucket_b_content(bucket_b_content)

assert parsed_b["first_line"] == "HEADER LT=3A DATA=123"
print(f"  first_line: {parsed_b['first_line']}: OK")

assert parsed_b["ftn_keys"] == ["2342", "2456", "9834", "3834"]
print(f"  FTN keys: {parsed_b['ftn_keys']}: OK")

assert parsed_b["qtn_keys"] == ["5501", "5502"]
print(f"  QTN keys: {parsed_b['qtn_keys']}: OK")

assert len(parsed_b["chip_data"]) == 4
print(f"  chip count: {len(parsed_b['chip_data'])}: OK")

assert parsed_b["chip_data"]["0_0"]["FTN"] == {"2342": "2859", "2456": "2", "9834": "35", "3834": "645"}
print(f"  chip(0,0) FTN: {parsed_b['chip_data']['0_0']['FTN']}: OK")

assert parsed_b["chip_data"]["0_0"]["QTN"] == {"5501": "10", "5502": "20"}
print(f"  chip(0,0) QTN: {parsed_b['chip_data']['0_0']['QTN']}: OK")

assert parsed_b["chip_data"]["1_0"]["FTN"]["2342"] == "3000"
print(f"  chip(1,0) FTN[2342]: {parsed_b['chip_data']['1_0']['FTN']['2342']}: OK")

assert parsed_b["chip_data"]["1_1"]["FTN"]["3834"] == "720"
assert parsed_b["chip_data"]["1_1"]["QTN"]["5502"] == "23"
print(f"  chip(1,1) FTN[3834]={parsed_b['chip_data']['1_1']['FTN']['3834']}, QTN[5502]={parsed_b['chip_data']['1_1']['QTN']['5502']}: OK")


# ========== 5) positions_module 테스트 ==========
print("\n=== 5) positions_module (save_positions_json) 테스트 ===")

from positions_module import save_positions_json, map_tile_after_rotation, centerize_col, centerize_row

# map_tile_after_rotation
assert map_tile_after_rotation(1, 2, 5, 10, 10) == (1, 2)  # no rotation
assert map_tile_after_rotation(1, 2, 7, 10, 10) == (2, 8)  # 90 CCW
assert map_tile_after_rotation(1, 2, 0, 10, 10) == (8, 7)  # 180
print("  map_tile_after_rotation: OK")

# centerize
assert centerize_col(5, 10) == 1
assert centerize_row(5, 10) == 0
print("  centerize_col / centerize_row: OK")

# 더미 samples: NETD=4, 칩 4개, bucket_b_parsed 포함
samples = [
    {"x": 0, "y": 0, "b": "0100", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 4, "transformed_values": "", "rot": 5,
     "bucket_b_match": {
         "matched": True, "key": "20260122/3BC170H3P_W01_20260122_031455.gz",
         "offset_sec": 0, "first_line": parsed_b["first_line"],
         "first_line_ok": True,
         "bucket_b_parsed": parsed_b,
     }},
    {"x": 1, "y": 0, "b": "0150", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 4, "transformed_values": "", "rot": 5},
    {"x": 0, "y": 1, "b": "0285", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 4, "transformed_values": "", "rot": 5},
    {"x": 1, "y": 1, "b": "0300", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 4, "transformed_values": "", "rot": 5},
]

out_path = os.path.join(images_root, "FAB_A", "P2A", "20260122", "test.png")
os.makedirs(os.path.dirname(out_path), exist_ok=True)

save_positions_json(
    samples, out_path, positions_root,
    kind="00P", rot_code=5,
    x_min=0, x_max=1, y_min=0, y_max=1,
    tilesW_after=2, tilesH_after=2,
    canvas_w=100, canvas_h=100,
    sx=1.0, sy=1.0,
    border_thin=1, defect_border_thickness=2,
)

# JSON 결과 확인
json_path = os.path.join(positions_root, "FAB_A", "P2A", "20260122", "test.json")
assert os.path.exists(json_path), f"JSON not created: {json_path}"

with open(json_path, "r", encoding="utf-8") as f:
    jobj = json.load(f)

print(f"  JSON saved: {json_path}")

# 기본 필드
assert jobj["netd"] == 4
print(f"  netd: {jobj['netd']}: OK")

assert jobj["gd"] == 2  # b < 200: 0100, 0150
print(f"  gd: {jobj['gd']}: OK")

expected_yield = f"{2/4*100:.2f}"
assert jobj["yield"] == expected_yield
print(f"  yield: {jobj['yield']}: OK")

assert jobj["lt"] == "3A"
print(f"  lt: {jobj['lt']}: OK")

assert "match" not in jobj, "match 필드 제거됨"
print("  match 필드 없음: OK")

assert jobj["bucket_b_key"] == "20260122/3BC170H3P_W01_20260122_031455.gz"
print(f"  bucket_b_key: {jobj['bucket_b_key']}: OK")

assert jobj["pgm"] == "PGM1"
print(f"  pgm: {jobj['pgm']}: OK")

# chips
assert len(jobj["chips"]) == 4
print(f"  chips count: {len(jobj['chips'])}: OK")

# chips[0] f/q (b 바로 아래)
c0 = jobj["chips"][0]
assert c0["f"] == {"2342": "2859", "2456": "2", "9834": "35", "3834": "645"}
print(f"  chips[0] f: {c0['f']}: OK")

assert c0["q"] == {"5501": "10", "5502": "20"}
print(f"  chips[0] q: {c0['q']}: OK")

# chips[0] key 순서: b → f → q → x_cal ...
chip_keys = list(c0.keys())
assert chip_keys.index("f") == chip_keys.index("b") + 1, "f must follow b"
assert chip_keys.index("q") == chip_keys.index("f") + 1, "q must follow f"
print("  chips key order (b → f → q): OK")

# chips[3] (1,1)
c3 = jobj["chips"][3]
assert c3["f"]["2342"] == "3100"
assert c3["q"]["5502"] == "23"
print(f"  chips[3] f[2342]={c3['f']['2342']}, q[5502]={c3['q']['5502']}: OK")

assert "rect" in c0 and "x0" in c0["rect"]
print(f"  chips[0] rect: {c0['rect']}: OK")


# ========== 6) bucketb_module 기타 테스트 ==========
print("\n=== 6) bucketb_module 기타 테스트 ===")

info = parse_bucket_a_key("20260122/01_3BC170H3-00P_N_20260122_022718.Z")
assert info is not None
assert info["lot_id"] == "3BC170H3P"
assert info["wafer_w"] == "W01"
print(f"  parse_bucket_a_key: lot_id={info['lot_id']}, wafer={info['wafer_w']}: OK")

offsets = list(iter_offsets_by_closeness((-2, 2)))
assert offsets[0] == 0
assert 1 in offsets and -1 in offsets
print(f"  iter_offsets_by_closeness(-2,2): {offsets}: OK")

keys = list(generate_bucket_b_candidate_keys(info, (-1, 1)))
assert len(keys) > 0
print(f"  generate_bucket_b_candidate_keys: {len(keys)} candidates: OK")

prefixes = list(generate_bucket_b_prefixes(info, (-10, 10)))
assert len(prefixes) > 0
assert all("3BC170H3P_W01_" in p for p in prefixes)
print(f"  generate_bucket_b_prefixes: {prefixes}: OK")


# ========== 7) Bucket B match 실패 시 테스트 (f/q 없음) ==========
print("\n=== 7) Bucket B match 실패 시 positions JSON ===")

samples_no_match = [dict(s) for s in samples]
samples_no_match[0]["bucket_b_match"] = {"matched": False, "reason": "not_found"}

out_path_nm = os.path.join(images_root, "FAB_A", "P2A", "20260122", "test_nomatch.png")
save_positions_json(
    samples_no_match, out_path_nm, positions_root,
    kind="00P", rot_code=5,
    x_min=0, x_max=1, y_min=0, y_max=1,
    tilesW_after=2, tilesH_after=2,
    canvas_w=100, canvas_h=100,
    sx=1.0, sy=1.0,
    border_thin=1, defect_border_thickness=2,
)

json_nomatch = os.path.join(positions_root, "FAB_A", "P2A", "20260122", "test_nomatch.json")
with open(json_nomatch, "r", encoding="utf-8") as f:
    jnm = json.load(f)

assert jnm["lt"] == ""
assert jnm["bucket_b_key"] == ""
print(f"  lt (empty): '{jnm['lt']}': OK")
print(f"  bucket_b_key (empty): '{jnm['bucket_b_key']}': OK")

c0_nm = jnm["chips"][0]
assert "f" not in c0_nm, "match 실패 시 f 없어야 함"
assert "q" not in c0_nm, "match 실패 시 q 없어야 함"
print("  chips[0] no f/q (match실패): OK")


# ========== 결과 요약 ==========
print(f"\n=== 결과 파일 ===")
for root, dirs, files in os.walk(_OUT):
    for fn in files:
        fp = os.path.join(root, fn)
        print(f"  {os.path.relpath(fp, _BASE)}")

print("\n" + "=" * 50)
print("ALL TESTS PASSED")
print(f"결과 확인: {_OUT}")
print("=" * 50)
