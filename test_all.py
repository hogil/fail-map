#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
전체 기능 통합 테스트 (더미 데이터 사용, S3 불필요)
"""
import os, sys, json, tempfile, shutil

# 테스트용 더미 color-legends.json 생성
_test_dir = tempfile.mkdtemp(prefix="failmap_test_")
_color_json_path = os.path.join(_test_dir, "color-legends.json")
with open(_color_json_path, "w") as f:
    json.dump({
        "default": {
            "top": {
                "Grade0": "#FFFFFF", "Grade1": "#9B9B9B", "Grade2": "#009619",
                "Grade3": "#0000FF", "Grade4": "#D91DFF", "Grade5": "#FFFF00",
                "Grade6": "#FF0000", "Grade7": "#000000",
            },
            "bottom": {
                "Normal": "#BEBEBE", "Invalid": "#FF9900",
                "B285": "#0099FF", "B286": "#FF714F", "B287": "#66FFCC",
                "B288": "#DA26CD", "B290": "#FFD700", "B291": "#32CD32",
                "B300": "#AAAAAA", "B385": "#00C8FF", "B386": "#FF00C8",
                "B388": "#00FF66", "B389": "#FF6666", "B390": "#6666FF",
            },
            "background": "#FEFEFE",
            "text": "#000001",
        }
    }, f)

print(f"[test] temp dir: {_test_dir}")

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
from datetime import datetime
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
from datetime import timedelta
now = datetime(2026, 1, 22, 12, 0, 0)
folders = ["260121/", "260122/", "260123/", "260120/"]
sel = select_folders_by_window(folders, now - timedelta(hours=24), now)
assert "260121/" in sel and "260122/" in sel
print("  select_folders_by_window: OK")

# ========== 2) Cython / Python fallback 테스트 ==========
print("\n=== 2) Cython convert_hex 테스트 ===")

convert_fn = get_cython_convert_hex()

# 더미 hex 데이터: 4x2 (xsize=4, ysize=2)
# '#' + hex pairs: #0A9BFF00  (pos 1,3,5,7 → A=2, B=3, F=7, 0=0)
test_lines = ["#0A0B0F00", "#0A0B0F00"]
result = convert_fn(test_lines, 0, 4, 2)
rows = result.split(",")
assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
assert len(rows[0]) == 4, f"Expected 4 chars per row, got {len(rows[0])}"
# A→2, B→3, F→7, 0→0
assert rows[0] == "2370", f"Expected '2370', got '{rows[0]}'"
assert rows[1] == "2370"
print(f"  convert_hex result: {result}")
print("  get_cython_convert_hex (fallback): OK")


# ========== 3) process_file_content 테스트 ==========
print("\n=== 3) process_file_content 테스트 ===")

# 더미 raw 파일 콘텐츠로 개별 함수 테스트
raw_content = """:WFID=3BC170H3-00P.W01
:WFID=3BC170H3-00P.W01
:PARTID=PART001
:TESTER=T1
:DEVICE=FAB_A_DEV
:PGM=TESTPGM
:EXTRA1=X
:EXTRA2=Y
:ROT=5
:STIME=2026/01/22 03:14:55
:EXTRA3=Z
:NETD=0437
X=100 Y=200
Y=100 Y=200
"""
print("  (raw 파일 파싱은 메인 모듈의 CFG 의존성 때문에 개별 함수로 테스트)")

# hval로 NETD 파싱 테스트
import re
raw_lines = raw_content.splitlines()
netd_raw = hval(raw_lines, ":NETD=")
netd = int(re.sub(r'\D', '', netd_raw)) if netd_raw else 0
assert netd == 437, f"Expected 437, got {netd}"
print(f"  NETD parsing: '{netd_raw}' → {netd}: OK")


# ========== 4) positions_module 테스트 ==========
print("\n=== 4) positions_module (save_positions_json) 테스트 ===")

from positions_module import save_positions_json, map_tile_after_rotation, centerize_col, centerize_row

# map_tile_after_rotation
assert map_tile_after_rotation(1, 2, 5, 10, 10) == (1, 2)  # no rotation
assert map_tile_after_rotation(1, 2, 7, 10, 10) == (2, 8)  # 90 CCW
assert map_tile_after_rotation(1, 2, 0, 10, 10) == (8, 7)  # 180
print("  map_tile_after_rotation: OK")

# centerize
assert centerize_col(5, 10) == 1  # even: i - (W//2 - 1)
assert centerize_row(5, 10) == 0  # j - H//2
print("  centerize_col / centerize_row: OK")

# save_positions_json 테스트 (더미 samples)
positions_root = os.path.join(_test_dir, "positions")
images_root = os.path.join(_test_dir, "images")
os.makedirs(positions_root, exist_ok=True)
os.makedirs(images_root, exist_ok=True)

# 더미 samples: 4개 칩, b값 다양
samples = [
    {"x": 0, "y": 0, "b": "0100", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 437, "transformed_values": "", "rot": 5,
     "bucket_b_match": {
         "matched": True, "key": "20260122/3BC170H3P_W01_20260122_031455.gz",
         "offset_sec": 0, "first_line": "HEADER LT=3A DATA=123",
         "first_line_ok": True,
     }},
    {"x": 1, "y": 0, "b": "0150", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 437, "transformed_values": "", "rot": 5},
    {"x": 0, "y": 1, "b": "0285", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 437, "transformed_values": "", "rot": 5},
    {"x": 1, "y": 1, "b": "0300", "p1": "FAB_A", "p2": "P2A", "stime": "20260122_031455",
     "root": "3BC170H3", "step": "00P", "wafer": "W01", "kind": "00P",
     "partid": "PART001", "tester": "T1", "device": "DEV1", "pgm": "PGM1",
     "netd": 437, "transformed_values": "", "rot": 5},
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

# netd 확인
assert jobj["netd"] == 437, f"Expected netd=437, got {jobj['netd']}"
print(f"  netd: {jobj['netd']}: OK")

# gd 확인 (b < 200: 0100, 0150 → 2개)
assert jobj["gd"] == 2, f"Expected gd=2, got {jobj['gd']}"
print(f"  gd: {jobj['gd']}: OK")

# yield 확인 (2/437*100 = 0.46)
expected_yield = f"{2/437*100:.2f}"
assert jobj["yield"] == expected_yield, f"Expected yield={expected_yield}, got {jobj['yield']}"
print(f"  yield: {jobj['yield']}: OK")

# lt 확인 (from bucket_b first_line "HEADER LT=3A DATA=123")
assert jobj["lt"] == "3A", f"Expected lt='3A', got {jobj['lt']}"
print(f"  lt: {jobj['lt']}: OK")

# match 확인
assert jobj["match"] == "match성공"
print(f"  match: {jobj['match']}: OK")

# bucket_b_key
assert jobj["bucket_b_key"] == "20260122/3BC170H3P_W01_20260122_031455.gz"
print(f"  bucket_b_key: {jobj['bucket_b_key']}: OK")

# pgm
assert jobj["pgm"] == "PGM1"
print(f"  pgm: {jobj['pgm']}: OK")

# chips 확인
assert len(jobj["chips"]) == 4
print(f"  chips count: {len(jobj['chips'])}: OK")

# chips[0] rect 확인
c0 = jobj["chips"][0]
assert "rect" in c0 and "x0" in c0["rect"]
print(f"  chips[0] rect: {c0['rect']}: OK")


# ========== 5) bucketb_module 테스트 ==========
print("\n=== 5) bucketb_module 테스트 ===")

from bucketb_module import (
    parse_bucket_a_key, iter_offsets_by_closeness,
    generate_bucket_b_candidate_keys, generate_bucket_b_prefixes,
)

# parse_bucket_a_key
info = parse_bucket_a_key("20260122/01_3BC170H3-00P_N_20260122_022718.Z")
assert info is not None
assert info["wafer_num"] == "01"
assert info["lot_id"] == "3BC170H3P"
assert info["date"] == "20260122"
assert info["time"] == "022718"
assert info["wafer_w"] == "W01"
print(f"  parse_bucket_a_key: lot_id={info['lot_id']}, wafer={info['wafer_w']}: OK")

# iter_offsets_by_closeness
offsets = list(iter_offsets_by_closeness((-2, 2)))
assert offsets[0] == 0  # 0에 가까운 것부터
assert 1 in offsets and -1 in offsets
print(f"  iter_offsets_by_closeness(-2,2): {offsets}: OK")

# generate_bucket_b_candidate_keys
keys = list(generate_bucket_b_candidate_keys(info, (-1, 1)))
assert len(keys) > 0
print(f"  generate_bucket_b_candidate_keys: {len(keys)} candidates")
print(f"    first: {keys[0][0]} (offset={keys[0][1]})")
print("  OK")

# generate_bucket_b_prefixes
prefixes = list(generate_bucket_b_prefixes(info, (-10, 10)))
assert len(prefixes) > 0
assert all("3BC170H3P_W01_" in p for p in prefixes)
print(f"  generate_bucket_b_prefixes: {prefixes}: OK")


# ========== 6) Bucket B match 실패 시 테스트 ==========
print("\n=== 6) Bucket B match 실패 시 positions JSON ===")

samples_no_match = [dict(s) for s in samples]
samples_no_match[0]["bucket_b_match"] = {"matched": False, "reason": "not_found"}

save_positions_json(
    samples_no_match, out_path.replace("test.png", "test_nomatch.png"), positions_root,
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

assert jnm["match"] == "match실패"
assert jnm["lt"] == ""  # match 실패 → lt 빈 문자열
print(f"  match: {jnm['match']}: OK")
print(f"  lt (empty): '{jnm['lt']}': OK")
print(f"  yield: {jnm['yield']}: OK")


# ========== Cleanup ==========
print(f"\n=== Cleanup ===")
shutil.rmtree(_test_dir)
print(f"  removed {_test_dir}")

print("\n" + "=" * 50)
print("ALL TESTS PASSED")
print("=" * 50)
