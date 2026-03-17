#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기존 이미지/positions JSON에 LT/TM이 없으면 보정하는 스크립트.
- positions JSON: lt/tm 필드가 비어있으면 랜덤 값 추가
- 이미지 파일: 파일명에 _LT_TM이 없으면 리네임

Usage:
    python backfill_lt_tm.py --images-root /path/to/images --positions-root /path/to/positions [--workers 8]
"""

import os, json, re, glob, argparse, random
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

LT_VALUES = ["EE", "PE", "PT"]
TM_VALUES = ["Normal", "Engineer", "Test"]


def backfill_position_json(json_path):
    """positions JSON에 lt/tm이 비어있으면 랜덤 값 추가. 변경 시 True 반환."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return False

    changed = False
    if not data.get("lt"):
        data["lt"] = random.choice(LT_VALUES)
        changed = True
    if not data.get("tm"):
        data["tm"] = random.choice(TM_VALUES)
        changed = True

    if changed:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    return changed


def rename_image_if_needed(png_path, positions_root):
    """
    이미지 파일명에 _LT_TM이 없으면:
    1) 대응하는 positions JSON에서 lt/tm 읽기
    2) 이미지 리네임
    3) positions JSON의 image_path도 업데이트
    """
    name = Path(png_path).stem  # e.g. ABC_S01_12_20260312_143000_87.5_3.2
    parts = name.split("_")

    # 이미 LT_TM이 붙어있는지 확인: 최소 8파트 (root_step_wafer_date_time_yield_sys_lt_tm = 9파트)
    # stime이 date_time으로 2파트이므로: root_step_wafer_date_time_yield_sys = 7파트
    # +lt+tm = 9파트
    # 현재 7파트면 LT_TM 없음, 9파트면 있음
    if len(parts) >= 9:
        return False  # 이미 있음

    # 대응하는 positions JSON 찾기
    img_dir = Path(png_path).parent
    img_rel = img_dir.name  # day
    json_name = name + ".json"

    # positions 경로 추정: images root 이후 경로를 positions root에 매핑
    img_path = Path(png_path)
    json_path = None

    # positions_root 하위에서 같은 이름의 json 찾기
    if positions_root:
        # 이미지 경로에서 base_root 이후 상대경로 추출
        # images/.../p1/p2/day/file.png -> positions/.../p1/p2/day/file.json
        try:
            for root, dirs, files in os.walk(positions_root):
                if json_name in files:
                    json_path = os.path.join(root, json_name)
                    break
        except Exception:
            pass

    lt = "NA"
    tm = "NA"
    if json_path and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            lt = data.get("lt") or "NA"
            tm = data.get("tm") or "NA"
        except Exception:
            pass

    # 리네임
    new_name = f"{name}_{lt}_{tm}.png"
    new_path = os.path.join(str(img_path.parent), new_name)
    try:
        os.rename(png_path, new_path)
    except Exception:
        return False

    # positions JSON의 image_path 업데이트
    if json_path and os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            old_img_path = data.get("image_path", "")
            if old_img_path:
                data["image_path"] = old_img_path.rsplit("/", 1)[0] + "/" + new_name
                with open(json_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    return True


def main():
    parser = argparse.ArgumentParser(description="Backfill LT/TM into positions JSON and image filenames")
    parser.add_argument("--images-root", required=True, help="이미지 루트 디렉토리")
    parser.add_argument("--positions-root", required=True, help="positions JSON 루트 디렉토리")
    parser.add_argument("--workers", type=int, default=8, help="병렬 워커 수 (기본 8)")
    args = parser.parse_args()

    # 1) positions JSON 보정
    json_files = glob.glob(os.path.join(args.positions_root, "**", "*.json"), recursive=True)
    print(f"[1/2] positions JSON 보정: {len(json_files)}개 파일 스캔")

    json_changed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for changed in ex.map(backfill_position_json, json_files):
            if changed:
                json_changed += 1
    print(f"  -> {json_changed}개 파일 lt/tm 추가")

    # 2) 이미지 파일명 리네임
    png_files = glob.glob(os.path.join(args.images_root, "**", "*.png"), recursive=True)
    print(f"[2/2] 이미지 파일명 보정: {len(png_files)}개 파일 스캔")

    def _rename(p):
        return rename_image_if_needed(p, args.positions_root)

    img_renamed = 0
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for renamed in ex.map(_rename, png_files):
            if renamed:
                img_renamed += 1
    print(f"  -> {img_renamed}개 파일 리네임")

    print(f"\n완료: JSON {json_changed}건 보정, 이미지 {img_renamed}건 리네임")


if __name__ == "__main__":
    main()
