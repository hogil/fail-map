#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
기존 positions JSON을 compact + 배열화 형식으로 변환하는 스크립트.
- f/q 딕셔너리 → ftn_keys/qtn_keys 상단 분리 + 값 배열
- f/q/rect/xs/ys 한 줄로 직렬화
- quad 필드 제거

Usage:
    python compact_positions.py --positions-root /path/to/positions [--workers 8] [--dry-run]
"""

import os, json, glob, argparse
from concurrent.futures import ThreadPoolExecutor

_COMPACT_KEYS = {"f", "q", "rect", "xs", "ys", "ftn_keys", "qtn_keys"}


class _CompactEncoder(json.JSONEncoder):
    """f/q/rect/xs/ys/ftn_keys/qtn_keys는 한 줄, 나머지는 indent=2."""

    def encode(self, o):
        return self._enc(o, 0)

    def _enc(self, o, lv):
        ind = "  " * lv
        if isinstance(o, dict):
            if not o:
                return "{}"
            items = []
            for k, v in o.items():
                if k in _COMPACT_KEYS and isinstance(v, (dict, list)):
                    vs = json.dumps(v, ensure_ascii=False, separators=(", ", ": "))
                else:
                    vs = self._enc(v, lv + 1)
                items.append(f'{ind}  "{k}": {vs}')
            return "{\n" + ",\n".join(items) + "\n" + ind + "}"
        elif isinstance(o, list):
            if not o:
                return "[]"
            items = [f"{ind}  {self._enc(i, lv + 1)}" for i in o]
            return "[\n" + ",\n".join(items) + "\n" + ind + "]"
        else:
            return json.dumps(o, ensure_ascii=False)


def compact_one(json_path, dry_run=False):
    """positions JSON 1개를 compact + 배열화. 변경 시 True 반환."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return False

    chips = data.get("chips")
    if not chips or not isinstance(chips, list):
        return False

    changed = False

    # ftn_keys / qtn_keys 수집 (이미 배열화된 경우 스킵)
    already_arrayed = "ftn_keys" in data
    if not already_arrayed:
        ftn_set = {}
        qtn_set = {}
        for c in chips:
            if isinstance(c.get("f"), dict):
                for k in c["f"]:
                    ftn_set[k] = None
            if isinstance(c.get("q"), dict):
                for k in c["q"]:
                    qtn_set[k] = None

        ftn_keys = list(ftn_set.keys())
        qtn_keys = list(qtn_set.keys())

        if ftn_keys or qtn_keys:
            # 키 분리
            if ftn_keys:
                data["ftn_keys"] = ftn_keys
            if qtn_keys:
                data["qtn_keys"] = qtn_keys

            # 값 배열화
            for c in chips:
                if isinstance(c.get("f"), dict):
                    c["f"] = [c["f"].get(k, "") for k in ftn_keys]
                if isinstance(c.get("q"), dict):
                    c["q"] = [c["q"].get(k, "") for k in qtn_keys]

            # ftn_keys/qtn_keys를 chips 앞에 배치
            ordered = {}
            for k, v in data.items():
                if k == "chips":
                    if "ftn_keys" in data:
                        ordered["ftn_keys"] = data["ftn_keys"]
                    if "qtn_keys" in data:
                        ordered["qtn_keys"] = data["qtn_keys"]
                if k not in ("ftn_keys", "qtn_keys"):
                    ordered[k] = v
            data = ordered
            changed = True

    # quad 제거
    for c in chips:
        if isinstance(c.get("rect"), dict) and "quad" in c["rect"]:
            del c["rect"]["quad"]
            changed = True

    if not changed and not dry_run:
        # compact 인코딩 적용 여부 확인 (원본과 비교)
        with open(json_path, "r", encoding="utf-8") as f:
            original = f.read()
        new_content = _CompactEncoder().encode(data)
        if new_content != original:
            changed = True

    if changed and not dry_run:
        new_content = _CompactEncoder().encode(data)
        with open(json_path, "w", encoding="utf-8") as f:
            f.write(new_content)

    return changed


def main():
    parser = argparse.ArgumentParser(
        description="Compact positions JSON: arrayify f/q + inline f/q/rect/xs/ys"
    )
    parser.add_argument(
        "--positions-root",
        default=r"D:\project\data\positions",
        help="positions JSON 루트 디렉토리",
    )
    parser.add_argument("--workers", type=int, default=8, help="병렬 워커 수")
    parser.add_argument(
        "--dry-run", action="store_true", help="실제 파일 수정 없이 대상 수만 출력"
    )
    args = parser.parse_args()

    json_files = glob.glob(
        os.path.join(args.positions_root, "**", "*.json"), recursive=True
    )
    print(f"스캔: {len(json_files)}개 파일")

    converted = 0

    def _work(p):
        return compact_one(p, dry_run=args.dry_run)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        for result in ex.map(_work, json_files):
            if result:
                converted += 1

    action = "변환 대상" if args.dry_run else "변환 완료"
    print(f"{action}: {converted}개 / {len(json_files)}개")


if __name__ == "__main__":
    main()
