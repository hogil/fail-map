#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Positions JSON 생성 모듈
- create_sample_image_func 에서 positions JSON 생성/저장 로직을 분리
- Bucket B match summary 포함
"""

import os, re, json

from utils import safe_prefix


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
    return i - (W // 2 - 1) if (W % 2 == 0) else i - (W // 2)


def centerize_row(j, H):
    return j - (H // 2)


def save_positions_json(
    samples, output_path, positions_root,
    kind, rot_code,
    x_min, x_max, y_min, y_max,
    tilesW_after, tilesH_after,
    canvas_w, canvas_h,
    sx, sy,
    border_thin, defect_border_thickness,
    json_format="compact",
):
    """
    positions JSON 생성 및 저장.
    Bucket B match summary도 포함.
    """
    meta0 = samples[0]
    p1 = meta0.get("p1", "NA")
    p2 = meta0.get("p2", "NA")
    stime = str(meta0.get("stime", ""))
    day = (stime.split('_')[0] if (stime and '_' in stime) else "NA")

    Ws, Hs = canvas_w, canvas_h
    tiles_w_rot = tilesW_after
    tiles_h_rot = tilesH_after

    xs_edges = [int(round(k * Ws / tiles_w_rot)) for k in range(tiles_w_rot + 1)]
    ys_edges = [int(round(k * Hs / tiles_h_rot)) for k in range(tiles_h_rot + 1)]

    chips_json = []
    for s in samples:
        rawb = (str(s.get('b') or "").lstrip('0') or '0')
        chips_json.append({
            "x_abs": int(s['x']), "y_abs": int(s['y']), "b": rawb,
        })

    # ===== Bucket B f/q per chip =====
    _bm = meta0.get("bucket_b_match")
    _b_parsed = None
    if isinstance(_bm, dict) and _bm.get("matched"):
        _b_parsed = _bm.get("bucket_b_parsed")
    if _b_parsed and _b_parsed.get("chip_data"):
        chip_data_b = _b_parsed["chip_data"]
        for chip_entry in chips_json:
            chip_key = f"{chip_entry['x_abs']}_{chip_entry['y_abs']}"
            cd = chip_data_b.get(chip_key)
            if cd:
                if cd.get("b"):
                    chip_entry["b"] = cd["b"]
                if cd.get("FTN"):
                    chip_entry["f"] = cd["FTN"]
                if cd.get("QTN"):
                    chip_entry["q"] = cd["QTN"]

    # rect / cal 은 f/q 뒤에 배치
    for idx, s in enumerate(samples):
        x_abs = int(s['x']); y_abs = int(s['y'])
        i0 = x_abs - x_min; j0 = y_abs - y_min
        i, j = map_tile_after_rotation(i0, j0, rot_code, tiles_w_rot, tiles_h_rot)
        x0, x1 = xs_edges[i], xs_edges[i + 1]
        y0, y1 = ys_edges[j], ys_edges[j + 1]
        chips_json[idx]["x_cal"] = int(centerize_col(i, tiles_w_rot))
        chips_json[idx]["y_cal"] = int(centerize_row(j, tiles_h_rot))
        chips_json[idx]["rect"] = {
            "x0": int(x0), "y0": int(y0), "x1": int(x1), "y1": int(y1),
        }

    # ===== netd / gd / yield / sys =====
    netd = int(meta0.get("netd", 0) or 0)
    _sys_bins_00p = {285, 286, 287, 288, 290, 291}
    _sys_bins_00c = {300, 385, 386, 388, 389, 390}
    sys_bins = _sys_bins_00c if kind == "00C" else _sys_bins_00p
    gd = 0
    sys_count = 0
    for s in samples:
        raw_b = str(s.get('b') or "").strip()
        b_num = re.sub(r'\D', '', raw_b)
        if b_num:
            try:
                n = int(b_num)
                if n < 200:
                    gd += 1
                if n in sys_bins:
                    sys_count += 1
            except:
                pass
    if netd > 0:
        yield_val = f"{gd / netd * 100:.2f}"
        sys_val = f"{sys_count / netd * 100:.2f}"
    else:
        yield_val = "0.00"
        sys_val = "0.00"

    # ===== Bucket B match summary =====
    if _bm is None:
        _bm = meta0.get("bucket_b_match")
    _b_key = ""
    _tm = ""
    _lt = ""
    if isinstance(_bm, dict) and _bm.get("matched"):
        _b_key = str(_bm.get("key") or "")
        _tm = str((_b_parsed or {}).get("tm") or "")
        _lt = str((_b_parsed or {}).get("lt") or "")
        # Bucket B yield로 덮어쓰기
        if _b_parsed and _b_parsed.get("yield"):
            yield_val = _b_parsed["yield"]
            gd = _b_parsed.get("gd", gd)

    json_obj = {
        "bucket_b_key": _b_key,
        "root": meta0.get("root", ""),
        "step": meta0.get("step", ""),
        "wafer": meta0.get("wafer", ""),
        "stime": stime,
        "partid": meta0.get("partid", ""),
        "tester": meta0.get("tester", ""),
        "device": meta0.get("device", ""),
        "pgm": meta0.get("pgm", ""),
        "netd": netd,
        "gd": gd,
        "yield": yield_val,
        "sys": sys_val,
        "tm": _tm,
        "lt": _lt,
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

    # ===== compact_array: f/q 키 분리 + 값 배열화 =====
    if json_format == "compact_array":
        ftn_set, qtn_set = {}, {}
        for c in chips_json:
            if isinstance(c.get("f"), dict):
                for k in c["f"]:
                    ftn_set[k] = None
            if isinstance(c.get("q"), dict):
                for k in c["q"]:
                    qtn_set[k] = None
        ftn_keys = list(ftn_set.keys())
        qtn_keys = list(qtn_set.keys())
        if ftn_keys or qtn_keys:
            for c in chips_json:
                if isinstance(c.get("f"), dict):
                    c["f"] = [c["f"].get(k, "") for k in ftn_keys]
                if isinstance(c.get("q"), dict):
                    c["q"] = [c["q"].get(k, "") for k in qtn_keys]
            # ftn_keys/qtn_keys를 chips 앞에 삽입
            ordered = {}
            for k, v in json_obj.items():
                if k == "chips":
                    if ftn_keys:
                        ordered["ftn_keys"] = ftn_keys
                    if qtn_keys:
                        ordered["qtn_keys"] = qtn_keys
                ordered[k] = v
            json_obj = ordered

    # ===== 직렬화 =====
    json_dir = os.path.join(positions_root, safe_prefix(p1), safe_prefix(p2), day)
    os.makedirs(json_dir, exist_ok=True)
    base_name = os.path.splitext(os.path.basename(output_path))[0]
    json_path = os.path.join(json_dir, base_name + ".json")

    if json_format in ("compact", "compact_array"):
        _ck = {"f", "q", "rect", "xs", "ys"}
        if json_format == "compact_array":
            _ck |= {"ftn_keys", "qtn_keys"}

        class _CompactEncoder(json.JSONEncoder):
            def encode(self, o):
                return self._encode(o, level=0)
            def _encode(self, o, level):
                indent = "  " * level
                if isinstance(o, dict):
                    if not o:
                        return "{}"
                    items = []
                    for k, v in o.items():
                        if k in _ck and isinstance(v, (dict, list)):
                            val_str = json.dumps(v, ensure_ascii=False, separators=(", ", ": "))
                        else:
                            val_str = self._encode(v, level + 1)
                        items.append(f'{indent}  "{k}": {val_str}')
                    return "{\n" + ",\n".join(items) + "\n" + indent + "}"
                elif isinstance(o, list):
                    if not o:
                        return "[]"
                    items = [f"{indent}  {self._encode(i, level + 1)}" for i in o]
                    return "[\n" + ",\n".join(items) + "\n" + indent + "]"
                else:
                    return json.dumps(o, ensure_ascii=False)

        with open(json_path, "w", encoding="utf-8") as f:
            f.write(_CompactEncoder().encode(json_obj))
    else:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(json_obj, f, ensure_ascii=False, indent=2)
