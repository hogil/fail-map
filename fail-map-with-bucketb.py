#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fail-bit map PNG + positions JSON generator (00P/00C dual pipeline)

핵심 동작(요구사항 반영):
1) 파일명 1차 필터: -00P_ 와 -00C_ 둘 다 잡음 (kind=00P/00C 구분)
2) 팔레트 인덱스 순서(고정):
   - Grade(=chip0~chip7)은 반드시 0~7 고정
   - 그 다음 bg, text
   - 그 다음 border(normal), border_inv(invalid)
   - 그 다음 BIN 테두리들
3) BIN 테두리 적용은 kind별 화이트리스트:
   - 00P: 285/286/287/288/290/291 만 색 테두리 적용
   - 00C: 300/385/386/388/389/390 만 색 테두리 적용
4) PNG 저장 후 positions JSON도 항상 같이 생성/저장 (00P/00C 무관)
   - JSON에 kind 포함
   - grid_edges(xs/ys) + chips[].rect(픽셀 좌표)를 제공해 UI가 클릭/오버레이 정답으로 사용 가능
5) Bucket B 매칭: bucketb_module에서 import하여 positions JSON에 match 결과 추가
"""

import os, re, time, multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
import numpy as np
from PIL import Image, ImageDraw
from tqdm import tqdm

from utils import (
    safe_name, safe_prefix,
    select_folders_by_window, cleanup_empty_p2_and_dates,
    hval, parse_stime_dt,
    choose_pair_by_device, setup_environment,
    ttf_cached, save_indexed32_png,
    build_palette, process_file_content,
    S3Manager,
)
from bucketb_module import CFG_B, S3ManagerB, build_bucket_b_match_map_prefixlist
from positions_module import save_positions_json, map_tile_after_rotation

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
    chunk_size: int = 10

    border_thickness: int = 1
    defect_border_thickness: int = 2
    default_tile_size: tuple = (24, 24)

    draw_empty_chip_text: bool = True
    empty_chip_text_field: str = "b"

    palette_colors: int = 32
    color_json: str = "/appdata/appuser/l3tracker-main/logs/color-legends.json"

    # ✅ 00P / 00C 파일명 middle 필터
    folder_filter_middles: dict = None  # {"00P":"-00P_", "00C":"-00C_"}

    hours_back_start: int = 0
    hours_back_end:   int = 2

    df_path: str = "/appdata/appuser/project/device_info.txt"
    df_positions: tuple = (4, 3, 1)  # (token, prefix1, prefix2) 1-based

    base_root: str = "/appdata/appuser/images"
    positions_root: str = "/appdata/appuser/positions"

CFG = PipelineConfig()
CFG.folder_filter_middles = {"00P": "-00P_", "00C": "-00C_"}

(PALETTE_HEX_MAP, PALETTE_INDEX_TO_KEY, KEY_TO_INDEX,
 IDX_INVALID_FILL, IDX_BG, IDX_TEXT, IDX_BORDER, IDX_BORDER_INV,
 IDX_B_DEF_00P, IDX_B_DEF_00C, PALETTE_32) = build_palette(CFG.color_json)

# =================== Image generation ===================

def create_sample_image_func(args):
    """
    워커에서 실행:
    1) samples(칩들)로 팔레트 인덱스 PNG 생성
    2) PNG 저장
    3) positions JSON 저장 (00P/00C 상관없이 항상)
    """
    (samples, output_path, border_thin, rot, default_tile_size,
     draw_empty_text, empty_text_field, defect_border_thickness, positions_root) = args

    if not samples:
        return None

    # --- 타일 그리드 크기 ---
    xs = [s['x'] for s in samples]; ys = [s['y'] for s in samples]
    x_min, x_max, y_min, y_max = min(xs), max(xs), min(ys), max(ys)
    tiles_w = x_max - x_min + 1
    tiles_h = y_max - y_min + 1

    # --- 타일 내부 픽셀 크기(sh,sw) 추정 ---
    first_valid = next((s for s in samples if s.get('transformed_values')), None)
    if first_valid and first_valid.get('transformed_values'):
        sr = first_valid['transformed_values'].split(',')
        sh = len(sr); sw = len(sr[0]) if sh > 0 else 0
        if sh == 0 or sw == 0:
            sh, sw = default_tile_size
    else:
        sh, sw = default_tile_size

    # --- 원본 캔버스(회전 전) ---
    H0, W0 = tiles_h * sh, tiles_w * sw
    idx0 = np.full((H0, W0), IDX_BG, dtype=np.uint8)  # 배경 인덱스로 초기화

    vmap, bmap, have = {}, {}, set()

    def _tile_ok(s):
        rs = s.get('transformed_values') or ""
        rows = rs.split(',') if rs else []
        return (len(rows) == sh) and (sh > 0) and all(len(r) == sw for r in rows)

    # --- 타일 채움(Grade 0..7 고정) / invalid fill ---
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
            # '0'..'7' -> 0..7 (팔레트 인덱스)
            vals = np.frombuffer(''.join(rows).encode('ascii'), dtype=np.uint8) - ord('0')
            idx0[y0:y1, x0:x1] = vals.reshape(sh, sw)
        else:
            idx0[y0:y1, x0:x1] = IDX_INVALID_FILL

    # --- 회전 ---
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

    # --- 팔레트 이미지 생성 ---
    imgP = Image.fromarray(idxR, mode='P')
    imgP.putpalette(PALETTE_32)

    # --- 정사각형 리사이즈(시각화/UI 편의) + 스케일 기록 ---
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

    # 타일 경계 픽셀(정사각 캔버스 기준)
    xs_pix = [int(round(k * W / tilesW_after)) for k in range(tilesW_after + 1)]
    ys_pix = [int(round(k * H / tilesH_after)) for k in range(tilesH_after + 1)]

    # --- 기본 격자 테두리(border) ---
    b = int(max(1, border_thin))
    for (ii0, jj0) in have:
        ii, jj = map_tile_after_rotation(ii0, jj0, rot_code, tilesW_after, tilesH_after)
        x0, x1 = xs_pix[ii], xs_pix[ii + 1]
        y0, y1 = ys_pix[jj], ys_pix[jj + 1]
        arr[y0:y0+b, x0:x1] = IDX_BORDER
        arr[y1-b:y1, x0:x1] = IDX_BORDER
        arr[y0:y1, x0:x0+b] = IDX_BORDER
        arr[y0:y1, x1-b:x1] = IDX_BORDER

    # invalid 타일 내부는 invalid fill로 덮기(안전)
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

    # ✅ kind별 BIN 테두리 세트 선택
    meta0 = samples[0]
    kind = str(meta0.get("kind", "00P")).upper()
    IDX_B_DEF_LOCAL = IDX_B_DEF_00C if kind == "00C" else IDX_B_DEF_00P

    # --- BIN/invalid 테두리 오버레이 + invalid 텍스트 ---
    for s in samples:
        x_abs = int(s['x']); y_abs = int(s['y'])
        ii0, jj0 = x_abs - x_min, y_abs - y_min
        ii, jj = map_tile_after_rotation(ii0, jj0, rot_code, tilesW_after, tilesH_after)
        x0, x1 = xs_pix[ii], xs_pix[ii + 1]
        y0, y1 = ys_pix[jj], ys_pix[jj + 1]

        ok = vmap.get((ii0, jj0), False)
        bval = bmap.get((ii0, jj0), "")

        # BIN 숫자 정규화: "0285" -> "285", "B285" -> "285", "BIN290" -> "290"
        mnum = re.search(r'(\d+)', str(bval))
        num_key = None
        if mnum:
            num_key = mnum.group(1)
            num_key = num_key[-3:]
            num_key = num_key.zfill(3)

        if not ok:
            cidx = IDX_BORDER_INV
        elif num_key in IDX_B_DEF_LOCAL:
            cidx = IDX_B_DEF_LOCAL[num_key]
        else:
            cidx = None

        if cidx is not None:
            base_img.paste(cidx, (x0, y0, x1, y0 + d))
            base_img.paste(cidx, (x0, y1 - d, x1, y1))
            base_img.paste(cidx, (x0, y0, x0 + d, y1))
            base_img.paste(cidx, (x1 - d, y0, x1, y1))

        # invalid 텍스트(예: 285 표시)
        if draw_empty_text and (not ok):
            rawb = str(s.get(empty_text_field) or s.get('b') or "").strip()
            if rawb:
                rawb = rawb[1:4] if len(rawb) >= 4 else rawb[-3:]
                inner_w = max(1, int(round((x1 - x0) * TEXT_FILL_RATIO)))
                inner_h = max(1, int(round((y1 - y0) * TEXT_FILL_RATIO)))
                font = ttf_cached(inner_w, inner_h, rawb)
                try:
                    tw = int(draw.textlength(rawb, font=font))
                    th = font.size
                except:
                    tw, th = inner_w, inner_h
                cx, cy = (x0 + x1) // 2, (y0 + y1) // 2
                draw.text((cx - tw // 2, cy - th // 2), rawb, fill=IDX_TEXT, font=font)

    # --- PNG 저장 ---
    save_indexed32_png(base_img, output_path)

    # --- positions JSON 생성/저장 (positions_module) ---
    Ws, Hs = base_img.size
    save_positions_json(
        samples, output_path, positions_root,
        kind, rot_code,
        x_min, x_max, y_min, y_max,
        tilesW_after, tilesH_after,
        Ws, Hs,
        sx, sy,
        border_thin, defect_border_thickness,
    )

    return output_path

# =================== Processor / Generator ===================

class DataProcessor:
    def __init__(self, cfg: PipelineConfig):
        self.cfg = cfg
        self.executor = ProcessPoolExecutor(max_workers=cfg.cpu_processes,
                                            mp_context=multiprocessing.get_context("spawn"))
    def close(self):
        self.executor.shutdown(wait=True)

    # tagged_pairs: (tok, p1, p2, kind, name, text)
    def process_files_parallel_tagged(self, tagged_pairs):
        if not tagged_pairs:
            return []
        file_contents = [(name, text) for _, _, _, _, name, text in tagged_pairs]

        results = list(tqdm(self.executor.map(
            process_file_content, file_contents,
            chunksize=max(1, len(file_contents)//(self.cfg.cpu_processes*4) or 1)
        ), total=len(file_contents), desc="Processing (Cython)"))

        out = []
        for idx, fr in enumerate(results):
            tok, p1, p2, kind = tagged_pairs[idx][0], tagged_pairs[idx][1], tagged_pairs[idx][2], tagged_pairs[idx][3]
            for r in fr:
                r["token"] = tok
                r["p1"] = p1
                r["p2"] = p2
                r["kind"] = kind  # ✅ 00P/00C
                out.append(r)
        return out

def _calc_yield(samples):
    """Yield = (gd / netd) * 100. gd = bin < 200인 칩 수, netd = 첫 샘플의 :NETD= 값."""
    netd = int(samples[0].get("netd", 0) or 0) if samples else 0
    gd = 0
    for s in samples:
        raw_b = str(s.get('b') or "").strip()
        b_num = re.sub(r'\D', '', raw_b)
        if b_num:
            try:
                if int(b_num) < 200:
                    gd += 1
            except:
                pass
    if netd > 0:
        return gd / netd * 100
    return 0.0


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

        # ✅ kind 포함해서 그룹 분리 (00P/00C 별도 이미지)
        for s in dataset_all:
            key = (
                s.get('kind','NA'),
                s.get('token','NA'), s.get('p1','NA'), s.get('p2','NA'),
                s.get('root',''), s.get('step',''), s.get('wafer',''), s.get('stime','NA')
            )
            groups[key].append(s)

        tasks, task_keys = [], []
        for (kind, tok, p1, p2, root, step, wafer, stime), samples in groups.items():
            if not samples:
                continue

            # rot majority vote
            rots = [int(x.get('rot',5) or 5) for x in samples]
            rot = Counter(rots).most_common(1)[0][0] if rots else 5
            for s in samples:
                s["rot"] = rot

            day = (stime.split('_')[0] if (stime and '_' in stime) else "NA")
            out_dir = os.path.join(base_root, safe_prefix(p1), safe_prefix(p2), day)
            os.makedirs(out_dir, exist_ok=True)

            wafer_for_name = wafer[1:] if str(wafer).startswith("W") else wafer
            yld = _calc_yield(samples)
            out_path = os.path.join(
                out_dir,
                f"{safe_name(root)}_{safe_name(step)}_{safe_name(wafer_for_name)}_{safe_name(stime)}_{yld:.1f}.png"
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
            task_keys.append((kind, tok, p1, p2))

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

    # token -> list[(p1,p2)]
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
    print(f"[middles] {CFG.folder_filter_middles}")
    print(f"[palette] chip idx 0..7 = {[KEY_TO_INDEX[f'chip{i}'] for i in range(8)]}  bg={IDX_BG} text={IDX_TEXT} border={IDX_BORDER} inv={IDX_BORDER_INV}")

    t0 = time.time()
    results = {}
    try:
        folders = s3.get_top_level_folders()
        print(f"[folders] total={len(folders)}")
        if not folders:
            print("No folders.")
            return results

        selected = select_folders_by_window(folders, start_ts, end_ts)
        print(f"[folders] selected={selected}")

        # ✅ 1차 필터: 파일명에서 token + kind(00P/00C) + 시간
        key_to_info, pf_stats = s3.prefilter_keys_by_filename(
            selected, token2pps, CFG.folder_filter_middles, start_ts, end_ts,
        )
        print(f"[prefilter(filename)] scanned={pf_stats.get('scanned',0)}  token_hit={pf_stats.get('token_hit',0)}  "
              f"time_hit={pf_stats.get('time_hit',0)}  kept={pf_stats.get('kept',0)}")

        matched_keys = list(key_to_info.keys())
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
                bucket_b_match_map = build_bucket_b_match_map_prefixlist(part_keys, s3b, CFG_B, chunk_label=f"{idx}/{total_chunks}", miss_dir=CFG.positions_root)

            contents = s3.download_and_decompress_parallel(part_keys)
            if not contents:
                print("  -> empty chunk")
                continue

            # token/kind는 key_to_info에서, p1/p2는 :DEVICE=로 결정
            tagged_pairs = []
            for name, text in contents:
                orig_key = name.split("::", 1)[0]
                info = key_to_info.get(orig_key)
                if info is None:
                    continue
                tok, kind = info  # "00P" or "00C"
                lines = text.splitlines()
                device_val = hval(lines, ":DEVICE=", max_lines=200)
                p1, p2 = choose_pair_by_device(tok, device_val, token2pps)
                tagged_pairs.append((tok, p1, p2, kind, name, text))
            del contents

            dataset_all = proc.process_files_parallel_tagged(tagged_pairs)

            # bucket_b_match 주입 (positions json에서 성공/실패 확인용)
            if bucket_b_match_map:
                for s in dataset_all:
                    akey = s.get("orig_key") or ""
                    meta = bucket_b_match_map.get(akey)
                    if meta is not None:
                        s["bucket_b_match"] = meta

            # 2차 필터: 본문 STIME이 파싱된 데이터만 유지(현재 요구대로 dt 존재만 확인)
            before = len(dataset_all)
            if (h0 != 0 or h1 != 0):
                kept = []
                for s in dataset_all:
                    dt = parse_stime_dt(s.get('stime'))
                    if dt is not None:
                        kept.append(s)
                dataset_all = kept
            after = len(dataset_all)
            print(f"[stime-filter] kept {after}/{before}")
            if not dataset_all:
                print("  -> no dataset in window")
                continue

            imgs, img_ok_by_key = img.generate_images_mixed(
                dataset_all,
                base_root=CFG.base_root,
                positions_root=CFG.positions_root
            )

            # accumulate 결과
            from collections import Counter
            ds_count_by_key = Counter((s.get('kind','NA'), s.get('token','NA'), s.get('p1','NA'), s.get('p2','NA')) for s in dataset_all)
            for (kind, tok, p1, p2), n in ds_count_by_key.items():
                k = (f"{p1}/{p2}", str(tok), p1, p2, kind)
                results.setdefault(k, {"dataset_size": 0, "image_count": 0})
                results[k]["dataset_size"] += n
                results[k]["image_count"]  += img_ok_by_key.get((kind, tok, p1, p2), 0)

            # chunk 종료 시 bucketB 매칭 성공/실패 요약 출력
            if bucket_b_match_map:
                _succ = sum(1 for v in bucket_b_match_map.values() if v.get("matched"))
                _fail = len(bucket_b_match_map) - _succ
                _read_ok = sum(1 for v in bucket_b_match_map.values() if v.get("matched") and v.get("first_line_ok"))

                print(
                    f"  -> chunk done in {round(time.time()-t_chunk, 2)}s  [bucketB] 성공={_succ} 실패={_fail} read_ok={_read_ok}/{_succ if _succ else 0}"
                )
            else:
                print(f"  -> chunk done in {round(time.time()-t_chunk, 2)}s  [bucketB] disabled_or_empty")

        total_secs = round(time.time()-t0, 2)

        # cleanup
        p1_set = set()
        for pairs in token2pps.values():
            for (p1, p2) in pairs:
                if p1 and p1 != "NA":
                    p1_set.add(p1)
        removed = cleanup_empty_p2_and_dates(CFG.base_root, p1_set)
        if removed:
            print(f"[cleanup] removed {removed} empty dirs")

        print(f"\n✅ Global done in {total_secs}s")
        print("\n🎯 Results by (prefix, token, p1, p2, kind)")
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
