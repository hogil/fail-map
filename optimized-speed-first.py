#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dual Bucket Pipeline - Speed Optimized Version
최대 속도 우선: 대용량 메모리 사용, 최대 병렬화
- Bucket B 전체 파일을 메모리에 사전 로드
- 더 큰 ThreadPool (256 threads)
- 더 작은 chunk size로 더 많은 병렬 처리
- 프리페칭 및 캐싱 전략
"""
import os, re, sys, time, json, io, zipfile, tempfile, shutil, gzip, tarfile, subprocess, importlib, multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict, Counter
import threading

import boto3
import pandas as pd
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from tqdm import tqdm
from botocore.config import Config

# =================== Speed-First Config ===================

@dataclass
class SpeedFirstConfig:
    bucket_name: str = 'eds-ec-memory.fbm-data'
    region_name: str = ''
    aws_access_key_id: str = 'ho.choi-LakeS3-F6B0U6'
    aws_secret_access_key: str = 'iYb7zYDVzitt4QVkUcR2'
    endpoint_url: str = 'http://lakes3.dataplatform.samsungds.net:9020'

    # 속도 최적화: 더 많은 연결과 스레드
    max_pool_connections: int = 512
    download_threads: int = 256  # 기본 128 → 256
    cpu_processes: int = multiprocessing.cpu_count()  # 모든 코어 사용
    chunk_size: int = 150  # 더 작은 청크로 더 빠른 응답

    # 프리페칭
    prefetch_enabled: bool = True
    prefetch_chunks: int = 2  # 2개 청크 미리 가져오기

    border_thickness: int = 1
    defect_border_thickness: int = 2
    default_tile_size: tuple = (24, 24)
    draw_empty_chip_text: bool = True
    empty_chip_text_field: str = "b"
    palette_colors: int = 32
    color_json: str = "/appdata/appuser/l3tracker-main/logs/color-legends.json"
    folder_filter_middle: str = "-00P_"
    hours_back_start: int = 0
    hours_back_end: int = 2
    df_path: str = "/appdata/appuser/project/device_info.txt"
    df_positions: tuple = (4, 3, 1)
    base_root: str = "/appdata/appuser/images"
    positions_root: str = "/appdata/appuser/positions"

@dataclass
class BucketBSpeedConfig:
    bucket_name: str = 'eds-ec-memory.fbm-data-secondary'
    region_name: str = ''
    aws_access_key_id: str = 'ho.choi-LakeS3-F6B0U6'
    aws_secret_access_key: str = 'iYb7zYDVzitt4QVkUcR2'
    endpoint_url: str = 'http://lakes3.dataplatform.samsungds.net:9020'
    max_pool_connections: int = 512
    download_threads: int = 256
    enabled: bool = True
    time_offset_range: tuple = (0, 10)

    # 속도 최적화: 전체 사전 로드
    preload_all: bool = True  # 모든 파일 메모리에 사전 로드

CFG = SpeedFirstConfig()
CFG_B = BucketBSpeedConfig()

print(f"⚡ SPEED-FIRST MODE: threads={CFG.download_threads}, processes={CFG.cpu_processes}, chunk_size={CFG.chunk_size}")

# ... (나머지 코드는 dual-bucket과 동일하지만, 추가 최적화 함수 포함) ...
# 간결성을 위해 핵심 차이점만 표시

class PreloadCache:
    """전체 Bucket B 데이터를 메모리에 사전 로드"""
    def __init__(self):
        self.cache = {}
        self.lock = threading.Lock()

    def load_all(self, s3_manager, keys):
        """모든 파일을 병렬로 사전 다운로드"""
        print(f"  [PreloadCache] Loading {len(keys)} files into memory...")
        t0 = time.time()

        contents = s3_manager.download_and_decompress_parallel(keys)

        with self.lock:
            for name, text in contents:
                self.cache[name] = text

        print(f"  [PreloadCache] Loaded {len(self.cache)} files in {time.time()-t0:.2f}s")
        return len(self.cache)

    def get(self, name):
        with self.lock:
            return self.cache.get(name)

# NOTE: 실제 프로덕션에서는 전체 파일이 필요하므로,
# 이 파일은 개념 증명용입니다.
# 실제로는 기존 dual-bucket 코드를 확장해야 합니다.

if __name__ == "__main__":
    print("⚡ Speed-First Dual Bucket Pipeline")
    print("   - 최대 병렬화 (256 threads)")
    print("   - 전체 사전 로드")
    print("   - 작은 청크 (150)")
    print("   - 모든 CPU 코어 사용")
