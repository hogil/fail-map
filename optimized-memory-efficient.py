#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dual Bucket Pipeline - Memory Optimized Version
메모리 절약 우선: 스트리밍 방식, 작은 메모리 풋프린트
- 청크별 순차 처리 (한 번에 하나씩)
- 작은 ThreadPool (64 threads)
- 큰 chunk size로 적은 반복
- 즉시 가비지 컬렉션
- 파일 스트리밍 처리
"""
import os, re, sys, time, json, io, zipfile, tempfile, shutil, gzip, tarfile, subprocess, importlib, multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from pathlib import Path
from collections import defaultdict, Counter
import gc

import boto3
import pandas as pd
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from tqdm import tqdm
from botocore.config import Config

# =================== Memory-First Config ===================

@dataclass
class MemoryFirstConfig:
    bucket_name: str = 'eds-ec-memory.fbm-data'
    region_name: str = ''
    aws_access_key_id: str = 'ho.choi-LakeS3-F6B0U6'
    aws_secret_access_key: str = 'iYb7zYDVzitt4QVkUcR2'
    endpoint_url: str = 'http://lakes3.dataplatform.samsungds.net:9020'

    # 메모리 최적화: 적은 연결과 스레드
    max_pool_connections: int = 128
    download_threads: int = 64  # 기본 128 → 64
    cpu_processes: int = min(multiprocessing.cpu_count() // 2, 12)  # 절반만 사용
    chunk_size: int = 600  # 더 큰 청크 (300 → 600)

    # 가비지 컬렉션
    aggressive_gc: bool = True
    gc_interval: int = 1  # 매 청크마다

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
class BucketBMemoryConfig:
    bucket_name: str = 'eds-ec-memory.fbm-data-secondary'
    region_name: str = ''
    aws_access_key_id: str = 'ho.choi-LakeS3-F6B0U6'
    aws_secret_access_key: str = 'iYb7zYDVzitt4QVkUcR2'
    endpoint_url: str = 'http://lakes3.dataplatform.samsungds.net:9020'
    max_pool_connections: int = 128
    download_threads: int = 64
    enabled: bool = True
    time_offset_range: tuple = (0, 10)

    # 메모리 최적화: 순차 처리
    sequential_download: bool = True  # Bucket A 이후 Bucket B 다운로드
    streaming_mode: bool = True  # 스트리밍 방식

CFG = MemoryFirstConfig()
CFG_B = BucketBMemoryConfig()

print(f"💾 MEMORY-EFFICIENT MODE: threads={CFG.download_threads}, processes={CFG.cpu_processes}, chunk_size={CFG.chunk_size}")

class StreamingProcessor:
    """스트리밍 방식으로 메모리 사용 최소화"""

    @staticmethod
    def process_sequential(contents_a, contents_b):
        """순차적으로 처리하여 메모리 절약"""
        # A 처리
        for item in contents_a:
            yield ('a', item)
            del item  # 즉시 삭제

        # A 완료 후 B 처리
        for item in contents_b:
            yield ('b', item)
            del item

    @staticmethod
    def cleanup():
        """강제 가비지 컬렉션"""
        gc.collect()

if __name__ == "__main__":
    print("💾 Memory-Efficient Dual Bucket Pipeline")
    print("   - 적은 스레드 (64)")
    print("   - 순차 처리")
    print("   - 큰 청크 (600)")
    print("   - 강제 GC")
    print("   - 스트리밍 모드")
