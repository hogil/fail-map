# Pipeline Overview

## 전체 흐름

```
device_info.txt (token/p1/p2 목록)
        │
        ▼
┌──────────────────────────────────────────────────┐
│  1. S3 Bucket A 파일 목록 조회                     │
│     eds-ec-memory.fbm-data (.Z 압축)              │
│     - 시간 윈도우 내 폴더 선택 (YYMMDD)             │
│     - 파일명에서 token + 00P/00C 매칭              │
│     - LOT ID '1' 시작 파일도 별도 수집              │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  2. Bucket B 매칭 (선택적)                         │
│     eds.m-eds-map-raw (.gz 압축)                  │
│     - A 파일명 → B 파일명 변환 (LOT+Wafer+시간)     │
│     - 시간 오프셋 -10~+10초 범위 탐색               │
│     - 실패 시 fallback: closest time 선택           │
│     - miss 기록: positions_root/match_miss.json    │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  3. 다운로드 + 압축 해제                            │
│     - 128 스레드 병렬 다운로드                       │
│     - .Z(LZW), .gz, .7z, .zip, .tar 자동 감지      │
│     - 중첩 아카이브 재귀 해제 (depth 6)              │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  4. 파일 파싱 (Cython/Python)                      │
│     - 헤더: WFID, DEVICE, STIME, NETD, ROT 등      │
│     - 칩별: X=, Y=, b=, hex 블록 → grade 변환       │
│     - token/p1/p2/kind 태깅                        │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  5. 이미지 생성 (ProcessPoolExecutor)               │
│     - 32색 팔레트 인덱스 PNG                        │
│     - grade 0~7 → 색상, BIN별 테두리 색상            │
│     - 회전(rot_code), 정사각 리사이즈                 │
│     - 파일명: root_step_wafer_stime_yield_sys.png   │
└──────────────┬───────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────┐
│  6. Positions JSON 생성                            │
│     - 칩별 좌표, bin, rect, f/q 데이터               │
│     - wafer 레벨 메타: yield, sys, tm, lt 등         │
│     - Bucket B match 정보 포함                      │
└──────────────────────────────────────────────────┘
```

## 소스 파일 역할

| 파일 | 역할 |
|------|------|
| `fail-map-with-bucketb.py` | 메인 파이프라인 (오케스트레이션) |
| `utils.py` | S3 클라이언트, 파일 파싱, 팔레트, 헬퍼 |
| `bucketb_module.py` | Bucket B 매칭 로직 |
| `positions_module.py` | Positions JSON 생성 |
| `cython_functions.pyx` | hex→grade 변환 최적화 |

## 설정 (PipelineConfig)

| 항목 | 기본값 | 설명 |
|------|--------|------|
| `bucket_name` | `eds-ec-memory.fbm-data` | Bucket A |
| `hours_back_start` | `0` | 시간 윈도우 시작 (현재 기준) |
| `hours_back_end` | `2` | 시간 윈도우 끝 |
| `download_threads` | `128` | S3 다운로드 병렬 수 |
| `cpu_processes` | `min(cpu, 24)` | 이미지 생성 프로세스 수 |
| `base_root` | `/appdata/appuser/images` | PNG 저장 루트 |
| `positions_root` | `/appdata/appuser/positions` | JSON 저장 루트 |
| `df_path` | `/appdata/appuser/project/device_info.txt` | 디바이스 정보 파일 |
