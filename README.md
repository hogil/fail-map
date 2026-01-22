# Fail Map - Dual Bucket Pipeline

반도체 웨이퍼 맵 처리 파이프라인 with **초고속 Dual Bucket 지원**

## Features

- S3 호환 스토리지에서 압축된 웨이퍼 맵 데이터 자동 수집
- 다중 압축 포맷 지원 (.Z, .gz, .7z, .zip, .tar)
- Cython 최적화 파싱
- 팔레트 기반 PNG 이미지 생성
- **NEW: Dual Bucket 병렬 처리** (시간차 파일 자동 매칭)
- 좌표 정보 JSON 자동 생성

## Files

### Core Implementations

| 파일 | 설명 | 특징 |
|------|------|------|
| `fail-map-claude.py` | **원본** 싱글 bucket 파이프라인 | 안정적, 검증된 코드 |
| `fail-map-dual-bucket.py` | **Dual bucket 표준 버전** | 균형잡힌 성능, 프로덕션 권장 ⭐ |
| `fail-map-bucketb-prefixlist.py` | Bucket B 매칭(-10~+10초) + positions JSON 기록 | **Prefix-list 방식(추천)** |
| `fail-map-bucketb-prefixlist-fallback.py` | Bucket B 매칭(+ positions JSON 기록) | **Prefix-list + closest fallback** |
| `fail-map-bucketb-nearest.py` | Bucket B 매칭(-10~+10초) + positions JSON 기록 | **같은 폴더에서 시간 가장 가까운 파일 매칭** |
| `fail-map-bucketb-closest.py` | Bucket B 매칭(+ positions JSON 기록) | **같은 LOT+WAFER에서 시간 가장 가까운 파일(제한 없음)** |

### Optimized Versions

> (삭제됨) `optimized-speed-first.py`, `optimized-memory-efficient.py` 는 유지보수 대상으로 두지 않아 저장소에서 제거했습니다.

## Architecture

```
┌─────────────────┐       ┌─────────────────┐
│   Bucket A      │       │   Bucket B      │
│  (Primary)      │       │  (Secondary)    │
│                 │       │  -10~+10초      │
└────────┬────────┘       └────────┬────────┘
         │                         │
         │    병렬 다운로드         │
         ├─────────────────────────┤
         │                         │
         ▼                         ▼
    ┌────────────────────────────────┐
    │  Fast Pattern Matching Engine  │
    │  (메모리 인덱스 + 시간 매칭)    │
    └───────────┬────────────────────┘
                │
                ▼
    ┌────────────────────────┐
    │  Memory Join (초고속)  │
    │  파일명 기반 조인       │
    └───────────┬────────────┘
                │
                ▼
    ┌────────────────────────┐
    │  Cython 파싱 (병렬)    │
    └───────────┬────────────┘
                │
                ▼
    ┌────────────────────────┐
    │  PNG 이미지 + JSON     │
    └────────────────────────┘
```

## Performance

### Dual Bucket 성능 비교

| 방식 | 시간 (1000 파일 기준) | 메모리 사용 |
|------|----------------------|-----------|
| 순차 처리 (naive) | 100% (기준) | 낮음 |
| **dual-bucket.py** | **~60%** ⭐ | 중간 |
| (삭제됨) optimized scripts | - | - |

### 최적화 전략

#### Speed-First
> (삭제됨) `optimized-speed-first.py` 는 저장소에서 제거되었습니다.

#### Memory-Efficient
> (삭제됨) `optimized-memory-efficient.py` 는 저장소에서 제거되었습니다.

## Dual Bucket Matching Strategy

### 1. Fast Index Building
```python
# Bucket B 파일 리스트를 1회만 가져와 메모리 인덱스 구축
bucket_b_index = FastBucketBIndex()
bucket_b_index.build_from_keys(all_keys_b)  # ~20초
```

### 2. Pattern Matching (초고속)
```python
# 파일명에서 시간 정보 추출
'20260111/token_00P_20260111_143025.Z' → ('token', '20260111', '143025')

# -10~+10초 범위 생성
'143025' → ['143015', ..., '143025', ..., '143035']

# 메모리 인덱스에서 검색 (< 0.1초)
matched_keys = index.find_matches(bucket_a_keys)
```

### 3. Parallel Download
```python
# Bucket A와 B를 동시 다운로드 (병렬)
with ThreadPoolExecutor(max_workers=2) as executor:
    future_a = executor.submit(download_bucket_a)
    future_b = executor.submit(download_bucket_b)
```

### 4. Memory Join
```python
# 메모리에서 파일명 기반 조인 (< 1초)
joined = join_contents_by_filename(contents_a, contents_b)
```

## Usage

### Single Bucket (원본)
```bash
python fail-map-claude.py
```

### Dual Bucket (표준)
```bash
python fail-map-dual-bucket.py
```

### Bucket B 매칭 + positions JSON 기록 (fail-map.py 기반, 추천)
```bash
python fail-map-bucketb-prefixlist.py
```

### Bucket B 매칭 + positions JSON 기록 (prefixlist + fallback: 후보 없으면 closest 선택)
```bash
python fail-map-bucketb-prefixlist-fallback.py
```

### Bucket B 매칭 + positions JSON 기록 (같은 폴더에서 시간 가장 가까운 파일)
```bash
python fail-map-bucketb-nearest.py
```

### Bucket B 매칭 + positions JSON 기록 (같은 LOT+WAFER에서 가장 가까운 시간, 시간 제한 없음)
```bash
python fail-map-bucketb-closest.py
```
## Configuration

### Bucket B 설정

```python
@dataclass
class BucketBConfig:
    bucket_name: str = 'eds.m-eds-map-raw'
    enabled: bool = True
    time_offset_range: tuple = (-10, 10)  # -10~+10초 범위
    file_ext: str = '.gz'  # Bucket B 파일 확장자
```

### 성능 튜닝

```python
# 속도 우선
download_threads: int = 256
chunk_size: int = 150

# 메모리 우선
download_threads: int = 64
chunk_size: int = 600
```

## JSON Output Format

Dual bucket 모드에서는 각 chip에 `bucket_b` 필드가 추가됩니다:

```json
{
  "chips": [
    {
      "x_abs": 10,
      "y_abs": 20,
      "b": "285",
      "bucket_b": {
        "transformed_values": "...",
        "b": "285",
        "stime": "20260111_143030",
        "partid": "...",
        "tester": "..."
      }
    }
  ]
}
```

## Filename Matching

### Bucket 정보
- **Bucket A (Primary)**: `eds-ec-memory.fbm-data` (.Z 파일)
- **Bucket B (Secondary)**: `eds.m-eds-map-raw` (.gz 파일)

### 파일명 변환 규칙

Bucket B 파일은 특별한 명명 규칙을 가집니다:

```
Bucket A: 01_3BC170H3-00P_N_20260122_022718.Z
          ↓ (자동 변환 + 시간 매칭)
Bucket B: 3BC170H3P_W01_20260122_022719.gz  (+1초)

변환 규칙:
1. 맨 앞 2글자 (01) → W01 (W 접두사 추가)
2. LOT ID (3BC170H3-00P) → 3BC170H3P (-00 제거 + 접미 1글자 결합)
3. 중간 구분자 (_N_) → 제거
4. 날짜/시간 → 유지 (±10초 차이 허용)
5. 확장자 .Z → .gz
```

## Requirements

```bash
pip install boto3 pandas numpy pillow tqdm botocore
pip install unlzw3 py7zr  # 압축 해제
```

## Performance Tips

1. **기본 추천**: `fail-map-bucketb-prefixlist.py` (S3 list 최소화 + 빠름)
2. **prefixlist인데 시간차가 종종 ±범위를 벗어남**: `fail-map-bucketb-prefixlist-fallback.py` (먼저 예측으로 찾고, 없으면 closest)
3. **여러 후보 중 가장 가까운 시간 선택(±range 내)**: `fail-map-bucketb-nearest.py`
4. **시간차가 종종 ±범위를 벗어남(제한 없음)**: `fail-map-bucketb-closest.py`
5. **Bucket B 데이터까지 실제로 파싱/병합**: `fail-map-dual-bucket.py`

## How It Works

### 기존 방식 (느림)
```
Bucket A 다운로드 → 처리 → 이미지/JSON 생성
                          ↓
           Bucket B 다운로드 → JSON 업데이트
           ⏱️ 총 시간 = T_A + T_B
```

### 새 방식 (빠름)
```
Bucket A 다운로드 ──┐
                    ├─→ 병렬 실행 ──→ 메모리 조인 ──→ 처리
Bucket B 다운로드 ──┘
⏱️ 총 시간 = max(T_A, T_B) + overhead (~21초)
```

## License

MIT

## Author

Created with Claude Code
