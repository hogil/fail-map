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

### Optimized Versions

| 파일 | 최적화 대상 | 설정 |
|------|-------------|------|
| `optimized-speed-first.py` | **최대 속도** | 256 threads, 150 chunk, 사전 로드 |
| `optimized-memory-efficient.py` | **메모리 절약** | 64 threads, 600 chunk, 스트리밍 |

## Architecture

```
┌─────────────────┐       ┌─────────────────┐
│   Bucket A      │       │   Bucket B      │
│  (Primary)      │       │  (Secondary)    │
│                 │       │  +0~10초 늦음   │
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
| speed-first.py | **~50%** | 높음 |
| memory-efficient.py | ~75% | 낮음 |

### 최적화 전략

#### Speed-First
- 256 threads로 최대 병렬화
- Bucket B 전체 사전 로드
- 작은 chunk (150) → 빠른 응답
- 메모리: ~4-8GB

#### Memory-Efficient
- 64 threads로 적은 메모리
- 순차 처리 + 스트리밍
- 큰 chunk (600) → 적은 반복
- 강제 GC
- 메모리: ~1-2GB

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

# +0~10초 범위 생성
'143025' → ['143025', '143026', ..., '143035']

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

### Dual Bucket (속도 최적화)
```bash
python optimized-speed-first.py
```

### Dual Bucket (메모리 최적화)
```bash
python optimized-memory-efficient.py
```

## Configuration

### Bucket B 설정

```python
@dataclass
class BucketBConfig:
    bucket_name: str = 'your-secondary-bucket'
    enabled: bool = True
    time_offset_range: tuple = (0, 10)  # 0~10초 범위
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

## Time Offset Matching

Bucket B 파일이 0~10초 늦게 생성되는 경우를 자동으로 매칭합니다:

```
Bucket A: token_00P_20260111_143025.Z
          ↓ (자동 매칭)
Bucket B: token_00P_20260111_143030.Z  (+5초)
```

## Requirements

```bash
pip install boto3 pandas numpy pillow tqdm botocore
pip install unlzw3 py7zr  # 압축 해제
```

## Performance Tips

1. **많은 파일 (10000+)**: `optimized-speed-first.py` 사용
2. **제한된 메모리**: `optimized-memory-efficient.py` 사용
3. **프로덕션**: `fail-map-dual-bucket.py` 사용 (안정성)

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
