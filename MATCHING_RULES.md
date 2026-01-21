# Bucket B 파일 매칭 규칙 (Detailed Matching Rules)

## 개요

Bucket A(eds-ec-memory.fbm-data)와 Bucket B(eds.m-eds-map-raw) 파일을 매칭하는 상세 규칙입니다.

## 파일명 변환 규칙

### Bucket A 파일명 구조
```
20260122/01_ABC123H3-00P_N_20260122_022718.Z
         ^^_^^^^^^^^-^^^_^_^^^^^^^^_^^^^^^.^
         │  │        │   │ │        │      └─ 확장자 (.Z)
         │  │        │   │ └────────┴──────── 날짜/시간 (YYYYMMDD_HHMMSS)
         │  │        │   └───────────────── 중간 구분자 (_N_)
         │  │        └──────────────────── -00 + 접미사 1글자 (-00P)
         │  └──────────────────────────── LOT ID 기본 부분 (ABC123H3)
         └─────────────────────────────── wafer 번호 (01)
```

### Bucket B 파일명 구조
```
20260122/ABC123H3P_W01_20260122_022719.gz
         ^^^^^^^^^_^^^_^^^^^^^^_^^^^^^.^^
         │         │   │        │      └─ 확장자 (.gz)
         │         │   └────────┴──────── 날짜/시간 (YYYYMMDD_HHMMSS, +0~10초)
         │         └─────────────────── W + wafer 번호 (W01)
         └───────────────────────────── LOT ID (ABC123H3P)
```

## 변환 규칙 상세

### 1. LOT ID 변환
- **Bucket A**: `ABC123H3-00P`
- **변환 과정**: `-00` 제거, 접미사 `P`만 결합
- **Bucket B**: `ABC123H3P`

**코드 구현**:
```python
# pattern: r'(\d{2})_([A-Z0-9]+)-00([A-Z])_[A-Z]_(\d{8})_(\d{6})\.Z'
# group(2): LOT BASE (ABC123H3)
# group(3): SUFFIX (P)
lot_id = f"{m.group(2)}{m.group(3)}"  # "ABC123H3P"
```

### 2. Wafer 번호 변환
- **Bucket A**: `01` (앞에 위치)
- **변환 과정**: `W` 접두사 추가, 파일명 중간으로 이동
- **Bucket B**: `W01` (LOT ID 뒤에 위치)

**코드 구현**:
```python
wafer = f"W{info['wafer']}"  # "W01"
```

### 3. 중간 구분자 제거
- **Bucket A**: `_N_` (또는 다른 단일 문자)
- **변환 과정**: 완전히 제거됨
- **Bucket B**: 없음

### 4. 시간 오프셋
- **Bucket A**: `022718` (2시 27분 18초)
- **변환 과정**: +0~10초 범위로 매칭
- **Bucket B**: `022718`, `022719`, ..., `022728` 중 하나

**코드 구현**:
```python
def generate_bucket_b_patterns(info, offset_range=(0, 10)):
    dt = datetime.strptime(f"{base_date}_{base_time}", "%Y%m%d_%H%M%S")
    for offset in range(offset_range[0], offset_range[1] + 1):
        new_dt = dt + timedelta(seconds=offset)
        patterns.append(f"{lot_id}_{wafer}_{new_dt:%Y%m%d_%H%M%S}.gz")
```

**특수 케이스**: 자정 넘어가는 경우도 안전하게 처리
```
Input:  20260122_235959  offset=+2
Output: 20260123_000001  (날짜도 자동 변경)
```

### 5. 확장자 변환
- **Bucket A**: `.Z` (LZW 압축)
- **Bucket B**: `.gz` (gzip 압축)

## 실제 예시

### 예시 1: 기본 케이스
```
Bucket A: 20260122/01_ABC123H3-00P_N_20260122_022718.Z
          ↓
Bucket B: 20260122/ABC123H3P_W01_20260122_022718.gz  (+0초)
          20260122/ABC123H3P_W01_20260122_022719.gz  (+1초)
          20260122/ABC123H3P_W01_20260122_022720.gz  (+2초)
          ...
          20260122/ABC123H3P_W01_20260122_022728.gz  (+10초)
```

### 예시 2: 자정 넘어가는 케이스
```
Bucket A: 20260122/05_ABC123-00N_M_20260122_235959.Z
          ↓
Bucket B: 20260122/ABC123N_W05_20260122_235959.gz  (+0초, 같은 날)
          20260122/ABC123N_W05_20260123_000000.gz  (+1초, 날짜 변경!)
          20260122/ABC123N_W05_20260123_000009.gz  (+10초)
```

## 매칭 프로세스

### 1단계: Bucket B 인덱스 구축
```python
bucket_b_index = FastBucketBIndex()
bucket_b_index.build_from_keys(all_bucket_b_keys)
```
- 모든 Bucket B 파일의 basename을 해시맵에 저장
- O(1) 검색을 위한 사전 준비

### 2단계: Bucket A 파일 파싱
```python
info = parse_bucket_a_filename("20260122/01_ABC123H3-00P_N_20260122_022718.Z")
# → {'wafer': '01', 'lot_id': 'ABC123H3P', 'date': '20260122', 'time': '022718'}
```

### 3단계: Bucket B 패턴 생성
```python
patterns = generate_bucket_b_patterns(info, offset_range=(0, 10))
# → ['ABC123H3P_W01_20260122_022718.gz',
#     'ABC123H3P_W01_20260122_022719.gz',
#     ...,
#     'ABC123H3P_W01_20260122_022728.gz']  # 총 11개
```

### 4단계: 빠른 매칭 (O(1))
```python
for pattern in patterns:
    if pattern in bucket_b_index.basename_to_key:
        matched_key = bucket_b_index.basename_to_key[pattern]
        return matched_key  # 첫 매칭 반환
```

### 5단계: 첫 줄 추출 (매칭 검증용)
```python
first_line = text_b.split("\n", 1)[0].rstrip("\r")
```
- Bucket B 파일의 첫 줄을 position JSON에 저장
- 매칭 성공/실패를 눈으로 확인 가능

## JSON 출력 형식

### wafer 레벨 (top-level)
```json
{
  "image_path": "/appdata/appuser/images/.../file.png",
  "bucket_b_match": {
    "matched": true,
    "bucket": "eds.m-eds-map-raw",
    "key": "20260122/ABC123H3P_W01_20260122_022719.gz",
    "first_line": ":WFID=ABC123H3P.W01",
    "first_line_ok": true,
    "time_offset_range": [0, 10]
  },
  "chips": [...]
}
```

### chip 레벨 (각 칩별)
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
        "stime": "20260122_022719",
        "partid": "...",
        "tester": "..."
      },
      "bucket_b_match": {
        "matched": true,
        "bucket": "eds.m-eds-map-raw",
        "key": "20260122/ABC123H3P_W01_20260122_022719.gz",
        "first_line": ":WFID=ABC123H3P.W01",
        "first_line_ok": true,
        "time_offset_range": [0, 10]
      }
    }
  ]
}
```

## 성능 최적화

### 병렬 다운로드
```python
with ThreadPoolExecutor(max_workers=2) as executor:
    future_a = executor.submit(download_bucket_a, keys_a)
    future_b = executor.submit(download_bucket_b, keys_b)
```
- Bucket A와 B를 동시에 다운로드
- 총 시간 = max(T_A, T_B) + 오버헤드

### 메모리 조인
```python
joined = join_contents_by_filename(contents_a, contents_b)
```
- 메모리에서 O(N) 조인
- 반복적인 S3 접근 불필요

### Fast Pattern Matching
- 시간 복잡도: O(파일 수 × 11개 패턴) ≈ O(N)
- 기존 순차 검색: O(N × M) (N=Bucket A, M=Bucket B)

## 디버그 출력

### Bucket B 샘플
```
[DEBUG] Bucket B sample basenames:
  - ABC123H3P_W01_20260122_022719.gz
  - ABC456N_W02_20260122_030145.gz
  ...
```

### 매칭 상세
```
[DEBUG] A[0] Full key: 20260122/01_ABC123H3-00P_N_20260122_022718.Z
[DEBUG] A[0] Basename: 01_ABC123H3-00P_N_20260122_022718.Z
[DEBUG] A[0] Parsed: {'wafer': '01', 'lot_id': 'ABC123H3P', 'date': '20260122', 'time': '022718'}
[DEBUG] A[0] Generated B patterns (showing first 3 of 11):
  ✓ ABC123H3P_W01_20260122_022718.gz
  ✓ ABC123H3P_W01_20260122_022719.gz
  ✗ ABC123H3P_W01_20260122_022720.gz
[DEBUG] A[0] Match result: ✓ MATCHED
[DEBUG] A[0] Matched B key: 20260122/ABC123H3P_W01_20260122_022719.gz
```

## 참고 파일

- **구현 파일**: `fail-map-dual-bucket.py`
  - `parse_bucket_a_filename()`: Line 125-145
  - `generate_bucket_b_patterns()`: Line 147-170
  - `FastBucketBIndex.find_match_map()`: Line 268-313
- **README**: `README.md`
- **테스트**: 실행 후 [DEBUG] 로그 확인

## 문의 사항

매칭이 실패하는 경우 다음을 확인하세요:
1. Bucket B 파일명이 `LOTID_WWW_YYYYMMDD_HHMMSS.gz` 형식인지
2. 시간 차이가 0~10초 범위인지
3. LOT ID가 정확히 일치하는지 (대소문자 포함)
4. 디버그 로그에서 "Generated B patterns" 확인
