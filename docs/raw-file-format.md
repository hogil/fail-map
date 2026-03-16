# Raw 파일 형식 (Bucket A / Bucket B)

## Bucket A 파일

### S3 정보

- **버킷**: `eds-ec-memory.fbm-data`
- **확장자**: `.Z` (LZW 압축)
- **경로 형식**: `YYYYMMDD/{wafer}_{lotid}-00{suffix}_{middle}_{date}_{time}.Z`

### 파일명 예시

```
20260122/01_ABC123H3-00P_N_20260122_022718.Z
         ^^_^^^^^^^^-^^^_^_^^^^^^^^_^^^^^^.^
         │  │        │   │ │        │      └─ 확장자 (.Z)
         │  │        │   │ └────────┴──────── 날짜/시간 (YYYYMMDD_HHMMSS)
         │  │        │   └───────────────── 중간 구분자 (_N_)
         │  │        └──────────────────── -00 + 접미사 (-00P 또는 -00C)
         │  └──────────────────────────── LOT ID (ABC123H3)
         └─────────────────────────────── wafer 번호 (01)
```

### 파일 내부 구조 (텍스트)

```
:WFID=ABC123H3-S01.W01          ← 줄 2 (1-based): LOT-STEP.WAFER
:PARTID=PART-VALUE              ← 헤더 영역
:TESTER=TESTER-VALUE
:DEVICE=DEVICE-VALUE
:PGM=PGM-VALUE
...
:ROT=5                          ← 줄 9: 회전 코드
:STIME=2026/01/22 02:27:18      ← 줄 10: 검사 시간
...
:XSIZE=24                       ← 줄 12: X 타일 크기
:YSIZE=24                       ← 줄 13: Y 타일 크기
:NETD=332                       ← Net Die 수
...
X=8 Y=2 b=0001                  ← 칩 데이터 시작
#0F0F0F0F0F0F0F0F0F0F0F0F...   ← hex 블록 (grade 데이터)
0F0F0F0F0F0F0F0F0F0F0F0F...
...
X=9 Y=2 b=0285                  ← 다음 칩
#0F0F0F0F...
```

### 헤더 파싱 (utils.py)

| 헤더 키 | 파싱 함수 | 설명 |
|---------|-----------|------|
| `:WFID=` | `find_initial_values_from_lines()` | root, step, wafer 추출 |
| `:DEVICE=` | `hval(lines, ":DEVICE=")` | 디바이스명 → p1/p2 매칭용 |
| `:STIME=` | `parse_stime()` | 검사 시간 → `YYYYMMDD_HHMMSS` |
| `:NETD=` | `hval(lines, ":NETD=")` | Net Die 수 |
| `:ROT=` | 줄 9 파싱 | 회전 코드 (0/3/5/7) |
| `:XSIZE=` / `:YSIZE=` | 줄 12/13 | 타일 내부 픽셀 크기 |

### Hex 블록 → Grade 변환

```
hex char → grade index
  '0'    → 0 (chip0, pass)
  '9'    → 1 (chip1)
  'A'    → 2 (chip2)
  'B'    → 3 (chip3)
  'C'    → 4 (chip4)
  'D'    → 5 (chip5)
  'E'    → 6 (chip6)
  'F'    → 7 (chip7)
```

한 줄에 `#` + hex 쌍(2바이트)으로 표현. 두 번째 바이트만 사용.

### BIN 값 (`b=`)

| 범위 | 분류 | 이미지 테두리 |
|------|------|--------------|
| < 200 | Normal (pass) | 기본 border (회색) |
| 285, 286, 287, 288, 290, 291 | 00P defect | 각 BIN별 고유 색상 |
| 300, 385, 386, 388, 389, 390 | 00C defect | 각 BIN별 고유 색상 |
| >= 200 그 외 | ETC | `#999999` 테두리 |
| (데이터 없음) | Invalid | `#FF9900` 테두리 + BIN 텍스트 표시 |

---

## Bucket B 파일

### S3 정보

- **버킷**: `eds.m-eds-map-raw`
- **확장자**: `.gz` (gzip 압축)
- **경로 형식**: `YYYYMMDD/{lotid}{suffix}_{wafer}_{date}_{time}.gz`

### 파일명 예시

```
20260122/ABC123H3P_W01_20260122_022719.gz
         ^^^^^^^^^_^^^_^^^^^^^^_^^^^^^.^^
         │         │   │        │      └─ 확장자 (.gz)
         │         │   └────────┴──────── 날짜/시간
         │         └─────────────────── W + wafer 번호
         └───────────────────────────── LOT ID + suffix
```

### A → B 파일명 변환 규칙

```
Bucket A: 01_ABC123H3-00P_N_20260122_022718.Z
          ↓
Bucket B: ABC123H3P_W01_20260122_022718.gz (±10초 범위)

1. wafer: 01 → W01
2. LOT ID: ABC123H3-00P → ABC123H3P (-00 제거, suffix만 결합)
3. 중간 구분자 _N_ → 제거
4. 시간: ±10초 오프셋 허용
5. 확장자: .Z → .gz
```

### 파일 내부 구조 (텍스트)

```
:WFID=ABC123H3P.W01 TM=Normal   ← 줄 1: TM 추출
...
LT=EE ...                        ← 줄 5: LT 추출
...
FTN=2342 2456 9834 3834           ← FTN 키 목록
QTN=5501 5502                     ← QTN 키 목록
...
X=8 Y=2                          ← 칩별 데이터
F=3219 3904 669 1988              ← FTN 값 (키 순서 대응)
Q=99 20                           ← QTN 값 (키 순서 대응)
X=9 Y=2
F=82 1142 2565 2022
Q=96 32
...
```

### Bucket B 파싱 (bucketb_module.py)

| 데이터 | 위치 | positions JSON 필드 |
|--------|------|-------------------|
| TM | 1번째 줄 `TM=` | `tm` |
| LT | 5번째 줄 `LT=` (2글자) | `lt` |
| FTN 키 | `FTN=` 줄 | chips[].f 키 |
| QTN 키 | `QTN=` 줄 | chips[].q 키 |
| F= 값 | 칩별 `F=` 줄 | chips[].f 값 |
| Q= 값 | 칩별 `Q=` 줄 | chips[].q 값 |
