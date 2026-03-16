# Positions JSON 구조

## 생성 위치

`positions_module.py` → `save_positions_json()`

## 저장 경로

```
{positions_root}/{p1}/{p2}/{day}/{basename}.json
```

예: `/appdata/appuser/positions/AB/P1AB/20260312/ABC123_S01_W12_20260312_143000_87.5_3.2.json`

## 전체 구조

```json
{
  "bucket_b_key": "20260122/ABC123H3P_W01_20260122_022719.gz",
  "root": "ABC123H3",
  "step": "S01",
  "wafer": "W01",
  "stime": "20260122_022718",
  "partid": "PARTID-VALUE",
  "tester": "TESTER-VALUE",
  "device": "DEVICE-VALUE",
  "pgm": "PGM-VALUE",
  "netd": 332,
  "gd": 290,
  "yield": "87.35",
  "sys": "3.21",
  "tm": "Normal",
  "lt": "EE",
  "coord": {
    "rot_code": 5,
    "x_min_abs": -10,
    "y_min_abs": -10,
    "x_max_abs": 10,
    "y_max_abs": 10,
    "tiles_w_rot": 20,
    "tiles_h_rot": 20,
    "grid_edges": {
      "xs": [0, 115, 230, "..."],
      "ys": [0, 115, 230, "..."]
    },
    "canvas": { "width": 2304, "height": 2304 },
    "scale": { "sx": 1.0, "sy": 1.0 },
    "border": 1,
    "defect_border": 2,
    "center_rule": { "even_x_zero": "left", "even_y_zero": "down" }
  },
  "chips": [
    {
      "x_abs": 8,
      "y_abs": 2,
      "b": "1",
      "f": { "2342": "3219", "2456": "3904" },
      "q": { "5501": "99", "5502": "20" },
      "x_cal": -3,
      "y_cal": -10,
      "rect": {
        "x0": 768, "y0": 192, "x1": 864, "y1": 288,
        "quad": [[768,192],[864,192],[864,288],[768,288]]
      }
    }
  ]
}
```

## 필드 설명

### Wafer 레벨 (top-level)

| 필드 | 타입 | 설명 | 출처 |
|------|------|------|------|
| `bucket_b_key` | string | 매칭된 Bucket B 파일 키 (없으면 `""`) | Bucket B 매칭 |
| `root` | string | LOT ID 앞부분 | Bucket A 파일 헤더 `:WFID=` |
| `step` | string | 스텝명 | `:WFID=` 파싱 |
| `wafer` | string | 웨이퍼 번호 | `:WFID=` 파싱 |
| `stime` | string | 검사 시간 `YYYYMMDD_HHMMSS` | `:STIME=` (10번째 줄) |
| `partid` | string | 파트 ID | `:PARTID=` |
| `tester` | string | 테스터 장비명 | `:TESTER=` |
| `device` | string | 디바이스명 | `:DEVICE=` |
| `pgm` | string | 프로그램명 | `:PGM=` |
| `netd` | int | Net Die 수 (총 테스트 가능 다이) | `:NETD=` |
| `gd` | int | Good Die 수 (BIN < 200) | 계산 |
| `yield` | string | `gd / netd * 100` (소수점 2자리) | 계산 |
| `sys` | string | defect bin count / netd * 100 (소수점 2자리) | 계산 |
| `tm` | string | Test Mode (예: `Normal`, `Engineer`, `Test`) | Bucket B 1번째 줄 `TM=` |
| `lt` | string | Lot Type (예: `EE`, `PE`, `PT`) | Bucket B 5번째 줄 `LT=` |

### sys 계산 BIN 목록

| kind | 대상 BIN |
|------|----------|
| 00P | 285, 286, 287, 288, 290, 291 |
| 00C | 300, 385, 386, 388, 389, 390 |

### coord (좌표 정보)

| 필드 | 설명 |
|------|------|
| `rot_code` | 회전 코드 (5=없음, 7=90CCW, 3=270CCW, 0=180) |
| `x_min_abs` / `x_max_abs` | 원본 X 좌표 범위 |
| `y_min_abs` / `y_max_abs` | 원본 Y 좌표 범위 |
| `tiles_w_rot` / `tiles_h_rot` | 회전 후 타일 수 |
| `grid_edges` | 픽셀 좌표 격자 경계 (`xs`, `ys`) |
| `canvas` | 최종 이미지 크기 (정사각 리사이즈 후) |
| `scale` | 리사이즈 스케일 (sx, sy) |
| `border` | 기본 격자 테두리 두께 |
| `defect_border` | BIN/invalid 테두리 두께 |

### chips[] (칩별 데이터)

| 필드 | 타입 | 설명 |
|------|------|------|
| `x_abs` | int | 원본 X 좌표 |
| `y_abs` | int | 원본 Y 좌표 |
| `b` | string | BIN 값 (앞쪽 0 제거). 모든 칩 포함 (normal/defect/invalid 무관) |
| `f` | object | FTN 데이터 (Bucket B) `{ "키": "값", ... }` |
| `q` | object | QTN 데이터 (Bucket B) `{ "키": "값", ... }` |
| `x_cal` | int | 중심 기준 X 좌표 |
| `y_cal` | int | 중심 기준 Y 좌표 |
| `rect` | object | 픽셀 영역 `{x0, y0, x1, y1, quad}` |
