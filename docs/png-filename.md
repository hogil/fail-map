# PNG 파일명 구조

## 저장 경로

```
{base_root}/{p1}/{p2}/{day}/{filename}.png
```

예: `/appdata/appuser/images/AB/P1AB/20260312/ABC123_S01_12_20260312_143000_87.5_3.2_EE_Normal.png`

## 파일명 형식

```
{root}_{step}_{wafer}_{stime}_{yield}_{sys}_{LT}_{TM}.png
```

| 구성요소 | 예시 | 설명 | 출처 |
|----------|------|------|------|
| `root` | `ABC123` | LOT ID 앞부분 | `:WFID=` 파싱 → `safe_name()` |
| `step` | `S01` | 스텝명 | `:WFID=` 파싱 → `safe_name()` |
| `wafer` | `12` | 웨이퍼 번호 (`W` 제거) | `:WFID=` 파싱, `W` prefix 제거 |
| `stime` | `20260312_143000` | 검사 시간 | `:STIME=` → `YYYYMMDD_HHMMSS` |
| `yield` | `87.5` | Good Die 비율 (소수점 1자리) | `gd / netd * 100` |
| `sys` | `3.2` | Defect BIN 비율 (소수점 1자리) | `sys_count / netd * 100` |
| `LT` | `EE` | Lot Type | Bucket B 5번째 줄 `LT=` (없으면 `NA`) |
| `TM` | `Normal` | Test Mode | Bucket B 1번째 줄 `TM=` (없으면 `NA`) |

## yield 계산

```
yield = (BIN < 200인 칩 수) / NETD * 100
```

## sys 계산

```
sys = (kind별 defect BIN 칩 수) / NETD * 100
```

| kind | 대상 BIN |
|------|----------|
| 00P | 285, 286, 287, 288, 290, 291 |
| 00C | 300, 385, 386, 388, 389, 390 |

## p1/p2 결정 규칙

### 일반 파일 (token 매칭)

- `device_info.txt`에서 token → (p1, p2) 쌍 로드
- `:DEVICE=` 값에 p1이 포함되면 해당 (p1, p2) 사용
- 매칭 실패 시 `("NA", "NA")`

### LOT ID '1' 시작 파일

- 파일명: `07_1AB382-00P_N_20260311_230051.Z` (LOT ID가 `1`로 시작)
- 조건: 00P/00C 필터 AND 적용
- p1 = `:DEVICE=` 값의 끝에서 3번째, 2번째 글자
- p2 = `"P1" + p1`
- 예: `:DEVICE=AKSEIFKXK-AB3` → p1=`AB`, p2=`P1AB`

## 디렉토리 구조 예시

```
/appdata/appuser/images/
├── AB/
│   └── P1AB/
│       └── 20260312/
│           ├── ABC123_S01_12_20260312_143000_87.5_3.2.png
│           └── ABC123_S01_13_20260312_143500_92.1_1.8.png
├── CD/
│   └── P1CD/
│       └── ...
└── NA/
    └── NA/
        └── ...  (매칭 실패한 파일들)
```
