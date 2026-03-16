# 팔레트 및 테두리 규칙

## 32색 팔레트 인덱스

| 인덱스 | 키 | 기본 색상 | 용도 |
|--------|-----|----------|------|
| 0 | chip0 | `#FFFFFF` | Grade 0 (pass) |
| 1 | chip1 | `#9B9B9B` | Grade 1 |
| 2 | chip2 | `#009619` | Grade 2 |
| 3 | chip3 | `#0000FF` | Grade 3 |
| 4 | chip4 | `#D91DFF` | Grade 4 |
| 5 | chip5 | `#FFFF00` | Grade 5 |
| 6 | chip6 | `#FF0000` | Grade 6 |
| 7 | chip7 | `#000000` | Grade 7 |
| 8 | bg | `#FEFEFE` | 배경 |
| 9 | text | `#000001` | 텍스트 |
| 10 | border | `#BEBEBE` | 기본 격자 테두리 |
| 11 | border_inv | `#FF9900` | Invalid 칩 테두리 |
| 12 | border_b285 | `#0099FF` | BIN 285 |
| 13 | border_b286 | `#FF714F` | BIN 286 |
| 14 | border_b287 | `#66FFCC` | BIN 287 |
| 15 | border_b288 | `#DA26CD` | BIN 288 |
| 16 | border_b290 | `#FFD700` | BIN 290 |
| 17 | border_b291 | `#32CD32` | BIN 291 |
| 18 | border_b300 | `#AAAAAA` | BIN 300 |
| 19 | border_b385 | `#00C8FF` | BIN 385 |
| 20 | border_b386 | `#FF00C8` | BIN 386 |
| 21 | border_b388 | `#00FF66` | BIN 388 |
| 22 | border_b389 | `#FF6666` | BIN 389 |
| 23 | border_b390 | `#6666FF` | BIN 390 |
| 24 | border_etc | `#999999` | ETC (기타 BIN) |
| 25~30 | (미사용) | `#000000` | 예약 |
| 31 | (invalid fill) | `#FFFFFF` | Invalid 칩 내부 채움 |

색상은 `color-legends.json`에서 로드. 없으면 위 기본값 사용.

## 테두리 적용 규칙

### 두께

- 기본 격자 테두리: `border_thickness` (기본 1px)
- BIN/Invalid/ETC 테두리: `defect_border_thickness` (기본 2px)

### BIN별 테두리 분류

```
칩 데이터 도착
    │
    ├─ transformed_values 없음 → Invalid (border_inv, #FF9900)
    │                            + 내부 IDX_INVALID_FILL (#FFFFFF)
    │                            + BIN 번호 텍스트 표시
    │
    ├─ BIN < 200 → Normal (기본 border만, 오버레이 없음)
    │
    ├─ 00P: BIN ∈ {285,286,287,288,290,291} → 해당 BIN 색상 테두리
    │  00C: BIN ∈ {300,385,386,388,389,390} → 해당 BIN 색상 테두리
    │
    └─ BIN >= 200 그 외 → ETC (border_etc, #999999)
```

### kind별 BIN 테두리 세트

| kind | 테두리 적용 BIN |
|------|----------------|
| 00P | 285, 286, 287, 288, 290, 291 |
| 00C | 300, 385, 386, 388, 389, 390 |
| 공통 | ETC (위 목록 외 BIN >= 200) |

## 회전 코드

| rot_code | 동작 |
|----------|------|
| 5 | 회전 없음 (기본) |
| 7 | 90도 반시계 (transpose + 상하반전) |
| 3 | 270도 반시계 (transpose + 좌우반전) |
| 0 | 180도 (상하+좌우 반전) |
