# ArkCity Fairness Verification (아크시티 공정성 검증)

아크시티 게임의 뽑기(가챠), 합성, 거래 기록, 경험치↔에테르 교환 배율이 공정하게 운영되고 있는지
**누구나 독립적으로 검증**할 수 있는 오픈소스 코드입니다.

## 이 레포지토리의 목적

| 검증 항목 | 방법 |
|-----------|------|
| **뽑기 확률** | 서버가 공개한 확률표와 이 코드의 확률표가 동일한지 비교 |
| **뽑기 결과** | seed + nonce를 입력하면 결과를 재현 → 서버 결과와 대조 |
| **시드 무결성** | Commit-Reveal 방식으로 서버가 결과를 미리 조작할 수 없음을 증명 |
| **거래 기록** | Merkle Tree + Polygon 블록체인 앵커링으로 기록 변조 불가 |
| **확률표 변조** | 확률표 해시가 Polygon에 기록됨 → 서버가 몰래 확률을 바꾸면 해시 불일치 |
| **교환 배율 공정성** | 경험치↔에테르 교환의 배율 추첨(0.9x~10x)이 Commit-Reveal로 공정하게 결정됨을 증명 |
| **교환 배율표 변조** | 교환 배율표 해시가 Polygon에 기록됨 → 서버가 몰래 배율을 바꾸면 해시 불일치 |

## 구조

```
gacha_engine.py          ← 서버에서 실제 사용하는 뽑기 엔진 (동일 사본)
exp_exchange_engine.py   ← 서버에서 실제 사용하는 교환 배율 엔진 (동일 사본)
merkle_tree.py           ← 거래 기록 Merkle Tree 검증 로직
verify.py                ← 커맨드라인 검증 도구
```

## 사용법

### 1. 뽑기 확률표 확인

```bash
python verify.py --rate-hash
```

출력 예시:
```
  ArkCity 확률표
  버전: v1.0
  해시: a1b2c3d4...

  [뽑기 확률]
    일반    92.869%
    고급     6.633%
    희귀     0.474%
    영웅     0.024%
    전설    합성 전용 (뽑기 불가)

  [합성 성공률]
    일반→고급    18.0%
    고급→희귀    18.0%
    희귀→영웅    11.0%
    영웅→전설    11.0%
    천장:       20회 실패 시 천장 수령 가능
```

### 2. 뽑기 결과 검증

대시보드(https://netrons.co.kr/game/dashboard)의 소환검증 탭에서 seed와 nonce를 확인한 후:

```bash
python verify.py --seed <서버시드> --nonce <클라이언트논스>
```

결과가 대시보드에 표시된 등급/캐릭터와 일치하면 공정한 뽑기입니다.

### 3. 시드 해시 검증

서버가 뽑기 전에 커밋한 해시와 실제 시드가 일치하는지:

```bash
python verify.py --seed-hash <커밋해시> --seed <서버시드>
```

### 4. 교환 배율표 확인

```bash
python verify.py --exchange-rate-hash
```

출력에는 교환 배율 테이블 버전과 해시, 각 배율별 당첨 확률이 포함됩니다.
대시보드 블록체인 탭에 기록된 `exchangeRateTableHash` 와 이 값이 일치하면
서버가 이 코드와 동일한 교환 배율표를 사용 중임이 증명됩니다.

## Commit-Reveal 뽑기란?

```
1. 서버가 시드(seed)를 생성하고, SHA-256 해시만 클라이언트에 전달 (커밋)
2. 클라이언트가 논스(nonce)를 생성하여 서버에 전달
3. 서버가 seed + nonce를 결합하여 난수 생성 → 등급/캐릭터 결정 (리빌)
4. seed를 공개하여 클라이언트가 결과를 검증
```

서버는 커밋 시점에 이미 시드를 확정했으므로, 클라이언트의 논스를 본 후 시드를 바꿀 수 없습니다.
클라이언트의 논스는 서버가 예측할 수 없으므로, 서버가 원하는 결과를 미리 만들 수 없습니다.

## 경험치↔에테르 교환 배율도 Commit-Reveal

경험치와 에테르를 교환할 때, 일정 확률로 기본 배율(0.9x)보다 높은 배율이 당첨될 수 있습니다.
이 배율 추첨도 가챠와 동일한 Commit-Reveal 방식으로 공정하게 결정됩니다.

```
1. 서버가 교환 seed를 생성하고 SHA-256 해시를 먼저 확정 (커밋)
2. 클라이언트가 교환 요청 시 새 nonce를 보냄
3. 서버가 seed + nonce 로 roll 을 계산하여 배율 테이블에서 당첨 배율 결정 (리빌)
4. 응답에 seed/nonce/seed_hash/roll/multiplier 전부 공개
```

각 교환 로그는 대시보드의 "경험치 교환 검증" 탭에서 SHA-256 재계산으로 즉시 검증할 수 있으며,
`exp_exchange_engine.py` 의 `resolve()` 함수로 동일하게 재현할 수 있습니다.

## Polygon 블록체인 앵커링

- 10분마다 모든 거래 기록의 Merkle Root를 Polygon 메인넷에 기록
- 앵커링 지갑: [대시보드에서 확인](https://netrons.co.kr/game/dashboard)
- 기록된 트랜잭션은 [Polygonscan](https://polygonscan.com)에서 누구나 확인 가능
- 트랜잭션의 Input Data와 대시보드의 Root Hash가 일치하면 기록이 원본 그대로임

## 확률표 · 교환 배율표 해시 검증

서버는 뽑기 확률표와 교환 배율표의 SHA-256 해시를 Polygon에 주기적으로 기록합니다.

1. `python verify.py --rate-hash` → 이 코드의 뽑기 확률표 해시 확인
2. `python verify.py --exchange-rate-hash` → 이 코드의 교환 배율표 해시 확인
3. 대시보드 블록체인 탭에서 기록된 두 해시(`rateTableHash`, `exchangeRateTableHash`) 확인
4. 각각 일치하면 → 서버가 이 코드와 동일한 확률표/배율표를 사용 중

## 라이선스

MIT License — 자유롭게 사용, 수정, 배포 가능
