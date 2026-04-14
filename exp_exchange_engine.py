"""
Commit-Reveal 경험치↔에테르 대박 교환 엔진

가챠(gacha_engine.py)와 동일한 commit-reveal 패턴:
- 서버 시드 생성 → seed_hash 커밋 → 클라이언트 논스 수신 → reveal → 배수 결정
- 매 교환마다 독립 롤 (per-transaction)
- 기대값 0.98028 (하우스 엣지 1.972%)

★ 의존성: hashlib / secrets — 표준 라이브러리만.
  이 파일은 GitHub 공개 레포(netronsdev/arkcity)에 그대로 복사되어
  `verify.py --exchange-rate-hash` 로 로컬 재계산 검증에 사용됨.
  서버 전용 모듈(game_core/database 등) import 금지.
"""

import hashlib
import secrets

# ══════════════════════════════════════════
#  확률표
# ══════════════════════════════════════════

JACKPOT_RATE_TABLE_VERSION = 1

# (배수, 확률) — 합산 1.0
# 기대값 = 0.9*0.8892 + 1.2*0.06 + 1.5*0.03 + 2.0*0.015 + 5.0*0.005 + 10.0*0.0008
#        = 0.98028
# 하우스 엣지 = 1 - 0.98028 ≈ 0.01972 (약 1.972%)
JACKPOT_RATE_TABLE = [
    (10.0, 0.0008),   # 차원 붕괴 (0.08%)
    (5.0,  0.0050),   # 격렬한 왜곡 (0.5%)
    (2.0,  0.0150),   # 강한 왜곡 (1.5%)
    (1.5,  0.0300),   # 가벼운 왜곡 (3.0%)
    (1.2,  0.0600),   # 미세 왜곡 (6.0%)
    (0.9,  0.8892),   # 일반 (88.92%)
]

# (배수, cumulative_upper_bound_exclusive) — roll % 100000 매핑
# 희귀 → 흔함 순서로 정렬 (가챠와 동일)
# roll 0 ~ 79    → 10.0x
# roll 80 ~ 579  → 5.0x
# roll 580 ~ 2079 → 2.0x
# roll 2080 ~ 5079 → 1.5x
# roll 5080 ~ 11079 → 1.2x
# roll 11080 ~ 99999 → 0.9x
JACKPOT_BUCKETS = [
    (10.0, 80),
    (5.0,  580),
    (2.0,  2080),
    (1.5,  5080),
    (1.2,  11080),
    (0.9,  100000),
]

# 과거 버전 영구 보존 (확률표 변경 시 v2에 추가, 검증 엔드포인트가
# rate_table_version 컬럼으로 올바른 과거 테이블을 선택)
HISTORICAL_BUCKETS = {
    1: JACKPOT_BUCKETS,
}


# ══════════════════════════════════════════
#  Commit-Reveal 핵심 함수 (가챠와 동일 구조)
# ══════════════════════════════════════════

def generate_seed() -> str:
    """서버 시드 생성 (64-char hex)"""
    return secrets.token_hex(32)


def commit(seed: str) -> str:
    """시드 커밋 — SHA-256 해시만 공개"""
    return hashlib.sha256(seed.encode()).hexdigest()


def reveal_roll(seed: str, nonce: str) -> int:
    """seed+nonce → SHA-256 → 앞 32비트 정수 → % 100000 → roll"""
    combined = f"{seed}:{nonce}"
    hash_hex = hashlib.sha256(combined.encode()).hexdigest()
    rv = int(hash_hex[:8], 16)
    return rv % 100000


def roll_to_multiplier(roll: int, version: int = JACKPOT_RATE_TABLE_VERSION) -> float:
    """roll (0~99999) → 배수 매핑 (버전별 테이블 사용)"""
    buckets = HISTORICAL_BUCKETS.get(version, JACKPOT_BUCKETS)
    for mul, upper in buckets:
        if roll < upper:
            return mul
    return 0.9  # 안전 폴백 (도달 불가)


def resolve(seed: str, nonce: str) -> dict:
    """seed+nonce → {roll, multiplier} (1회 교환 결과)"""
    roll = reveal_roll(seed, nonce)
    multiplier = roll_to_multiplier(roll)
    return {
        "roll": roll,
        "multiplier": multiplier,
    }


# ══════════════════════════════════════════
#  확률표 해시 (변조 검증용, GitHub 공개)
# ══════════════════════════════════════════

def compute_exchange_rate_table_hash() -> str:
    """확률표의 SHA-256 해시 — 블록체인 앵커링 + GitHub 공개용

    canonical 문자열 포맷은 gacha_engine.compute_rate_table_hash() 와 동형.
    확률표(rate) + 버킷(upper) 둘 다 포함하여 어느 쪽이 바뀌어도 해시가 달라짐.
    """
    canonical = f"EXCHANGE_RATE_TABLE_VERSION={JACKPOT_RATE_TABLE_VERSION};"
    for mul, prob in JACKPOT_RATE_TABLE:
        canonical += f"({mul},{prob});"
    for mul, upper in JACKPOT_BUCKETS:
        canonical += f"B({mul},{upper});"
    return hashlib.sha256(canonical.encode()).hexdigest()


EXCHANGE_RATE_TABLE_HASH = compute_exchange_rate_table_hash()
