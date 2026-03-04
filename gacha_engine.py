"""
ArkCity Gacha Engine — Commit-Reveal 뽑기 시스템
================================================
이 파일은 아크시티 게임 서버에서 실제로 사용하는 뽑기 엔진의 동일 사본입니다.
누구나 이 코드를 실행하여 자신의 뽑기 결과가 공정했는지 독립적으로 검증할 수 있습니다.

검증 방법:
    1. 대시보드(https://netrons.co.kr/game/dashboard)에서 자신의 소환 기록 확인
    2. seed_hash, nonce, 결과를 확인
    3. 이 스크립트의 verify.py를 사용하여 결과가 동일한지 검증

확률표 버전: v1.0
확률표 SHA-256 해시: 아래 RATE_TABLE_HASH 참조
"""

import hashlib
import secrets

# ══════════════════════════════════════════
#  확률표
# ══════════════════════════════════════════

RATE_TABLE_VERSION = "v1.0"

# (등급, 확률) — 합산 1.0
RATE_TABLE = [
    (0, 0.92869),  # 일반 (92.869%)
    (1, 0.06633),  # 고급 (6.633%)
    (2, 0.00474),  # 희귀 (0.474%)
    (3, 0.00024),  # 영웅 (0.024%)
    # 전설(4) = 합성 전용, 뽑기 불가
]

CHAR_COUNT = 20
GRADE_COUNT = 5  # 일반=0, 고급=1, 희귀=2, 영웅=3, 전설=4

GRADE_NAMES = {0: "일반", 1: "고급", 2: "희귀", 3: "영웅", 4: "전설"}

# ── 합성 설정 ──
SYNTHESIS_SUCCESS_RATE = [0.18, 0.18, 0.11, 0.11]  # 일반→고급, 고급→희귀, 희귀→영웅, 영웅→전설
PITY_THRESHOLD = 20

# ── 강화 확률 ──
ENHANCE_SAFE_MAX = 6      # +0~+6: 100% (안전 강화)
ENHANCE_RATE = 0.30       # +7 이상: 30%
ENHANCE_DESTROY_MIN = 7   # +7 이상 실패 시 장비 파괴

# ── 확률표 해시 (변조 검증용) ──
def compute_rate_table_hash() -> str:
    """확률표의 SHA-256 해시를 계산합니다.
    이 해시가 블록체인에 기록된 값과 일치하면 확률표가 변조되지 않은 것입니다."""
    canonical = f"RATE_TABLE_VERSION={RATE_TABLE_VERSION};"
    for grade, rate in RATE_TABLE:
        canonical += f"({grade},{rate});"
    canonical += f"SYNTHESIS={','.join(str(r) for r in SYNTHESIS_SUCCESS_RATE)};"
    canonical += f"PITY={PITY_THRESHOLD};"
    canonical += f"ENHANCE_SAFE_MAX={ENHANCE_SAFE_MAX};ENHANCE_RATE={ENHANCE_RATE};ENHANCE_DESTROY_MIN={ENHANCE_DESTROY_MIN}"
    return hashlib.sha256(canonical.encode()).hexdigest()

RATE_TABLE_HASH = compute_rate_table_hash()


# ══════════════════════════════════════════
#  Commit-Reveal 핵심 함수
# ══════════════════════════════════════════

def generate_seed() -> str:
    """서버 시드 생성 (64-char hex)"""
    return secrets.token_hex(32)


def commit(seed: str) -> str:
    """시드 커밋 — SHA-256 해시만 클라이언트에 전달"""
    return hashlib.sha256(seed.encode()).hexdigest()


def reveal(seed: str, nonce: str) -> int:
    """시드+논스 → 결합 해시 → 정수 난수 (0 ~ 4,294,967,295)"""
    combined = f"{seed}:{nonce}"
    hash_hex = hashlib.sha256(combined.encode()).hexdigest()
    return int(hash_hex[:8], 16)


# ══════════════════════════════════════════
#  뽑기 매핑
# ══════════════════════════════════════════

def map_to_grade(random_value: int) -> int:
    """난수 → 등급 매핑 (RATE_TABLE 기준)"""
    normalized = (random_value % 10000) / 10000.0  # 0.0000 ~ 0.9999
    cumulative = 0.0
    for grade, rate in RATE_TABLE:
        cumulative += rate
        if normalized < cumulative:
            return grade
    return RATE_TABLE[-1][0]  # fallback


def map_to_character(random_value: int, grade: int) -> int:
    """등급 내 캐릭터 선택 (균등 분배)"""
    # 상위 비트 사용 (하위 비트는 등급 매핑에 사용)
    shifted = random_value >> 8
    return shifted % CHAR_COUNT


def pull_single(seed: str, nonce: str) -> dict:
    """1회 뽑기 실행 → {grade, char_index, random_value}"""
    rv = reveal(seed, nonce)
    grade = map_to_grade(rv)
    char_index = map_to_character(rv, grade)
    return {
        "grade": grade,
        "char_index": char_index,
        "random_value": rv,
    }


def pull_multi(seed: str, nonce: str, count: int = 11) -> list:
    """다회 뽑기 — 각 결과마다 nonce에 인덱스 추가"""
    results = []
    for i in range(count):
        sub_nonce = f"{nonce}:{i}"
        result = pull_single(seed, sub_nonce)
        result["sub_nonce"] = sub_nonce
        results.append(result)
    return results


# ══════════════════════════════════════════
#  합성 판정
# ══════════════════════════════════════════

def synthesis_roll(grade: int, pity_count: int = 0) -> bool:
    """합성 성공 여부 판정 (순수 RNG — 천장은 ClaimPity로 별도 수령)
    grade: 소스 등급 (0~3)
    pity_count: 미사용 (호환성 유지)
    returns: True=성공
    """
    if grade < 0 or grade >= len(SYNTHESIS_SUCCESS_RATE):
        return False
    # 순수 랜덤 판정 (천장 강제 성공 제거 — ClaimPity로만 천장 수령)
    rv = int(secrets.token_hex(4), 16)
    normalized = (rv % 10000) / 10000.0
    return normalized < SYNTHESIS_SUCCESS_RATE[grade]
