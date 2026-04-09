"""
Commit-Reveal 뽑기 엔진
- 서버 시드 생성 → 해시 커밋 → 클라이언트 논스 수신 → reveal → 등급/캐릭터 결정
- 합성 로직 (4→1, 실패 3소모 1반환, 천장 20회)
"""

import hashlib
import secrets

# ══════════════════════════════════════════
#  확률표
# ══════════════════════════════════════════

RATE_TABLE_VERSION = "v1.0"

# (등급, 확률) — 합산 1.0
RATE_TABLE = [
    (0, 0.92869),  # 일반
    (1, 0.06633),  # 고급
    (2, 0.00474),  # 희귀
    (3, 0.00024),  # 영웅
    # 전설(4) = 합성 전용, 뽑기 불가
]

CHAR_COUNT = 20
GRADE_COUNT = 5  # 일반=0 고급=1 희귀=2 영웅=3 전설=4

# ── 합성 설정 ──
SYNTHESIS_SUCCESS_RATE = [0.18, 0.18, 0.11, 0.11]  # 일반→고급, 고급→희귀, 희귀→영웅, 영웅→전설
PITY_THRESHOLD = 20

# ── 강화 확률 ──
ENHANCE_SAFE_MAX = 6      # +0~+6: 100% (안전 강화)
ENHANCE_RATE = 0.30       # +7 이상: 30%
ENHANCE_DESTROY_MIN = 7   # +7 이상 실패 시 장비 파괴

# ── 등급 이름 ──
GRADE_NAMES = {0: "일반", 1: "고급", 2: "희귀", 3: "영웅", 4: "전설"}

# ── 확률표 해시 (변조 검증용) ──
def compute_rate_table_hash() -> str:
    """확률표의 SHA-256 해시를 계산 — 블록체인 앵커링 + GitHub 공개용"""
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


def reveal_float(seed: str, nonce: str) -> float:
    """reveal → [0.0, 1.0) 정규화 float (강화/합성/상자 확률 판정용)"""
    rv = reveal(seed, nonce)
    return (rv % 1000000) / 1000000.0


def reveal_randint(seed: str, nonce: str, a: int, b: int) -> int:
    """reveal → [a, b] 범위 정수 (상자 보상 수량 등)"""
    rv = reveal(seed, nonce)
    return a + (rv % (b - a + 1))


# ══════════════════════════════════════════
#  뽑기 매핑
# ══════════════════════════════════════════

def map_to_grade(random_value: int) -> int:
    """난수 → 등급 매핑 (RATE_TABLE 기준)"""
    normalized = (random_value % 1000000) / 1000000.0  # 0.000000 ~ 0.999999
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
    normalized = (rv % 1000000) / 1000000.0
    return normalized < SYNTHESIS_SUCCESS_RATE[grade]
