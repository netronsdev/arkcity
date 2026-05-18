"""
ArkForce probability and cost engine for public transparency.
Uses gacha_engine commit-reveal helpers for deterministic verification.

Module load asserts the stage table invariants from the design spec.
"""
import hashlib
from typing import Dict, List, Tuple

try:
    from game_core import gacha_engine as ge
except ModuleNotFoundError:  # Allows root-level `import server.game_core.arkforce_engine`.
    from server.game_core import gacha_engine as ge

RATE_TABLE_VERSION = "arkforce_v1"
SUCCESS_RATE_BP = 5000
MAX_STAGE = 10

# ★ 2026-05-19: % → 절대값 전환, ASPD/SPD 폐기, 5종(HP/ATK/DEF/CRI/CDMG)
# CRI/CDMG는 절대 % (CRI +1 = 치명타 확률 +1%p), HP/ATK/DEF는 정수 스탯 가산
# 장비 +99 강화 시스템 기준 — 10단계 = 끝판콘텐츠 압도적 보너스
ZERO_STAT = {
    "hp_abs": 0,
    "atk_abs": 0,
    "def_abs": 0,
    "cri_abs": 0,
    "cdmg_abs": 0,
}

# (stage, cost, protect_cost, refund, stat_dict)
STAGES: List[Tuple[int, int, int, int, Dict[str, int]]] = [
    (1, 100_000, 200_000, 200_000, {"hp_abs": 300, "atk_abs": 30, "def_abs": 10, "cri_abs": 1, "cdmg_abs": 5}),
    (2, 200_000, 600_000, 800_000, {"hp_abs": 700, "atk_abs": 70, "def_abs": 25, "cri_abs": 2, "cdmg_abs": 10}),
    (3, 800_000, 2_400_000, 3_200_000, {"hp_abs": 1_500, "atk_abs": 150, "def_abs": 50, "cri_abs": 3, "cdmg_abs": 15}),
    (4, 3_200_000, 9_600_000, 12_800_000, {"hp_abs": 3_000, "atk_abs": 300, "def_abs": 100, "cri_abs": 5, "cdmg_abs": 25}),
    (5, 12_800_000, 38_400_000, 51_200_000, {"hp_abs": 6_000, "atk_abs": 600, "def_abs": 200, "cri_abs": 7, "cdmg_abs": 40}),
    (6, 51_200_000, 153_600_000, 204_800_000, {"hp_abs": 12_000, "atk_abs": 1_200, "def_abs": 400, "cri_abs": 10, "cdmg_abs": 60}),
    (7, 204_800_000, 614_400_000, 819_200_000, {"hp_abs": 25_000, "atk_abs": 2_500, "def_abs": 800, "cri_abs": 13, "cdmg_abs": 85}),
    (8, 819_200_000, 2_457_600_000, 3_276_800_000, {"hp_abs": 50_000, "atk_abs": 5_000, "def_abs": 1_600, "cri_abs": 16, "cdmg_abs": 115}),
    (9, 3_276_800_000, 9_830_400_000, 13_107_200_000, {"hp_abs": 100_000, "atk_abs": 10_000, "def_abs": 3_200, "cri_abs": 20, "cdmg_abs": 150}),
    (10, 13_107_200_000, 39_321_600_000, 52_428_800_000, {"hp_abs": 200_000, "atk_abs": 20_000, "def_abs": 6_500, "cri_abs": 25, "cdmg_abs": 200}),
]

assert len(STAGES) == MAX_STAGE, "STAGES must match MAX_STAGE"
for _stage, _cost, _protect_cost, _refund, _stat in STAGES:
    if _stage == 1:
        assert _protect_cost == _cost * 2, "stage 1 protect cost must be 2x"
    else:
        assert _protect_cost == _cost * 3, "stage 2+ protect cost must be 3x"
for _i in range(MAX_STAGE - 1):
    assert STAGES[_i][3] == STAGES[_i + 1][1], (
        "refund of stage N must equal cost of stage N+1"
    )


def _seed_for_reveal(seed: bytes | str) -> str:
    if isinstance(seed, bytes):
        return seed.hex()
    return str(seed)


def cost_for(before_stage: int, use_protect: bool) -> int:
    stage = int(before_stage or 0)
    if stage < 0 or stage >= MAX_STAGE:
        raise ValueError("before_stage out of range")
    row = STAGES[stage]
    return row[2] if use_protect else row[1]


def refund_for(current_stage: int) -> int:
    stage = int(current_stage or 0)
    if stage <= 0:
        return 0
    if stage > MAX_STAGE:
        raise ValueError("current_stage out of range")
    return STAGES[stage - 1][3]


def stat_for(stage: int) -> dict:
    stage = int(stage or 0)
    if stage <= 0:
        return dict(ZERO_STAT)
    if stage > MAX_STAGE:
        raise ValueError("stage out of range")
    return dict(STAGES[stage - 1][4])


def roll(seed: bytes | str, nonce: str) -> tuple[bool, int]:
    roll_value = ge.reveal_randint(_seed_for_reveal(seed), str(nonce), 0, 9999)
    return roll_value < SUCCESS_RATE_BP, roll_value


def compute_hash(
    account_id: int,
    before_stage: int,
    seed: str,
    nonce: str,
    roll_value: int,
    prev_hash: str,
) -> str:
    payload = (
        f"{prev_hash or ''}{int(account_id)}{int(before_stage)}"
        f"{seed}{nonce}{int(roll_value)}"
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def config_payload() -> dict:
    return {
        "version": RATE_TABLE_VERSION,
        "success_rate_basis_points": SUCCESS_RATE_BP,
        "stages": [
            {
                "stage": stage,
                "cost": cost,
                "protect_cost": protect_cost,
                "refund": refund,
                "stat": dict(stat),
            }
            for stage, cost, protect_cost, refund, stat in STAGES
        ],
    }
