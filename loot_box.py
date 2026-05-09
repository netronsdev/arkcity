"""
Combat Enhance Scroll Box probability engine.
Uses gacha_engine commit-reveal helpers for verifiability.

Module load asserts the weights sum to WEIGHT_TOTAL.
"""
from typing import Dict, List, Tuple

try:
    from game_core import gacha_engine as ge
except ModuleNotFoundError:  # Allows root-level `import server.game_core.loot_box`.
    from server.game_core import gacha_engine as ge

RATE_TABLE_VERSION = "box_combat_scroll_v1"

# (tier_name, reward_scrolls, weight). Order is descending by reward.
BOX_WEIGHTS: List[Tuple[str, int, int]] = [
    ("jackpot", 10000, 1_135),
    ("mega", 1000, 11_350),
    ("huge", 100, 113_500),
    ("big", 50, 227_000),
    ("med", 20, 567_500),
    ("small", 10, 1_135_000),
    ("common", 5, 5_003_657),
    ("miss", 2, 2_940_858),
]
WEIGHT_TOTAL = 10_000_000
assert sum(w for _, _, w in BOX_WEIGHTS) == WEIGHT_TOTAL, (
    "BOX_WEIGHTS must sum to WEIGHT_TOTAL"
)

# Pre-computed cumulative bands [(upper_exclusive, tier_name, reward)]
_CUMULATIVE_BANDS: List[Tuple[int, str, int]] = []
_acc = 0
for _name, _reward, _weight in BOX_WEIGHTS:
    _acc += _weight
    _CUMULATIVE_BANDS.append((_acc, _name, _reward))


def _seed_for_reveal(seed: bytes | str) -> str:
    """Normalize seed storage bytes to the string input expected by gacha_engine."""
    if isinstance(seed, bytes):
        return seed.hex()
    return seed


def roll_one(seed: bytes | str, nonce: str) -> Tuple[str, int]:
    """One pull. Returns (tier_name, reward_count)."""
    r = ge.reveal_randint(_seed_for_reveal(seed), nonce, 0, WEIGHT_TOTAL - 1)
    for upper, name, reward in _CUMULATIVE_BANDS:
        if r < upper:
            return name, reward
    # Defensive fallback (unreachable due to assert above)
    last = _CUMULATIVE_BANDS[-1]
    return last[1], last[2]


def open_batch(
    seed: bytes | str, base_nonce: str, count: int
) -> Tuple[List[Tuple[str, int]], Dict[str, int], int]:
    """Open `count` boxes. Each pull uses derived nonce f"{base_nonce}#{i}".

    Returns:
        results: list of (tier_name, reward) length=count
        summary: {tier_name: count_of_that_tier_in_batch}
        total_scrolls: sum of rewards
    """
    if count < 1:
        raise ValueError("count must be >= 1")
    results: List[Tuple[str, int]] = []
    summary: Dict[str, int] = {name: 0 for name, _, _ in BOX_WEIGHTS}
    total = 0
    for i in range(count):
        tier, reward = roll_one(seed, f"{base_nonce}#{i}")
        results.append((tier, reward))
        summary[tier] += 1
        total += reward
    return results, summary, total


def public_rate_table() -> dict:
    """Returned by GET /game/api/box/rates for transparency."""
    return {
        "version": RATE_TABLE_VERSION,
        "weight_total": WEIGHT_TOTAL,
        "tiers": [
            {
                "tier": name,
                "reward": reward,
                "weight": weight,
                "probability": weight / WEIGHT_TOTAL,
            }
            for name, reward, weight in BOX_WEIGHTS
        ],
        "expected_value": sum(
            reward * weight for _, reward, weight in BOX_WEIGHTS
        ) / WEIGHT_TOTAL,
    }
