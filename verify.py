"""
ArkCity 뽑기 검증 도구
=====================
대시보드에서 확인한 seed와 nonce를 입력하면
뽑기 결과를 독립적으로 재현하여 서버 결과와 대조할 수 있습니다.

사용법:
    python verify.py --seed <시드> --nonce <논스>
    python verify.py --seed-hash <시드해시> --seed <시드>
    python verify.py --rate-hash

예시:
    # 단일 뽑기 검증
    python verify.py --seed abc123...def --nonce user_nonce_here

    # 시드 해시 검증 (서버가 커밋한 해시와 실제 시드가 일치하는지)
    python verify.py --seed-hash e5f6...abc --seed abc123...def

    # 현재 확률표 해시 출력
    python verify.py --rate-hash
"""

import argparse
import sys
from gacha_engine import (
    commit, reveal, map_to_grade, map_to_character,
    RATE_TABLE, RATE_TABLE_VERSION, RATE_TABLE_HASH,
    GRADE_NAMES, CHAR_COUNT, SYNTHESIS_SUCCESS_RATE, PITY_THRESHOLD,
    ENHANCE_SAFE_MAX, ENHANCE_RATE, ENHANCE_DESTROY_MIN,
)


def verify_pull(seed: str, nonce: str):
    """시드+논스로 뽑기 결과를 재현합니다."""
    rv = reveal(seed, nonce)
    grade = map_to_grade(rv)
    char_index = map_to_character(rv, grade)

    print("=" * 50)
    print("  ArkCity 뽑기 결과 검증")
    print("=" * 50)
    print(f"  시드:       {seed[:32]}...")
    print(f"  논스:       {nonce}")
    print(f"  시드 해시:  {commit(seed)[:32]}...")
    print(f"  난수값:     {rv}")
    print(f"  정규화:     {(rv % 10000) / 10000.0:.4f}")
    print("-" * 50)
    print(f"  등급:       {GRADE_NAMES[grade]} (등급 {grade})")
    print(f"  캐릭터:     #{char_index}")
    print("=" * 50)
    print()
    print("대시보드의 결과와 위 값이 일치하면 → 공정한 뽑기입니다.")
    print("불일치하면 → 서버가 다른 로직을 적용했다는 증거입니다.")


def verify_seed_hash(seed_hash: str, seed: str):
    """서버가 사전에 커밋한 해시와 실제 시드가 일치하는지 검증합니다."""
    computed = commit(seed)
    match = computed == seed_hash

    print("=" * 50)
    print("  시드 해시 검증")
    print("=" * 50)
    print(f"  서버 커밋 해시: {seed_hash[:32]}...")
    print(f"  계산된 해시:    {computed[:32]}...")
    print(f"  일치 여부:      {'일치' if match else '불일치'}")
    print("=" * 50)

    if match:
        print("서버가 뽑기 전에 커밋한 시드가 실제 사용된 시드와 동일합니다.")
        print("→ 서버가 결과를 보고 시드를 바꾸지 않았음이 증명됩니다.")
    else:
        print("경고: 해시가 일치하지 않습니다!")
        print("→ 서버가 커밋한 시드와 다른 시드를 사용했을 가능성이 있습니다.")


def show_rate_info():
    """현재 확률표 정보를 출력합니다."""
    print("=" * 50)
    print("  ArkCity 확률표")
    print("=" * 50)
    print(f"  버전: {RATE_TABLE_VERSION}")
    print(f"  해시: {RATE_TABLE_HASH}")
    print()
    print("  [뽑기 확률]")
    for grade, rate in RATE_TABLE:
        print(f"    {GRADE_NAMES[grade]:6s}  {rate*100:8.3f}%")
    print(f"    {'전설':6s}  합성 전용 (뽑기 불가)")
    print()
    print("  [합성 성공률]")
    synth_names = ["일반→고급", "고급→희귀", "희귀→영웅", "영웅→전설"]
    for name, rate in zip(synth_names, SYNTHESIS_SUCCESS_RATE):
        print(f"    {name:12s}  {rate*100:5.1f}%")
    print(f"    천장:        {PITY_THRESHOLD}회 실패 시 천장 수령 가능")
    print()
    print("  [강화 확률]")
    print(f"    +0 ~ +{ENHANCE_SAFE_MAX}:   100.0% (안전 강화, 실패 없음)")
    print(f"    +{ENHANCE_DESTROY_MIN} 이상:    {ENHANCE_RATE*100:5.1f}% (실패 시 장비 파괴)")
    print("=" * 50)
    print()
    print("이 해시가 블록체인에 기록된 확률표 해시와 일치하면")
    print("→ 서버가 공개한 것과 동일한 확률표를 사용하고 있다는 증거입니다.")


def main():
    parser = argparse.ArgumentParser(description="ArkCity 뽑기 검증 도구")
    parser.add_argument("--seed", type=str, help="서버 시드 (reveal 후 공개된 값)")
    parser.add_argument("--nonce", type=str, help="클라이언트 논스")
    parser.add_argument("--seed-hash", type=str, help="서버가 커밋한 시드 해시")
    parser.add_argument("--rate-hash", action="store_true", help="현재 확률표 해시 출력")

    args = parser.parse_args()

    if args.rate_hash:
        show_rate_info()
    elif args.seed and args.nonce:
        verify_pull(args.seed, args.nonce)
    elif args.seed_hash and args.seed:
        verify_seed_hash(args.seed_hash, args.seed)
    else:
        parser.print_help()
        print()
        print("예시:")
        print("  python verify.py --rate-hash")
        print("  python verify.py --seed abc123 --nonce my_nonce")
        print("  python verify.py --seed-hash e5f6abc --seed abc123")
        sys.exit(1)


if __name__ == "__main__":
    main()
