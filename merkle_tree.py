"""
Merkle Tree — 8종 ledger 통합 무결성 검증 + 배치 집계 앵커링
- ether_ledger, item_ledger, gacha_transactions, synthesis_log,
  chest_open_log, boss_kill_log, enhance_log, exp_exchange_log
- 트랜잭션 해시 → 바이너리 트리 → 루트 해시 → Polygon 앵커링
- 배치마다 소스별 집계 + 총 유통량 등식을 summary leaf로 포함
- v5: 해시 체인 검증 + 총량 감시 + 교차 검증
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta
from game_core.audit import _compute_row_hash, _ether_payload, _item_payload

logger = logging.getLogger("game_server")

# 통합 Merkle 배치 대상 테이블 + SELECT 컬럼
LEDGER_TABLES = {
    "ether_ledger": "id, account_id, delta, balance_after, reason, created_at, server_id",
    "item_ledger": "id, account_id, item_uid, def_id, delta, reason, created_at, server_id",
    "gacha_transactions": "id, seed, nonce, result_grade, result_char_index, server_id",
    "synthesis_log": "id, account_id, source_grade, target_grade, success, seed, seed_hash, created_at, server_id",
    "chest_open_log": "id, account_id, chest_grade, got_rare_item, got_hero_item, got_synchro_chip, dungeon_id, stage, boss_kill_id, seed, seed_hash, created_at, server_id",
    "boss_kill_log": "id, dungeon_id, stage, boss_type, spawn_time, death_time, total_damage, participant_count, boss_max_hp, is_weekly, created_at, server_id",
    "enhance_log": "id, account_id, item_uid, before_level, after_level, success, destroyed, seed, seed_hash, created_at, server_id",
    "exp_exchange_log": "id, account_id, server_id, direction, input_amount, output_amount, fee_amount, before_level, after_level, before_exp, after_exp, before_ether, after_ether, created_at, seed, nonce, seed_hash, roll, multiplier, rate_table_version",
}


def hash_tx(tx_data: str) -> str:
    """트랜잭션 데이터 → SHA-256 해시"""
    return hashlib.sha256(tx_data.encode()).hexdigest()


def build_merkle_root(tx_hashes: list) -> str:
    """해시 리스트 → Merkle Root 계산"""
    if not tx_hashes:
        return ""
    layer = tx_hashes[:]
    while len(layer) > 1:
        next_layer = []
        for i in range(0, len(layer), 2):
            left = layer[i]
            right = layer[i + 1] if i + 1 < len(layer) else left
            combined = hashlib.sha256((left + right).encode()).hexdigest()
            next_layer.append(combined)
        layer = next_layer
    return layer[0]


def format_ledger_data(table: str, row: dict) -> str:
    """범용 ledger 데이터를 정규화된 문자열로 변환 (v2: server_id 포함)"""
    sid = row.get('server_id', 1)
    if table == "ether_ledger":
        return f"ether|{sid}|{row['id']}|{row['account_id']}|{row['delta']}|{row['balance_after']}|{row['reason']}|{row['created_at']}"
    elif table == "item_ledger":
        return f"item|{sid}|{row['id']}|{row['account_id']}|{row['item_uid']}|{row['def_id']}|{row['delta']}|{row['reason']}|{row['created_at']}"
    elif table == "gacha_transactions":
        return f"gacha|{sid}|{row['id']}|{row.get('seed','')}|{row.get('nonce','')}|{row['result_grade']}|{row['result_char_index']}"
    elif table == "synthesis_log":
        return (f"synth|{sid}|{row['id']}|{row['account_id']}|{row['source_grade']}|{row['target_grade']}"
                f"|{row['success']}|{row.get('seed','') or ''}|{row.get('seed_hash','') or ''}"
                f"|{row['created_at']}")
    elif table == "chest_open_log":
        return (f"chest|{sid}|{row['id']}|{row['account_id']}|{row['chest_grade']}"
                f"|{row.get('got_rare_item',False)}|{row.get('got_hero_item',False)}"
                f"|{row.get('got_synchro_chip',False)}"
                f"|{row.get('dungeon_id',0)}|{row.get('stage',0)}|{row.get('boss_kill_id',0)}"
                f"|{row.get('seed','') or ''}|{row.get('seed_hash','') or ''}"
                f"|{row['created_at']}")
    elif table == "boss_kill_log":
        return (f"bosskill|{sid}|{row['id']}|{row['dungeon_id']}|{row['stage']}"
                f"|{row['boss_type']}|{row['spawn_time']}|{row['death_time']}"
                f"|{row['total_damage']}|{row['participant_count']}"
                f"|{row['boss_max_hp']}|{row.get('is_weekly',False)}"
                f"|{row['created_at']}")
    elif table == "enhance_log":
        return (f"enhance|{sid}|{row['id']}|{row['account_id']}|{row['item_uid']}"
                f"|{row['before_level']}|{row['after_level']}"
                f"|{row['success']}|{row['destroyed']}"
                f"|{row.get('seed','') or ''}|{row.get('seed_hash','') or ''}"
                f"|{row['created_at']}")
    elif table == "exp_exchange_log":
        # v1 (레거시/구 클라): seed NULL → 기존 포맷 그대로 유지 (앵커 해시 불변)
        # v2 (대박 commit-reveal): seed 포함 → 신규 필드 확장
        base = (f"expex|{sid}|{row['id']}|{row['account_id']}"
                f"|{row['direction']}|{row['input_amount']}|{row['output_amount']}|{row['fee_amount']}"
                f"|{row['before_level']}|{row['after_level']}"
                f"|{row['before_exp']}|{row['after_exp']}"
                f"|{row['before_ether']}|{row['after_ether']}"
                f"|{row['created_at']}")
        if row.get('seed') is None:
            return base
        return (base
                + f"|{row.get('seed','')}|{row.get('nonce','')}|{row.get('seed_hash','')}"
                + f"|{row.get('roll','')}|{row.get('multiplier','')}|{row.get('rate_table_version','')}")
    else:
        return f"{table}|{sid}|{row.get('id','')}|{row.get('account_id','')}"


def compute_batch_summary(cur, new_sources: dict) -> dict:
    """배치 내 트랜잭션의 소스별 집계 + 총 유통량 계산.
    이 summary가 Merkle leaf로 포함되어 온체인 앵커링됨.
    → 운영자 DB 삽입 시 통계적 이상치로 감지 가능."""
    summary = {
        "ether": {"generated": {}, "destroyed": {}, "total_gen": 0, "total_dest": 0},
        "items": {"created": {}, "destroyed": {}},
        "exp_exchange": {"count": 0, "ether_to_exp": 0, "exp_to_ether": 0, "total_fee": 0},
        "gacha": {"count": 0, "by_grade": {}},
        "synthesis": {"count": 0, "success": 0, "fail": 0},
        "chest": {"count": 0, "rare": 0, "hero": 0, "chip": 0},
        "enhance": {"count": 0, "success": 0, "fail": 0, "destroyed": 0},
        "boss_kills": {"count": 0},
        "supply": {"player_balances": 0, "marketplace_escrow": 0, "total": 0},
    }

    # ── 에테르 소스별 집계 (거래소 이전분은 transfer로 분리) ──
    _TRANSFER_REASONS = ("marketplace_buy", "marketplace_sell", "marketplace_buy_cross", "marketplace_sell_cross")
    if "ether_ledger" in new_sources:
        fid, lid = new_sources["ether_ledger"]
        cur.execute("""
            SELECT reason,
                   COALESCE(SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END), 0) AS gen,
                   COALESCE(SUM(CASE WHEN delta < 0 THEN ABS(delta) ELSE 0 END), 0) AS dest
            FROM ether_ledger WHERE id BETWEEN %s AND %s
            GROUP BY reason
        """, (fid, lid))
        for r in cur.fetchall():
            g, d = int(r["gen"]), int(r["dest"])
            if r["reason"] in _TRANSFER_REASONS:
                # 거래소 이전분은 생성/소멸에 포함하지 않음
                if g > 0:
                    summary["ether"].setdefault("transfer", {})[r["reason"]] = g
                if d > 0:
                    summary["ether"].setdefault("transfer", {})[r["reason"] + "_out"] = d
                continue
            if g > 0:
                summary["ether"]["generated"][r["reason"]] = g
            if d > 0:
                summary["ether"]["destroyed"][r["reason"]] = d
            summary["ether"]["total_gen"] += g
            summary["ether"]["total_dest"] += d

    # ── 아이템 소스별 집계 ──
    if "item_ledger" in new_sources:
        fid, lid = new_sources["item_ledger"]
        cur.execute("""
            SELECT reason,
                   COALESCE(SUM(CASE WHEN delta > 0 THEN 1 ELSE 0 END), 0) AS created,
                   COALESCE(SUM(CASE WHEN delta < 0 THEN 1 ELSE 0 END), 0) AS destroyed
            FROM item_ledger WHERE id BETWEEN %s AND %s
            GROUP BY reason
        """, (fid, lid))
        for r in cur.fetchall():
            c, d = int(r["created"]), int(r["destroyed"])
            if c > 0:
                summary["items"]["created"][r["reason"]] = c
            if d > 0:
                summary["items"]["destroyed"][r["reason"]] = d

    # ── 뽑기 집계 ──
    if "gacha_transactions" in new_sources:
        fid, lid = new_sources["gacha_transactions"]
        cur.execute("""
            SELECT result_grade, COUNT(*) AS cnt
            FROM gacha_transactions WHERE id BETWEEN %s AND %s
            GROUP BY result_grade
        """, (fid, lid))
        for r in cur.fetchall():
            grade_name = {0: "일반", 1: "고급", 2: "희귀", 3: "영웅", 4: "전설"}.get(
                r["result_grade"], str(r["result_grade"]))
            summary["gacha"]["by_grade"][grade_name] = int(r["cnt"])
            summary["gacha"]["count"] += int(r["cnt"])

    # ── 합성 집계 ──
    if "synthesis_log" in new_sources:
        fid, lid = new_sources["synthesis_log"]
        cur.execute("""
            SELECT success, COUNT(*) AS cnt
            FROM synthesis_log WHERE id BETWEEN %s AND %s
            GROUP BY success
        """, (fid, lid))
        for r in cur.fetchall():
            cnt = int(r["cnt"])
            summary["synthesis"]["count"] += cnt
            if r["success"]:
                summary["synthesis"]["success"] += cnt
            else:
                summary["synthesis"]["fail"] += cnt

    # ── 상자 오픈 집계 ──
    if "chest_open_log" in new_sources:
        fid, lid = new_sources["chest_open_log"]
        cur.execute("""
            SELECT COUNT(*) AS cnt,
                   SUM(CASE WHEN got_rare_item THEN 1 ELSE 0 END) AS rare,
                   SUM(CASE WHEN got_hero_item THEN 1 ELSE 0 END) AS hero,
                   SUM(CASE WHEN got_synchro_chip THEN 1 ELSE 0 END) AS chip
            FROM chest_open_log WHERE id BETWEEN %s AND %s
        """, (fid, lid))
        r = cur.fetchone()
        if r:
            summary["chest"]["count"] = int(r["cnt"] or 0)
            summary["chest"]["rare"] = int(r["rare"] or 0)
            summary["chest"]["hero"] = int(r["hero"] or 0)
            summary["chest"]["chip"] = int(r["chip"] or 0)

    # ── 강화 집계 ──
    if "enhance_log" in new_sources:
        fid, lid = new_sources["enhance_log"]
        cur.execute("""
            SELECT COUNT(*) AS cnt,
                   SUM(CASE WHEN success THEN 1 ELSE 0 END) AS s,
                   SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS f,
                   SUM(CASE WHEN destroyed THEN 1 ELSE 0 END) AS d
            FROM enhance_log WHERE id BETWEEN %s AND %s
        """, (fid, lid))
        r = cur.fetchone()
        if r:
            summary["enhance"]["count"] = int(r["cnt"] or 0)
            summary["enhance"]["success"] = int(r["s"] or 0)
            summary["enhance"]["fail"] = int(r["f"] or 0)
            summary["enhance"]["destroyed"] = int(r["d"] or 0)

    # ── 보스 킬 집계 ──
    if "boss_kill_log" in new_sources:
        fid, lid = new_sources["boss_kill_log"]
        cur.execute("""
            SELECT boss_type, dungeon_id, stage, COUNT(*) AS cnt,
                   COALESCE(SUM(total_damage), 0) AS dmg,
                   COALESCE(SUM(participant_count), 0) AS participants
            FROM boss_kill_log WHERE id BETWEEN %s AND %s
            GROUP BY boss_type, dungeon_id, stage
        """, (fid, lid))
        boss_name_kr = {
            "Reina": "레이나", "Iris": "아이리스", "Luna": "루나", "Noah": "노아",
            "Seraphine": "세라핀", "Kaia": "카이아", "Yuki": "유키", "Mei": "메이",
            "Vika": "비카", "Nyx": "닉스", "Freya": "프레야", "Zephyra": "제피라",
            "Sable": "세이블", "Astrid": "아스트리드", "Lilith": "릴리스",
            "Hana": "하나", "Roxy": "록시", "Aria": "아리아", "Sola": "솔라", "Echo": "에코",
            "AbyssEndBoss": "심연의 지배자",
            "normal": "일반", "elite": "엘리트", "weekly": "주간",
        }
        by_type = {}
        total_count = 0
        for r in cur.fetchall():
            cnt = int(r["cnt"])
            total_count += cnt
            bt = boss_name_kr.get(r["boss_type"], r["boss_type"])
            label = f"던전{r['dungeon_id']} {r['stage']}층 {bt}"
            by_type[label] = {"count": cnt, "totalDamage": int(r["dmg"]),
                              "participants": int(r["participants"])}
        summary["boss_kills"]["count"] = total_count
        summary["boss_kills"]["detail"] = by_type

    # ── 경험치 교환 집계 ──
    if "exp_exchange_log" in new_sources:
        fid, lid = new_sources["exp_exchange_log"]
        cur.execute("""
            SELECT direction, COUNT(*) AS cnt, COALESCE(SUM(fee_amount), 0) AS fee
            FROM exp_exchange_log WHERE id BETWEEN %s AND %s
            GROUP BY direction
        """, (fid, lid))
        for r in cur.fetchall():
            cnt = int(r["cnt"])
            summary["exp_exchange"]["count"] += cnt
            if r["direction"] == "ether_to_exp":
                summary["exp_exchange"]["ether_to_exp"] += cnt
            elif r["direction"] == "exp_to_ether":
                summary["exp_exchange"]["exp_to_ether"] += cnt
            summary["exp_exchange"]["total_fee"] += int(r["fee"])

    # ── 총 유통량 등식 (스냅샷) ──
    # player_balances + marketplace_escrow = total
    cur.execute("SELECT COALESCE(SUM(ether), 0) AS total FROM characters_econ")
    player_bal = int(cur.fetchone()["total"])

    cur.execute("""
        SELECT COALESCE(SUM(seller_received), 0) AS escrow
        FROM marketplace_transactions
        WHERE seller_ether_claimed = FALSE
    """)
    escrow = int(cur.fetchone()["escrow"])

    summary["supply"]["player_balances"] = player_bal
    summary["supply"]["marketplace_escrow"] = escrow
    summary["supply"]["total"] = player_bal + escrow

    return summary


def build_unified_batch(cur, conn) -> bool:
    """8종 ledger 통합 Merkle 배치 생성 + 배치 집계 summary leaf 포함.
    신규 데이터 있을 때만. Returns True if created."""
    # 최근 배치들에서 테이블별 워터마크(마지막 처리 ID) 추출.
    # ★ 버그 수정(2026-04-12): 과거에는 최신 1개 배치만 읽어서 그 배치가 건드리지 않은
    #   테이블의 워터마크가 0으로 떨어졌음 → 다음 배치에서 해당 테이블 전체 재스캔 발생.
    #   → cross_validate 오탐(ORPHAN_KILLS/RATE_EXCEEDED/NO_SESSION), merkle leaf 중복 포함,
    #      대량 CPU/DB 낭비를 유발. 최근 N개 배치 스캔 후 테이블별 max(last_id)로 복원.
    cur.execute(
        "SELECT sources, last_tx_id FROM merkle_batches ORDER BY id DESC LIMIT 500"
    )
    rows = cur.fetchall()
    watermarks = {}
    for row in rows:
        if row.get("sources"):
            src = row["sources"] if isinstance(row["sources"], dict) else json.loads(row["sources"])
            for table, range_info in src.items():
                last = range_info[1]
                if last > watermarks.get(table, 0):
                    watermarks[table] = last
        elif row.get("last_tx_id"):
            # 레거시 gacha-only 배치 → gacha 워터마크만 설정
            if row["last_tx_id"] > watermarks.get("gacha_transactions", 0):
                watermarks["gacha_transactions"] = row["last_tx_id"]

    # 각 테이블에서 신규 행 수집 + 서버별 해시 추적
    all_hashes = []
    new_sources = {}
    server_hash_map = {}  # {server_id: [hash, ...]} — 서버별 서브루트용
    for table, columns in LEDGER_TABLES.items():
        last_id = watermarks.get(table, 0)
        cur.execute(f"SELECT {columns} FROM {table} WHERE id > %s ORDER BY id", (last_id,))
        rows = cur.fetchall()
        if rows:
            for r in rows:
                rd = dict(r)
                sid = rd.get("server_id", 1)
                h = hash_tx(format_ledger_data(table, rd))
                all_hashes.append(h)
                server_hash_map.setdefault(sid, []).append(h)
            new_sources[table] = [rows[0]["id"], rows[-1]["id"]]

    if not all_hashes:
        return False

    # ── 서버별 서브루트 계산 ──
    sub_roots = {}
    for sid in sorted(server_hash_map.keys()):
        sub_roots[str(sid)] = build_merkle_root(server_hash_map[sid])

    # ── 배치 집계 계산 + summary leaf 추가 ──
    summary = compute_batch_summary(cur, new_sources)
    summary_json = json.dumps(summary, sort_keys=True, separators=(',', ':'))
    summary_hash = hash_tx(f"summary|{summary_json}")
    all_hashes.append(summary_hash)  # 마지막 leaf (기존 인덱스 불변 = 하위호환)

    root = build_merkle_root(all_hashes)

    # 하위 호환: gacha 범위가 있으면 first/last_tx_id에도 기록
    gacha_range = new_sources.get("gacha_transactions")
    first_tx_id = gacha_range[0] if gacha_range else None
    last_tx_id = gacha_range[1] if gacha_range else None

    cur.execute(
        """INSERT INTO merkle_batches (root_hash, tx_count, first_tx_id, last_tx_id, sources, summary, sub_roots)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (root, len(all_hashes), first_tx_id, last_tx_id,
         json.dumps(new_sources), summary_json, json.dumps(sub_roots))
    )
    conn.commit()

    table_summary = ", ".join(f"{t}:{s[1]-s[0]+1}" for t, s in new_sources.items())
    supply = summary["supply"]["total"]
    sr_info = ", ".join(f"s{k}={v[:8]}..." for k, v in sub_roots.items())
    logger.info(f"[Merkle] 통합 배치 생성: {len(all_hashes)}건 ({table_summary}), 총유통량={supply:,}, 서브루트=[{sr_info}]")
    return True


# ═══════════════════════════════════════════
#  v5 — 3대 보안 검증 (async, game_server.py에서 호출)
# ═══════════════════════════════════════════

async def verify_hash_chain(conn, table: str, first_id: int, last_id: int) -> dict:
    """배치 범위의 오프체인 해시 체인 무결성 검증.
    ★ 해시 체인은 server_id별 독립 체인이므로, 서버별로 분리 검증한다.
    row_hash가 NULL인 행(마이그레이션 과도기)은 스킵."""
    from game_core.audit import _ether_payload, _item_payload

    if first_id <= 0 or last_id <= 0:
        return {"passed": True, "checked": 0, "skipped": 0, "broken": []}

    if table == "ether_ledger":
        cols = "id, server_id, account_id, delta, balance_after, reason, reference_id, row_hash"
    else:
        cols = "id, server_id, account_id, item_uid, def_id, delta, quantity, enhance_level, reason, reference_id, counterpart_id, row_hash"

    rows = await conn.fetch(
        f"SELECT {cols} FROM {table} WHERE id BETWEEN $1 AND $2 ORDER BY id",
        first_id, last_id
    )
    if not rows:
        return {"passed": True, "checked": 0, "skipped": 0, "broken": []}

    # server_id별로 그룹화
    by_server = {}
    for row in rows:
        sid = row["server_id"] if row["server_id"] is not None else 0
        by_server.setdefault(sid, []).append(row)

    broken = []
    skipped = 0
    checked = 0

    for sid, server_rows in by_server.items():
        # 이 서버의 배치 시작 직전 행의 hash (같은 server_id 체인에서)
        first_row_id = server_rows[0]["id"]
        prev = await conn.fetchval(
            f"SELECT row_hash FROM {table} WHERE server_id = $1 AND id < $2 ORDER BY id DESC LIMIT 1",
            sid, first_row_id
        )
        prev_hash = prev or "GENESIS"

        for row in server_rows:
            if not row["row_hash"]:
                skipped += 1
                # ★ recording(audit.py)과 동일하게 GENESIS로 리셋
                # record_ether: SELECT 직전행 → NULL이면 prev_hash="GENESIS"
                prev_hash = "GENESIS"
                continue
            checked += 1
            # ★ 500행마다 이벤트루프 양보 — 대량 해시 검증 시 블로킹 방지
            if checked % 500 == 0:
                await asyncio.sleep(0)
            if table == "ether_ledger":
                payload = _ether_payload(
                    row["account_id"], row["delta"], row["balance_after"],
                    row["reason"], row.get("reference_id"))
            else:
                payload = _item_payload(
                    row["account_id"], row["item_uid"], row["def_id"],
                    row["delta"], row["quantity"], row["enhance_level"],
                    row["reason"], row.get("reference_id"), row.get("counterpart_id"))
            expected = _compute_row_hash(payload, prev_hash)
            if expected != row["row_hash"]:
                broken.append({"id": row["id"], "server": sid, "expected": expected[:16], "actual": row["row_hash"][:16]})
                if len(broken) >= 10:
                    break
            prev_hash = row["row_hash"]

    return {
        "passed": len(broken) == 0,
        "checked": checked,
        "skipped": skipped,
        "broken": broken,
    }


async def capture_supply_snapshot(conn, batch_id: int, first_id: int, last_id: int,
                                  ws_manager) -> dict:
    """총량 스냅샷 + 이상 탐지 규칙 → supply_snapshots 저장.
    ★ 서버별로 분리 저장 (server_id 컬럼). AUTH 서버에서만 호출됨."""
    _TRANSFER_REASONS = {"marketplace_buy", "marketplace_sell", "marketplace_buy_cross", "marketplace_sell_cross"}
    _ADMIN_REASONS = {"test_cheat", "admin_grant"}
    _REASON_KR = {
        "dungeon_kill": "몬스터 처치", "dungeon_kill_preflush": "몬스터 처치",
        "boss_kill": "보스 처치", "offline_earn": "오토파일럿",
        "battlepass_reward": "배틀패스", "dungeon_owner_payout": "던전 주인 보상",
        "mailbox_claim": "우편함", "enhance_cost": "장비 강화", "enhance_cost_safe": "안전 강화",
        "enhance_cost_confirmed": "확정 강화", "enhance_cost_destroy_protect": "파괴방지 강화",
        "enhance_success": "강화 성공",
        "enhance_success_confirmed": "확정 강화 성공", "enhance_success_destroy_protect": "파괴방지 강화 성공",
        "gacha_cost": "뽑기 비용", "exchange_cost": "거래소 등록비",
        "guild_create": "길드 창설", "marketplace_fee": "거래소 수수료",
        "marketplace_fee_cross": "거래소 수수료", "admin_grant": "운영자 지급",
        "test_cheat": "테스트 치트", "guild_attendance": "길드 출석",
        "offline_hunt_auto_buy": "자동 구매", "shop_buy": "상점 구매",
        "option_grant": "옵션 부여", "option_recalibrate": "옵션 재조정",
        "quality_upgrade": "품질 승급", "sell_item": "아이템 판매",
        "sell_by_grade": "등급별 일괄 판매", "open_chest": "상자 개봉",
        "pvp_track": "PVP 위치추적",
        "exp_exchange_buy": "경험치 구매", "exp_exchange_sell": "경험치 판매",
        "quest_reward": "퀘스트 보상", "reward_recovery": "보상 복구",
        "던전주인 해금": "던전주인 해금",
    }

    # 서버 목록 조회
    server_ids = [r["id"] for r in await conn.fetch("SELECT id FROM servers ORDER BY id")]
    if not server_ids:
        server_ids = [1]

    all_alerts = []

    # 테스트 서버 목록 (RATE_SPIKE, SUPPLY_SURGE 스킵)
    test_sids = {r["id"] for r in await conn.fetch("SELECT id FROM servers WHERE is_test = TRUE")}

    for sid in server_ids:
        # 1) 이 구간 reason별 생성/소멸 (서버별, 거래소 이전분 제외)
        gen_by = {}
        dest_by = {}
        total_gen = 0
        total_dest = 0
        if first_id > 0 and last_id > 0:
            stats = await conn.fetch("""
                SELECT reason,
                       COALESCE(SUM(CASE WHEN delta > 0 THEN delta ELSE 0 END), 0) AS gen,
                       COALESCE(SUM(CASE WHEN delta < 0 THEN ABS(delta) ELSE 0 END), 0) AS dest
                FROM ether_ledger WHERE id BETWEEN $1 AND $2 AND server_id = $3
                GROUP BY reason
            """, first_id, last_id, sid)
            for r in stats:
                if r["reason"] in _TRANSFER_REASONS:
                    continue
                g, d = int(r["gen"]), int(r["dest"])
                reason_kr = _REASON_KR.get(r["reason"], r["reason"])
                if g > 0:
                    gen_by[reason_kr] = g
                if d > 0:
                    dest_by[reason_kr] = d
                # 치트/관리자 에테르는 기록하되 이상감지 계산에서 제외
                if r["reason"] not in _ADMIN_REASONS:
                    total_gen += g
                    total_dest += d

        # 2) 현재 총 공급량 (서버별)
        player_bal = int(await conn.fetchval(
            "SELECT COALESCE(SUM(ether), 0) FROM characters_econ WHERE server_id = $1", sid))
        escrow = int(await conn.fetchval("""
            SELECT COALESCE(SUM(mt.seller_received), 0)
            FROM marketplace_transactions mt
            JOIN characters_econ c ON c.account_id = mt.seller_account_id AND c.server_id = $1
            WHERE mt.seller_ether_claimed = FALSE
        """, sid))
        total_supply = player_bal + escrow

        # 3) 활성 유저 — DB 기반 (서버별)
        online = int(await conn.fetchval("""
            SELECT COUNT(DISTINCT s.account_id) FROM sessions s
            JOIN characters_econ c ON c.account_id = s.account_id AND c.server_id = $1
            WHERE s.last_active_at >= NOW() - INTERVAL '2 minutes'
        """, sid))
        autopilot = int(await conn.fetchval("""
            SELECT COUNT(DISTINCT account_id) FROM offline_hunt_sessions
            WHERE ended_at IS NULL AND server_id = $1
        """, sid))
        # 온라인+오토파일럿 중복 제거
        active = int(await conn.fetchval("""
            SELECT COUNT(DISTINCT aid) FROM (
                SELECT s.account_id AS aid FROM sessions s
                JOIN characters_econ c ON c.account_id = s.account_id AND c.server_id = $1
                WHERE s.last_active_at >= NOW() - INTERVAL '2 minutes'
                UNION
                SELECT account_id AS aid FROM offline_hunt_sessions
                WHERE ended_at IS NULL AND server_id = $1
            ) t
        """, sid))
        epp = total_gen // max(active, 1) if total_gen > 0 else 0

        # 4) 이상 탐지
        alerts = []

        # A: 접속자 0명인데 dungeon_kill 에테르 발생
        dk = gen_by.get("몬스터 처치", 0)
        if active == 0 and dk > 0:
            alerts.append({"type": "GHOST_FARMING",
                           "msg": f"접속자 0명(온라인 {online}+오토 {autopilot}), 몬스터 처치 +{dk:,}"})

        # B: 24시간 평균 대비 유저당 3배 초과 (테스트 서버 제외)
        if active > 0 and sid not in test_sids:
            avg_24h = await conn.fetchval("""
                SELECT AVG(ether_per_player) FROM supply_snapshots
                WHERE created_at > NOW() - INTERVAL '24 hours'
                  AND active_players > 0 AND server_id = $1
            """, sid)
            if avg_24h and avg_24h > 0 and epp > avg_24h * 3:
                alerts.append({"type": "RATE_SPIKE",
                               "msg": f"유저당 {epp:,} (24h 평균 {int(avg_24h):,}의 {epp/avg_24h:.1f}배)"})

        # C: 단일 계정 독점 — 소수 테스터 환경에서 항상 오탐이므로 비활성화
        # if active >= 2 and total_gen > 0 and first_id > 0:
        #     ...WHALE_ANOMALY...

        # D: 총 공급량 이전 대비 5% 급증 (테스트 서버 제외)
        if sid not in test_sids:
            admin_gen = sum(gen_by.get(r, 0) for r in _ADMIN_REASONS)
            prev_supply = await conn.fetchval(
                "SELECT total_supply FROM supply_snapshots WHERE server_id = $1 ORDER BY id DESC LIMIT 1",
                sid)
            if prev_supply and prev_supply > 0:
                adjusted_supply = total_supply - admin_gen
                growth = (adjusted_supply - prev_supply) / prev_supply
                if growth > 0.05:
                    alerts.append({"type": "SUPPLY_SURGE",
                                   "msg": f"총량 {prev_supply:,} → {total_supply:,} (+{growth*100:.1f}%)"})

        # 5) 크로스서버 유출/유입 계산
        # outflow: 구매자 에테르 원장에서 즉시 기록됨 (buy_cross + fee_cross)
        # inflow: marketplace_transactions 기준 (미수령이어도 거래 시점에 유입 계산)
        cross_out = 0
        cross_in = 0
        if first_id > 0 and last_id > 0:
            cross_out = int(await conn.fetchval("""
                SELECT COALESCE(SUM(ABS(delta)), 0)
                FROM ether_ledger
                WHERE id BETWEEN $1 AND $2 AND server_id = $3
                  AND reason IN ('marketplace_buy_cross', 'marketplace_fee_cross')
            """, first_id, last_id, sid))
            # inflow: 이 배치 구간의 시간 범위 내 크로스서버 판매 거래 (수령 여부 무관)
            batch_start_time = await conn.fetchval(
                "SELECT created_at FROM ether_ledger WHERE id = $1", first_id)
            batch_end_time = await conn.fetchval(
                "SELECT created_at FROM ether_ledger WHERE id = $1", last_id)
            if batch_start_time and batch_end_time:
                cross_in = int(await conn.fetchval("""
                    SELECT COALESCE(SUM(seller_received), 0)
                    FROM marketplace_transactions
                    WHERE is_cross_server = TRUE AND seller_server_id = $1
                      AND transacted_at >= $2 AND transacted_at <= $3
                """, sid, batch_start_time, batch_end_time))

        # 6) 저장 (서버별)
        await conn.execute("""
            INSERT INTO supply_snapshots
            (batch_id, total_supply, player_balances, marketplace_escrow,
             generated, destroyed, active_players, online_players, autopilot_players,
             ether_per_player, gen_by_reason, dest_by_reason, alerts, server_id,
             cross_server_outflow, cross_server_inflow)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
        """, batch_id, total_supply, player_bal, escrow,
             total_gen, total_dest, active, online, autopilot,
             epp, json.dumps(gen_by), json.dumps(dest_by), json.dumps(alerts), sid,
             cross_out, cross_in)

        if alerts:
            logger.warning(f"[SUPPLY ALERT] batch={batch_id} server={sid}: {alerts}")
            all_alerts.extend(alerts)

    # 글로벌 합산 (전체 서버 — 호환용)
    g_supply = int(await conn.fetchval("SELECT COALESCE(SUM(ether), 0) FROM characters_econ"))
    return {"alerts": all_alerts, "total_supply": g_supply, "generated": 0}


async def cross_validate_batch(conn, batch_id: int, first_id: int, last_id: int,
                               ws_manager) -> dict:
    """교차 검증: dungeon_kill 에테르가 실제 게임 활동과 일치하는지."""
    alerts = []
    if first_id <= 0 or last_id <= 0:
        return {"passed": True, "alerts": alerts}

    # ★ 배치의 실제 시간 범위 사용 (NOW() 기준이면 재시작 후 세션 오탐 발생)
    batch_times = await conn.fetchrow("""
        SELECT MIN(created_at) AS earliest, MAX(created_at) AS latest
        FROM ether_ledger WHERE id BETWEEN $1 AND $2
    """, first_id, last_id)
    if not batch_times or not batch_times["earliest"]:
        return {"passed": True, "alerts": alerts}
    # ::timestamp 캐스트: offline_hunt_sessions 컬럼이 'timestamp without time zone'
    batch_start = batch_times["earliest"] - timedelta(minutes=2)
    batch_end = batch_times["latest"] + timedelta(minutes=1)

    # 1) dungeon_kill 에테르 받은 계정 목록
    kill_earners = await conn.fetch("""
        SELECT account_id, server_id, SUM(delta) AS total, COUNT(*) AS tx_count
        FROM ether_ledger
        WHERE id BETWEEN $1 AND $2 AND reason = 'dungeon_kill' AND delta > 0
        GROUP BY account_id, server_id
    """, first_id, last_id)

    # ★ 배치 시간 범위 기준 세션 확인 (NOW()가 아닌 배치 시점의 세션)
    online_rows = await conn.fetch("""
        SELECT DISTINCT s.account_id FROM sessions s
        WHERE s.last_active_at >= $1
    """, batch_start)
    online_set = {r["account_id"] for r in online_rows}

    # ★ 오프라인 세션 일괄 조회 (earner별 개별 쿼리 → 1회 일괄)
    offline_rows = await conn.fetch("""
        SELECT DISTINCT account_id, server_id FROM offline_hunt_sessions
        WHERE started_at <= $1
          AND (ended_at IS NULL OR ended_at >= $2)
    """, batch_end, batch_start)
    offline_set = {(r["account_id"], r["server_id"]) for r in offline_rows}

    # ★ 닉네임 일괄 조회 (earner별 개별 쿼리 → 1회 일괄)
    all_earner_aids = [int(e["account_id"]) for e in kill_earners]
    if all_earner_aids:
        nick_rows = await conn.fetch(
            "SELECT id, nickname FROM accounts WHERE id = ANY($1::int[])",
            all_earner_aids)
        nick_map = {r["id"]: r["nickname"] for r in nick_rows}
    else:
        nick_map = {}

    for earner in kill_earners:
        aid = earner["account_id"]
        earner_sid = earner["server_id"] or 1
        was_online = int(aid) in online_set

        if not was_online:
            if (int(aid), earner_sid) not in offline_set:
                nn = nick_map.get(int(aid), aid)
                alerts.append({
                    "type": "NO_SESSION",
                    "msg": f"{nn or aid}: +{int(earner['total']):,} 에테르, 접속/사냥 세션 없음"
                })

    # 2) reference_id 없는 dungeon_kill
    orphan_count = await conn.fetchval("""
        SELECT COUNT(*) FROM ether_ledger
        WHERE id BETWEEN $1 AND $2
          AND reason = 'dungeon_kill' AND reference_id IS NULL
    """, first_id, last_id)
    if orphan_count and orphan_count > 0:
        alerts.append({
            "type": "ORPHAN_KILLS",
            "msg": f"reference_id 없는 몬스터 처치 {orphan_count}건"
        })

    # 3) 10분간 이론적 최대 에테르 초과 (넉넉한 상한)
    MAX_ETHER_10MIN = 5_000_000
    for earner in kill_earners:
        if earner["total"] > MAX_ETHER_10MIN:
            nn = nick_map.get(int(earner["account_id"]), earner["account_id"])
            alerts.append({
                "type": "RATE_EXCEEDED",
                "msg": f"{nn or earner['account_id']}: {int(earner['total']):,} (한도 {MAX_ETHER_10MIN:,})"
            })

    # 검증 요약 통계
    total_ether_rows = await conn.fetchval(
        "SELECT COUNT(*) FROM ether_ledger WHERE id BETWEEN $1 AND $2",
        first_id, last_id)
    total_kill_ether = sum(int(e["total"]) for e in kill_earners)

    summary = {
        "검증 원장 행수": int(total_ether_rows or 0),
        "몬스터 처치 계정수": len(kill_earners),
        "몬스터 처치 총 에테르": total_kill_ether,
        "세션 대조 계정수": len(kill_earners),
        "세션 검증": f"{len(kill_earners)}명 전원 접속/사냥 확인" if not any(
            a["type"] == "NO_SESSION" for a in alerts) else "이상 감지",
        "경로 검증": f"orphan 0건" if not any(
            a["type"] == "ORPHAN_KILLS" for a in alerts) else "이상 감지",
        "속도 검증": f"전원 한도 이내" if not any(
            a["type"] == "RATE_EXCEEDED" for a in alerts) else "이상 감지",
        "접속자(DB)": len(online_set),
    }

    details = {"alerts": alerts, "summary": summary}

    # 결과 저장
    await conn.execute("""
        INSERT INTO integrity_checks (check_type, batch_id, passed, details)
        VALUES ('cross_validate', $1, $2, $3)
    """, batch_id, len(alerts) == 0, json.dumps(details, ensure_ascii=False))

    if alerts:
        logger.warning(f"[CROSS-VALIDATE] batch={batch_id}: {alerts}")

    return {"passed": len(alerts) == 0, "alerts": alerts, "summary": summary}
