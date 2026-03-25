"""
Merkle Tree — 6종 ledger 통합 무결성 검증 + 배치 집계 앵커링
- ether_ledger, item_ledger, gacha_transactions, synthesis_log, chest_open_log, boss_kill_log
- 트랜잭션 해시 → 바이너리 트리 → 루트 해시 → Polygon 앵커링
- 배치마다 소스별 집계 + 총 유통량 등식을 summary leaf로 포함
"""

import hashlib
import json
import logging

logger = logging.getLogger("game_server")

# 통합 Merkle 배치 대상 테이블 + SELECT 컬럼
LEDGER_TABLES = {
    "ether_ledger": "id, account_id, delta, balance_after, reason, created_at",
    "item_ledger": "id, account_id, item_uid, def_id, delta, reason, created_at",
    "gacha_transactions": "id, seed, nonce, result_grade, result_char_index",
    "synthesis_log": "id, account_id, source_grade, target_grade, success, seed, seed_hash, created_at",
    "chest_open_log": "id, account_id, chest_grade, got_rare_item, got_hero_item, got_synchro_chip, dungeon_id, stage, boss_kill_id, seed, seed_hash, created_at",
    "boss_kill_log": "id, dungeon_id, stage, boss_type, spawn_time, death_time, total_damage, participant_count, boss_max_hp, is_weekly, created_at",
    "enhance_log": "id, account_id, item_uid, before_level, after_level, success, destroyed, seed, seed_hash, created_at",
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
    """범용 ledger 데이터를 정규화된 문자열로 변환"""
    if table == "ether_ledger":
        return f"ether|{row['id']}|{row['account_id']}|{row['delta']}|{row['balance_after']}|{row['reason']}|{row['created_at']}"
    elif table == "item_ledger":
        return f"item|{row['id']}|{row['account_id']}|{row['item_uid']}|{row['def_id']}|{row['delta']}|{row['reason']}|{row['created_at']}"
    elif table == "gacha_transactions":
        return f"gacha|{row['id']}|{row.get('seed','')}|{row.get('nonce','')}|{row['result_grade']}|{row['result_char_index']}"
    elif table == "synthesis_log":
        return (f"synth|{row['id']}|{row['account_id']}|{row['source_grade']}|{row['target_grade']}"
                f"|{row['success']}|{row.get('seed','') or ''}|{row.get('seed_hash','') or ''}"
                f"|{row['created_at']}")
    elif table == "chest_open_log":
        return (f"chest|{row['id']}|{row['account_id']}|{row['chest_grade']}"
                f"|{row.get('got_rare_item',False)}|{row.get('got_hero_item',False)}"
                f"|{row.get('got_synchro_chip',False)}"
                f"|{row.get('dungeon_id',0)}|{row.get('stage',0)}|{row.get('boss_kill_id',0)}"
                f"|{row.get('seed','') or ''}|{row.get('seed_hash','') or ''}"
                f"|{row['created_at']}")
    elif table == "boss_kill_log":
        return (f"bosskill|{row['id']}|{row['dungeon_id']}|{row['stage']}"
                f"|{row['boss_type']}|{row['spawn_time']}|{row['death_time']}"
                f"|{row['total_damage']}|{row['participant_count']}"
                f"|{row['boss_max_hp']}|{row.get('is_weekly',False)}"
                f"|{row['created_at']}")
    elif table == "enhance_log":
        return (f"enhance|{row['id']}|{row['account_id']}|{row['item_uid']}"
                f"|{row['before_level']}|{row['after_level']}"
                f"|{row['success']}|{row['destroyed']}"
                f"|{row.get('seed','') or ''}|{row.get('seed_hash','') or ''}"
                f"|{row['created_at']}")
    else:
        return f"{table}|{row.get('id','')}|{row.get('account_id','')}"


def compute_batch_summary(cur, new_sources: dict) -> dict:
    """배치 내 트랜잭션의 소스별 집계 + 총 유통량 계산.
    이 summary가 Merkle leaf로 포함되어 온체인 앵커링됨.
    → 운영자 DB 삽입 시 통계적 이상치로 감지 가능."""
    summary = {
        "ether": {"generated": {}, "destroyed": {}, "total_gen": 0, "total_dest": 0},
        "items": {"created": {}, "destroyed": {}},
        "gacha": {"count": 0, "by_grade": {}},
        "synthesis": {"count": 0, "success": 0, "fail": 0},
        "chest": {"count": 0, "rare": 0, "hero": 0, "chip": 0},
        "enhance": {"count": 0, "success": 0, "fail": 0, "destroyed": 0},
        "boss_kills": {"count": 0},
        "supply": {"player_balances": 0, "marketplace_escrow": 0, "total": 0},
    }

    # ── 에테르 소스별 집계 ──
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
        type_kr = {"normal": "일반", "elite": "엘리트", "weekly": "주간"}
        by_type = {}
        total_count = 0
        for r in cur.fetchall():
            cnt = int(r["cnt"])
            total_count += cnt
            bt = type_kr.get(r["boss_type"], r["boss_type"])
            label = f"던전{r['dungeon_id']} {r['stage']}층 {bt}"
            by_type[label] = {"count": cnt, "totalDamage": int(r["dmg"]),
                              "participants": int(r["participants"])}
        summary["boss_kills"]["count"] = total_count
        summary["boss_kills"]["detail"] = by_type

    # ── 총 유통량 등식 (스냅샷) ──
    # player_balances + marketplace_escrow = total
    cur.execute("SELECT COALESCE(SUM(ether), 0) AS total FROM characters")
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
    """6종 ledger 통합 Merkle 배치 생성 + 배치 집계 summary leaf 포함.
    신규 데이터 있을 때만. Returns True if created."""
    # 최신 배치에서 워터마크(마지막 처리 ID) 추출
    cur.execute("SELECT sources, last_tx_id FROM merkle_batches ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    watermarks = {}
    if row:
        if row.get("sources"):
            src = row["sources"] if isinstance(row["sources"], dict) else json.loads(row["sources"])
            for table, range_info in src.items():
                watermarks[table] = range_info[1]  # last_id
        elif row.get("last_tx_id"):
            # 레거시 gacha-only 배치 → gacha 워터마크만 설정
            watermarks["gacha_transactions"] = row["last_tx_id"]

    # 각 테이블에서 신규 행 수집
    all_hashes = []
    new_sources = {}
    for table, columns in LEDGER_TABLES.items():
        last_id = watermarks.get(table, 0)
        cur.execute(f"SELECT {columns} FROM {table} WHERE id > %s ORDER BY id", (last_id,))
        rows = cur.fetchall()
        if rows:
            for r in rows:
                h = hash_tx(format_ledger_data(table, dict(r)))
                all_hashes.append(h)
            new_sources[table] = [rows[0]["id"], rows[-1]["id"]]

    if not all_hashes:
        return False

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
        """INSERT INTO merkle_batches (root_hash, tx_count, first_tx_id, last_tx_id, sources, summary)
           VALUES (%s, %s, %s, %s, %s, %s)""",
        (root, len(all_hashes), first_tx_id, last_tx_id,
         json.dumps(new_sources), summary_json)
    )
    conn.commit()

    table_summary = ", ".join(f"{t}:{s[1]-s[0]+1}" for t, s in new_sources.items())
    supply = summary["supply"]["total"]
    logger.info(f"[Merkle] 통합 배치 생성: {len(all_hashes)}건 ({table_summary}), 총유통량={supply:,}")
    return True
