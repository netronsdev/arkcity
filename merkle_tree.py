"""
ArkCity Merkle Tree — 거래 무결성 검증
=====================================
서버는 모든 거래 기록(에테르/아이템/뽑기/합성/상자)을 Merkle Tree로 구성하고,
Root Hash를 Polygon 블록체인에 10분마다 기록합니다.

이 파일은 서버에서 사용하는 Merkle Tree 로직의 동일 사본입니다.
대시보드에서 제공하는 Merkle Proof를 이 코드로 독립 검증할 수 있습니다.
"""

import hashlib


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


def verify_proof(tx_hash: str, proof: list, root_hash: str) -> bool:
    """Merkle Proof로 트랜잭션 검증

    Args:
        tx_hash: 검증할 트랜잭션의 해시
        proof: [{"side": "left"|"right", "hash": "..."}] 형태의 경로
        root_hash: 블록체인에 기록된 Merkle Root

    Returns:
        True면 해당 트랜잭션이 해당 배치에 포함되었음이 증명됨
    """
    current = tx_hash
    for step in proof:
        if step["side"] == "left":
            current = hashlib.sha256((step["hash"] + current).encode()).hexdigest()
        else:
            current = hashlib.sha256((current + step["hash"]).encode()).hexdigest()
    return current == root_hash


def format_tx_data(table: str, row: dict) -> str:
    """거래 데이터를 정규화된 문자열로 변환 (해시 입력용)

    서버와 동일한 포맷을 사용해야 동일한 해시가 나옵니다.
    """
    if table == "ether_ledger":
        return f"ether|{row['id']}|{row['account_id']}|{row['delta']}|{row['balance_after']}|{row['reason']}|{row['created_at']}"
    elif table == "item_ledger":
        return f"item|{row['id']}|{row['account_id']}|{row['item_uid']}|{row['def_id']}|{row['delta']}|{row['reason']}|{row['created_at']}"
    elif table == "gacha_transactions":
        return f"gacha|{row['id']}|{row.get('seed','')}|{row.get('nonce','')}|{row['result_grade']}|{row['result_char_index']}"
    elif table == "synthesis_log":
        return f"synth|{row['id']}|{row['account_id']}|{row['source_grade']}|{row['target_grade']}|{row['success']}|{row['created_at']}"
    elif table == "chest_open_log":
        return f"chest|{row['id']}|{row['account_id']}|{row['chest_grade']}|{row.get('got_rare_item',False)}|{row.get('got_hero_item',False)}|{row.get('got_synchro_chip',False)}|{row['created_at']}"
    else:
        return f"{table}|{row.get('id','')}|{row.get('account_id','')}"
