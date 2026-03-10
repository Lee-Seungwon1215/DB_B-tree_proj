# =============================================================================
# benchmark/single_insert.py - 단건 INSERT 성능 측정
# =============================================================================
# 레코드를 1건씩 commit하며 단건 삽입 latency를 측정합니다.
# 배치 INSERT와 달리 각 레코드가 독립적인 트랜잭션으로 처리됩니다.
#
# 측정 항목:
#   - 단건 삽입 평균 지연시간 (ms, commit 포함)
#   - 삽입 처리량 (RPS)
# =============================================================================

import time
import hashlib
from datetime import datetime, timezone
from config import SINGLE_INSERT_COUNT

# 기존 1M 레코드와 ID 충돌 방지용 오프셋 (스케일 최대 1M 대비 충분히 큼)
_ID_OFFSET = 10_000_000_000


def run(conn, strategy_module, algorithm_name: str, sig_pool: list) -> dict:
    """
    단건 INSERT 벤치마크를 실행합니다.

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 모듈
        algorithm_name  : 알고리즘 이름
        sig_pool        : 사전 생성된 서명 풀 [(public_key, signature, msg_hash), ...]

    Returns:
        측정 결과 dict
    """
    count   = min(SINGLE_INSERT_COUNT, len(sig_pool))
    pool_sz = len(sig_pool)
    latencies = []

    print(f"  [SINGLE INSERT] {count}건 단건 삽입 시작...")

    for i in range(count):
        pk, sig, _ = sig_pool[i % pool_sz]
        record = {
            "id":           _ID_OFFSET + i,
            "created_at":   datetime.now(timezone.utc),
            "signer_id":    i,
            "message_hash": hashlib.sha256(f"si_{i}".encode()).hexdigest(),
            "algorithm":    algorithm_name,
            "public_key":   pk,
            "signature":    sig,
        }

        start = time.perf_counter()
        strategy_module.insert_record(conn, record)
        end   = time.perf_counter()

        latencies.append((end - start) * 1000)

    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
    throughput_rps = 1000 / avg_latency_ms if avg_latency_ms > 0 else 0

    print(f"  [SINGLE INSERT] 완료: 평균 {avg_latency_ms:.3f}ms")

    # 측정용으로 삽입한 레코드 정리 (이후 범위INS를 빈 테이블에서 시작하기 위해)
    cleanup_ids = [_ID_OFFSET + i for i in range(count)]
    strategy_module.range_delete_records(conn, cleanup_ids)
    print(f"  [SINGLE INSERT] 임시 레코드 {count}건 정리 완료")

    return {
        "operation":      "SINGLE_INSERT",
        "insert_count":   count,
        "avg_latency_ms": round(avg_latency_ms, 4),
        "min_latency_ms": round(min(latencies), 4) if latencies else 0,
        "max_latency_ms": round(max(latencies), 4) if latencies else 0,
        "throughput_rps": round(throughput_rps, 2),
    }
