# =============================================================================
# benchmark/sig_hash_query.py - 서명 해시 조회 벤치마크 (전략 C 전용)
# =============================================================================
# sig_hash 인덱스를 실제로 사용하는 조회를 측정합니다.
# 전략 C의 sig_hash 인덱스 유지 비용이 정당한지 검증합니다.
# =============================================================================

import time
import random
from config import POINT_QUERY_COUNT


def run(conn, strategy_module, inserted_ids: list) -> dict:
    """
    sig_hash 기반 단건 조회 벤치마크를 실행합니다. (전략 C 전용)

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 C 모듈
        inserted_ids    : INSERT에서 삽입된 ID 목록

    Returns:
        측정 결과 dict
    """
    sample_ids = random.sample(inserted_ids, min(POINT_QUERY_COUNT, len(inserted_ids)))
    sig_hashes = strategy_module.get_sig_hashes(conn, sample_ids)

    if not sig_hashes:
        return {
            "avg_latency_ms": None, "min_latency_ms": None,
            "max_latency_ms": None, "throughput_qps": None,
        }

    latencies = []

    print(f"  [SIG HASH QUERY] {len(sig_hashes)}회 서명 해시 조회 시작...")

    for sig_hash in sig_hashes:
        start = time.perf_counter()
        strategy_module.sig_hash_query(conn, sig_hash)
        end = time.perf_counter()
        latencies.append((end - start) * 1000)

    avg_latency_ms = sum(latencies) / len(latencies)
    min_latency_ms = min(latencies)
    max_latency_ms = max(latencies)
    throughput_qps = 1000 / avg_latency_ms if avg_latency_ms > 0 else 0

    print(f"  [SIG HASH QUERY] 완료: 평균 {avg_latency_ms:.3f}ms")

    return {
        "avg_latency_ms": round(avg_latency_ms, 4),
        "min_latency_ms": round(min_latency_ms, 4),
        "max_latency_ms": round(max_latency_ms, 4),
        "throughput_qps": round(throughput_qps, 2),
    }
