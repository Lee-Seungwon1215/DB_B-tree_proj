# =============================================================================
# benchmark/point_query.py - Point Query (단건 조회) 성능 측정
# =============================================================================
# 랜덤하게 선택된 ID로 단건 조회를 반복하여 평균 성능을 측정합니다.
#
# 측정 항목:
#   - 단건 조회 평균 지연시간 (ms)
#   - 단건 조회 처리량 (QPS, queries per second)
# =============================================================================

import time
import random
from config import POINT_QUERY_COUNT


def run(conn, strategy_module, inserted_ids: list) -> dict:
    """
    Point Query 벤치마크를 실행합니다.

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 모듈
        inserted_ids    : INSERT에서 삽입된 ID 목록

    Returns:
        측정 결과 dict
    """
    # 전체 ID 중에서 랜덤하게 POINT_QUERY_COUNT개 선택
    query_ids = random.sample(inserted_ids, min(POINT_QUERY_COUNT, len(inserted_ids)))

    latencies = []   # 각 쿼리의 지연시간 (ms)

    print(f"  [POINT QUERY] {POINT_QUERY_COUNT}회 단건 조회 시작...")

    for record_id in query_ids:
        start = time.perf_counter()
        strategy_module.point_query(conn, record_id)
        end   = time.perf_counter()

        latency_ms = (end - start) * 1000
        latencies.append(latency_ms)

    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
    min_latency_ms = min(latencies) if latencies else 0
    max_latency_ms = max(latencies) if latencies else 0
    throughput_qps = 1000 / avg_latency_ms if avg_latency_ms > 0 else 0

    print(f"  [POINT QUERY] 완료: 평균 {avg_latency_ms:.3f}ms")

    return {
        "operation":        "POINT_QUERY",
        "query_count":      len(latencies),
        "avg_latency_ms":   round(avg_latency_ms, 4),
        "min_latency_ms":   round(min_latency_ms, 4),
        "max_latency_ms":   round(max_latency_ms, 4),
        "throughput_qps":   round(throughput_qps, 2),   # queries per second
    }
