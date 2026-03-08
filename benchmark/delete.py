# =============================================================================
# benchmark/delete.py - DELETE 성능 측정
# =============================================================================
# 랜덤하게 선택된 레코드를 삭제하며 성능을 측정합니다.
#
# 측정 항목:
#   - 단건 삭제 평균 지연시간 (ms)
#   - 삭제 처리량 (DPS, deletes per second)
# =============================================================================

import time
import random
from config import DELETE_COUNT


def run(conn, strategy_module, inserted_ids: list) -> dict:
    """
    DELETE 벤치마크를 실행합니다.

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 모듈
        inserted_ids    : INSERT에서 삽입된 ID 목록

    Returns:
        측정 결과 dict
    """
    # 삭제할 ID를 랜덤 선택 (전체의 일부만 삭제)
    delete_count = min(DELETE_COUNT, len(inserted_ids))
    delete_ids   = random.sample(inserted_ids, delete_count)

    latencies = []   # 각 삭제의 지연시간 (ms)

    print(f"  [DELETE] {delete_count}건 삭제 시작...")

    for record_id in delete_ids:
        start = time.perf_counter()
        strategy_module.delete_record(conn, record_id)
        end   = time.perf_counter()

        latency_ms = (end - start) * 1000
        latencies.append(latency_ms)

    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
    throughput_dps = 1000 / avg_latency_ms if avg_latency_ms > 0 else 0

    print(f"  [DELETE] 완료: 평균 {avg_latency_ms:.3f}ms")

    return {
        "operation":        "DELETE",
        "delete_count":     delete_count,
        "avg_latency_ms":   round(avg_latency_ms, 4),
        "min_latency_ms":   round(min(latencies), 4) if latencies else 0,
        "max_latency_ms":   round(max(latencies), 4) if latencies else 0,
        "throughput_dps":   round(throughput_dps, 2),   # deletes per second
    }
