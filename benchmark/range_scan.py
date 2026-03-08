# =============================================================================
# benchmark/range_scan.py - Range Scan (범위 조회) 성능 측정
# =============================================================================
# 생성시간 기준 범위 조회를 반복하여 성능을 측정합니다.
# 전체 레코드의 약 10% 해당하는 시간 구간을 랜덤하게 선택합니다.
#
# 측정 항목:
#   - 범위 조회 평균 지연시간 (ms)
#   - 평균 조회 결과 수 (몇 건이 반환되었는지)
#   - 처리량 (QPS)
# =============================================================================

import time
from config import RANGE_SCAN_COUNT
from data.generator_phase1 import get_time_range


def run(conn, strategy_module, algorithm_name: str, n: int) -> dict:
    """
    Range Scan 벤치마크를 실행합니다.

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 모듈
        algorithm_name  : 알고리즘 이름 (시간 범위 계산에 사용)
        n               : 총 레코드 수

    Returns:
        측정 결과 dict
    """
    latencies    = []   # 각 범위 조회의 지연시간 (ms)
    result_counts = []  # 각 범위 조회에서 반환된 레코드 수

    print(f"  [RANGE SCAN] {RANGE_SCAN_COUNT}회 범위 조회 시작...")

    for i in range(RANGE_SCAN_COUNT):
        # 매 조회마다 다른 랜덤 시간 범위 선택 (전체의 10% 구간)
        start_time, end_time = get_time_range(algorithm_name, n)

        start = time.perf_counter()
        results = strategy_module.range_scan(conn, start_time, end_time)
        end   = time.perf_counter()

        latency_ms = (end - start) * 1000
        latencies.append(latency_ms)
        result_counts.append(len(results) if results else 0)

    avg_latency_ms  = sum(latencies) / len(latencies)     if latencies else 0
    avg_result_count = sum(result_counts) / len(result_counts) if result_counts else 0
    throughput_qps  = 1000 / avg_latency_ms if avg_latency_ms > 0 else 0

    print(f"  [RANGE SCAN] 완료: 평균 {avg_latency_ms:.3f}ms, 평균 결과 {avg_result_count:.0f}건")

    return {
        "operation":         "RANGE_SCAN",
        "query_count":       len(latencies),
        "avg_latency_ms":    round(avg_latency_ms, 4),
        "min_latency_ms":    round(min(latencies), 4) if latencies else 0,
        "max_latency_ms":    round(max(latencies), 4) if latencies else 0,
        "avg_result_count":  round(avg_result_count, 1),
        "throughput_qps":    round(throughput_qps, 2),
    }
