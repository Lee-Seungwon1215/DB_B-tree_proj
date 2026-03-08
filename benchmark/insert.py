# =============================================================================
# benchmark/insert.py - INSERT 성능 측정
# =============================================================================
# 레코드를 배치로 삽입하면서 성능을 측정합니다.
#
# 측정 항목:
#   - 총 삽입 시간 (초)
#   - 레코드당 평균 삽입 시간 (ms)
#   - 초당 삽입 건수 (throughput)
# =============================================================================

import time
from config import ALGORITHMS


BATCH_SIZE = 500   # 한 번에 삽입할 레코드 수 (너무 크면 메모리 부족)


def run(conn, strategy_module, algorithm_name: str, records_generator) -> dict:
    """
    INSERT 벤치마크를 실행합니다.

    Args:
        conn               : PostgreSQL 연결 객체
        strategy_module    : 전략 모듈 (strategy_a, strategy_b, ...)
        algorithm_name     : 알고리즘 이름
        records_generator  : 레코드를 배치로 yield하는 제너레이터

    Returns:
        측정 결과 dict
    """
    total_records   = 0
    total_time_sec  = 0.0
    inserted_ids    = []   # 나중에 Point Query / DELETE에 사용할 ID 목록

    print(f"  [INSERT] 삽입 시작...")

    for batch in records_generator:
        # 배치 삽입 시간 측정
        start = time.perf_counter()
        strategy_module.insert_batch(conn, batch)
        end   = time.perf_counter()

        total_time_sec += (end - start)
        total_records  += len(batch)

        # 삽입된 ID 수집 (나중 실험에서 사용)
        inserted_ids.extend([r["id"] for r in batch])

        # 진행률 출력
        print(f"  [INSERT] {total_records:,}건 삽입 완료", end="\r")

    print(f"\n  [INSERT] 완료: {total_records:,}건 / {total_time_sec:.2f}초")

    # 결과 계산
    avg_latency_ms = (total_time_sec / total_records) * 1000 if total_records > 0 else 0
    throughput     = total_records / total_time_sec if total_time_sec > 0 else 0

    return {
        "operation":        "INSERT",
        "total_records":    total_records,
        "total_time_sec":   round(total_time_sec, 4),
        "avg_latency_ms":   round(avg_latency_ms, 4),
        "throughput_rps":   round(throughput, 2),   # records per second
        "inserted_ids":     inserted_ids,           # 다음 실험에 전달용
    }
