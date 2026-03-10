# =============================================================================
# benchmark/range_delete.py - 범위 DELETE 성능 측정
# =============================================================================
# 랜덤하게 선택된 N개 레코드를 단일 DELETE 쿼리로 한번에 삭제합니다.
# 단건 삭제(DELETE 반복)와 달리 하나의 트랜잭션에서 처리됩니다.
# 통계 안정성을 위해 RANGE_DELETE_REPEAT회 반복 측정 후 평균을 구합니다.
# (각 반복은 겹치지 않는 ID 슬라이스를 사용)
#
# 측정 항목:
#   - 범위 삭제 건당 평균 지연시간 (ms, REPEAT회 평균)
#   - 삭제 처리량 (DPS)
# =============================================================================

import time
import random
from config import RANGE_DELETE_COUNT, RANGE_DELETE_REPEAT


def run(conn, strategy_module, inserted_ids: list) -> dict:
    """
    범위 DELETE 벤치마크를 실행합니다.

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 모듈
        inserted_ids    : INSERT에서 삽입된 ID 목록

    Returns:
        측정 결과 dict
    """
    needed = RANGE_DELETE_COUNT * RANGE_DELETE_REPEAT
    repeat = RANGE_DELETE_REPEAT if len(inserted_ids) >= needed else max(1, len(inserted_ids) // RANGE_DELETE_COUNT)
    count  = RANGE_DELETE_COUNT

    # 겹치지 않도록 pool을 셔플 후 슬라이스
    pool = random.sample(inserted_ids, min(count * repeat, len(inserted_ids)))

    print(f"  [RANGE DELETE] {count}건 × {repeat}회 일괄 삭제 시작...")

    per_record_latencies = []

    for i in range(repeat):
        targets = pool[i * count : (i + 1) * count]

        start = time.perf_counter()
        strategy_module.range_delete_records(conn, targets)
        elapsed_ms = (time.perf_counter() - start) * 1000

        per_record_latencies.append(elapsed_ms / len(targets))

    avg_per_record_ms = sum(per_record_latencies) / len(per_record_latencies)
    throughput_dps    = 1000 / avg_per_record_ms if avg_per_record_ms > 0 else 0

    print(f"  [RANGE DELETE] 완료: 건당 평균 {avg_per_record_ms:.4f}ms ({repeat}회 평균)")

    return {
        "operation":       "RANGE_DELETE",
        "delete_count":    count,
        "repeat":          repeat,
        "per_record_ms":   round(avg_per_record_ms, 4),
        "throughput_dps":  round(throughput_dps, 2),
    }
