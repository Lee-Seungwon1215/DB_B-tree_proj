# =============================================================================
# benchmark/update.py - UPDATE 성능 측정 (서명 갱신)
# =============================================================================
# 랜덤하게 선택된 레코드의 공개키와 서명을 새 값으로 갱신하며 성능을 측정합니다.
# 키 교체(key rotation) / 재서명 시나리오를 시뮬레이션합니다.
#
# 측정 항목:
#   - 단건 갱신 평균 지연시간 (ms)
#   - 갱신 처리량 (UPS, updates per second)
# =============================================================================

import time
import random
from config import UPDATE_COUNT


def run(conn, strategy_module, inserted_ids: list, sig_pool: list) -> dict:
    """
    UPDATE 벤치마크를 실행합니다.

    Args:
        conn            : PostgreSQL 연결 객체
        strategy_module : 전략 모듈
        inserted_ids    : INSERT에서 삽입된 ID 목록
        sig_pool        : 사전 생성된 서명 풀 [(public_key, signature, msg_hash), ...]

    Returns:
        측정 결과 dict
    """
    update_count = min(UPDATE_COUNT, len(inserted_ids))
    update_ids   = random.sample(inserted_ids, update_count)
    pool_sz      = len(sig_pool)

    latencies = []   # 각 갱신의 지연시간 (ms)

    print(f"  [UPDATE] {update_count}건 서명 갱신 시작...")

    for i, record_id in enumerate(update_ids):
        pk, sig, _ = sig_pool[i % pool_sz]

        start = time.perf_counter()
        strategy_module.update_record(conn, record_id, pk, sig)
        end   = time.perf_counter()

        latency_ms = (end - start) * 1000
        latencies.append(latency_ms)

    avg_latency_ms = sum(latencies) / len(latencies) if latencies else 0
    throughput_ups = 1000 / avg_latency_ms if avg_latency_ms > 0 else 0

    print(f"  [UPDATE] 완료: 평균 {avg_latency_ms:.3f}ms")

    return {
        "operation":        "UPDATE",
        "update_count":     update_count,
        "avg_latency_ms":   round(avg_latency_ms, 4),
        "min_latency_ms":   round(min(latencies), 4) if latencies else 0,
        "max_latency_ms":   round(max(latencies), 4) if latencies else 0,
        "throughput_ups":   round(throughput_ups, 2),   # updates per second
    }
