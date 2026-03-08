# =============================================================================
# data/generator.py - 실제 서명 기반 레코드 생성기
# =============================================================================
# 서명 풀(pool) 방식으로 동작합니다:
#   1. 실험 전 SIG_POOL_SIZE개의 실제 (pk, sig) 쌍을 미리 생성
#   2. INSERT 시 pool[i % pool_size] 순환 사용 → DB 성능만 측정
#
# SPHINCS+ 등 느린 알고리즘도 1M건 실험이 현실적으로 가능합니다.
# =============================================================================

import os
import hashlib
import random
import time
from datetime import datetime, timedelta, timezone

from config import ALGORITHMS, SIG_POOL_SIZE
from data.signer import sign


# =============================================================================
# 공통 유틸리티
# =============================================================================

def _make_message_hash() -> str:
    return hashlib.sha256(os.urandom(32)).hexdigest()

def _make_timestamps(n: int) -> list:
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 1, 1, tzinfo=timezone.utc)
    total_sec = int((end - start).total_seconds())
    return [start + timedelta(seconds=random.randint(0, total_sec)) for _ in range(n)]


# =============================================================================
# 서명 풀 생성
# =============================================================================

def build_sig_pool(algorithm_name: str, pool_size: int = SIG_POOL_SIZE) -> list:
    """
    algorithm_name에 대해 pool_size개의 (pk, sig, message_hash) 튜플을 생성합니다.
    Returns:
        [(pk_bytes, sig_bytes, msg_hash_str), ...]
    """
    print(f"  [{algorithm_name}] 서명 풀 생성 중 ({pool_size}개)...", flush=True)
    t0 = time.time()

    pool = []
    for i in range(pool_size):
        msg_hash = _make_message_hash()
        message  = msg_hash.encode()
        pk, sig  = sign(algorithm_name, message)
        pool.append((pk, sig, msg_hash))

        if (i + 1) % max(1, pool_size // 10) == 0:
            elapsed = time.time() - t0
            rate = (i + 1) / elapsed
            remaining = (pool_size - i - 1) / rate if rate > 0 else 0
            print(f"    {i+1}/{pool_size} ({rate:.1f}/s, 잔여 {remaining:.0f}초)",
                  flush=True)

    elapsed = time.time() - t0
    print(f"  풀 생성 완료: {pool_size}개 / {elapsed:.1f}초", flush=True)
    return pool


# =============================================================================
# 레코드 생성기
# =============================================================================

def generate_records(algorithm_name: str, n: int,
                     sig_pool: list = None,
                     batch_size: int = 1000):
    """
    실제 서명 기반 레코드를 배치 단위로 yield합니다.

    Args:
        algorithm_name : config.py의 알고리즘 이름
        n              : 총 레코드 수
        sig_pool       : 사전 생성된 (pk, sig, msg_hash) 풀.
                         None이면 자동 생성합니다.
        batch_size     : 배치 크기

    Yields:
        list of record dict
    """
    if sig_pool is None:
        sig_pool = build_sig_pool(algorithm_name)

    pool_size   = len(sig_pool)
    timestamps  = _make_timestamps(n)

    batch = []
    for i in range(n):
        pk, sig, msg_hash = sig_pool[i % pool_size]
        record = {
            "id":           i + 1,
            "created_at":   timestamps[i],
            "signer_id":    random.randint(1, 10_000),
            "message_hash": msg_hash,
            "algorithm":    algorithm_name,
            "public_key":   pk,
            "signature":    sig,
        }
        batch.append(record)

        if len(batch) == batch_size:
            yield batch
            batch = []

    if batch:
        yield batch


def get_time_range(algorithm_name: str, n: int, ratio: float = 0.1):
    """범위 조회 실험용 시간 범위 반환"""
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 1, 1, tzinfo=timezone.utc)
    total_sec = int((end - start).total_seconds())
    window = int(total_sec * ratio)
    offset = random.randint(0, total_sec - window)
    range_start = start + timedelta(seconds=offset)
    range_end   = range_start + timedelta(seconds=window)
    return range_start, range_end
