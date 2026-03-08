# =============================================================================
# data/generator_phase1.py - Phase 1: 랜덤 바이트 데이터 생성
# =============================================================================
# 실제 PQC 서명 대신, 정확한 크기의 랜덤 바이트를 생성합니다.
#
# 사용 이유:
#   - 데이터 생성 속도가 매우 빠름 (PQC 서명 생성은 느림)
#   - 서명 크기만 동일하면 DB B+tree 성능에 미치는 영향은 동일
#   - 1,620번 실험을 현실적인 시간 내에 완료 가능
#
# Phase 2 (실제 서명)와 비교하여 결과가 동일한지 검증합니다.
# =============================================================================

import os
import hashlib
import random
from datetime import datetime, timedelta, timezone
from config import ALGORITHMS


def generate_message_hash() -> str:
    """
    임의의 메시지 해시를 생성합니다. (64자리 16진수 SHA-256)
    실제 메시지를 해싱한 것처럼 보이도록 랜덤 바이트를 해싱합니다.
    """
    return hashlib.sha256(os.urandom(32)).hexdigest()


def generate_random_bytes(size: int) -> bytes:
    """
    지정한 크기의 랜덤 바이트를 생성합니다.
    PQC 서명/공개키 크기에 맞는 바이트를 시뮬레이션합니다.
    """
    return os.urandom(size)


def generate_timestamps(n: int, start_year: int = 2023) -> list:
    """
    n개의 랜덤 타임스탬프를 생성합니다.
    범위 조회 실험을 위해 1년 범위 내에 균등 분포합니다.

    Args:
        n          : 생성할 타임스탬프 수
        start_year : 시작 연도

    Returns:
        datetime 리스트 (timezone-aware, UTC)
    """
    start = datetime(start_year, 1, 1, tzinfo=timezone.utc)
    end   = datetime(start_year + 1, 1, 1, tzinfo=timezone.utc)
    total_seconds = int((end - start).total_seconds())

    timestamps = [
        start + timedelta(seconds=random.randint(0, total_seconds))
        for _ in range(n)
    ]
    return timestamps


def generate_records(algorithm_name: str, n: int, batch_size: int = 1000) -> iter:
    """
    알고리즘에 맞는 크기의 랜덤 레코드를 배치 단위로 생성합니다.
    메모리를 아끼기 위해 제너레이터(generator)로 구현합니다.

    Args:
        algorithm_name : config.py의 알고리즘 이름 (예: 'ml-dsa-44')
        n              : 총 생성할 레코드 수
        batch_size     : 한 번에 yield할 배치 크기

    Yields:
        batch_size 크기의 record dict 리스트
    """
    algo = ALGORITHMS[algorithm_name]
    sig_size = algo["sig_size"]  # 서명 크기 (bytes)
    pk_size  = algo["pk_size"]   # 공개키 크기 (bytes)

    # 타임스탬프를 미리 생성 (범위 조회 균등 분포를 위해)
    timestamps = generate_timestamps(n)

    batch = []
    for i in range(n):
        record = {
            "id":           i + 1,                        # 1부터 시작하는 고유번호
            "created_at":   timestamps[i],                 # 랜덤 생성시간
            "signer_id":    random.randint(1, 10_000),     # 1~10,000 사이 서명자 ID
            "message_hash": generate_message_hash(),       # 랜덤 메시지 해시
            "algorithm":    algorithm_name,                # 알고리즘 이름
            "public_key":   generate_random_bytes(pk_size),  # 랜덤 공개키
            "signature":    generate_random_bytes(sig_size), # 랜덤 서명
        }
        batch.append(record)

        # 배치가 찼으면 yield하고 초기화
        if len(batch) == batch_size:
            yield batch
            batch = []

    # 마지막 남은 배치 yield
    if batch:
        yield batch


def get_time_range(algorithm_name: str, n: int, ratio: float = 0.1):
    """
    범위 조회 실험에 사용할 시간 범위를 계산합니다.
    전체 데이터의 ratio 비율에 해당하는 시간 구간을 반환합니다.

    Args:
        algorithm_name : 알고리즘 이름 (현재는 미사용, 확장성 위해 포함)
        n              : 총 레코드 수
        ratio          : 조회할 비율 (기본 10%)

    Returns:
        (start_time, end_time) 튜플
    """
    start = datetime(2023, 1, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 1, 1, tzinfo=timezone.utc)
    total_seconds = int((end - start).total_seconds())

    # 전체 기간의 ratio만큼의 구간을 랜덤하게 선택
    window = int(total_seconds * ratio)
    offset = random.randint(0, total_seconds - window)

    range_start = start + timedelta(seconds=offset)
    range_end   = range_start + timedelta(seconds=window)
    return range_start, range_end
