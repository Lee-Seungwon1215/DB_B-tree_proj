# =============================================================================
# data/generator_phase2.py - Phase 2: 실제 PQC 서명 생성
# =============================================================================
# liboqs(Open Quantum Safe) 라이브러리를 사용하여 실제 PQC 서명을 생성합니다.
#
# 지원 알고리즘:
#   - ML-DSA (44, 65, 87)    : NIST 표준, liboqs 지원
#   - SPHINCS+ (SHAKE/SHA2)  : NIST 표준, liboqs 지원
#   - AIMER, HAETAE, FAEST   : liboqs 미지원 → Phase 1 랜덤 바이트로 대체
#
# 설치 방법:
#   pip install liboqs-python
#   (liboqs C 라이브러리가 먼저 설치되어 있어야 합니다)
#   brew install liboqs  (Mac M2)
# =============================================================================

import os
import hashlib
import random
from datetime import datetime, timezone
from data.generator_phase1 import generate_message_hash, generate_timestamps, generate_random_bytes
from config import ALGORITHMS, LIBOQS_NAME_MAP


def is_liboqs_supported(algorithm_name: str) -> bool:
    """
    해당 알고리즘이 liboqs에서 지원되는지 확인합니다.
    """
    return algorithm_name in LIBOQS_NAME_MAP


def generate_real_records(algorithm_name: str, n: int, batch_size: int = 100) -> iter:
    """
    실제 PQC 서명을 생성하여 레코드를 만듭니다.
    liboqs 미지원 알고리즘은 Phase 1 랜덤 바이트로 자동 대체합니다.

    Args:
        algorithm_name : 알고리즘 이름 (예: 'ml-dsa-44')
        n              : 총 생성할 레코드 수
        batch_size     : 배치 크기 (PQC 서명 생성은 느려서 작게 설정)

    Yields:
        batch_size 크기의 record dict 리스트
    """
    # liboqs 미지원 알고리즘 → Phase 1으로 대체
    if not is_liboqs_supported(algorithm_name):
        print(f"[Phase 2] {algorithm_name}: liboqs 미지원 → 랜덤 바이트로 대체")
        from data.generator_phase1 import generate_records
        yield from generate_records(algorithm_name, n, batch_size)
        return

    # liboqs 임포트 (설치되지 않은 경우 오류 출력 후 Phase 1으로 대체)
    try:
        import oqs
    except ImportError:
        print(f"[Phase 2] liboqs-python 미설치 → 랜덤 바이트로 대체")
        print("  설치 명령: pip install liboqs-python")
        from data.generator_phase1 import generate_records
        yield from generate_records(algorithm_name, n, batch_size)
        return

    liboqs_name = LIBOQS_NAME_MAP[algorithm_name]
    timestamps  = generate_timestamps(n)

    print(f"[Phase 2] {algorithm_name} ({liboqs_name}) 실제 서명 생성 시작...")

    # liboqs 서명 객체 생성
    with oqs.Signature(liboqs_name) as signer:
        # 키 쌍 생성 (공개키, 비밀키)
        public_key = signer.generate_keypair()

        batch = []
        for i in range(n):
            # 서명할 메시지 생성 (랜덤 32바이트)
            message = os.urandom(32)

            # 실제 PQC 서명 생성
            signature = signer.sign(message)

            record = {
                "id":           i + 1,
                "created_at":   timestamps[i],
                "signer_id":    random.randint(1, 10_000),
                "message_hash": hashlib.sha256(message).hexdigest(),
                "algorithm":    algorithm_name,
                "public_key":   bytes(public_key),
                "signature":    bytes(signature),
            }
            batch.append(record)

            if len(batch) == batch_size:
                yield batch
                batch = []

                # 진행률 출력 (Phase 2는 느리기 때문에 진행 상황 표시)
                completed = (i + 1) / n * 100
                print(f"  진행률: {completed:.1f}% ({i+1}/{n})", end="\r")

        if batch:
            yield batch

    print(f"\n[Phase 2] {algorithm_name} 서명 생성 완료")
