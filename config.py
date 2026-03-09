# =============================================================================
# config.py - 실험 전체 설정 파일
# =============================================================================
# 알고리즘 스펙, 실험 스케일, DB 연결 정보를 관리합니다.
# sig_size / pk_size 는 실제 라이브러리 서명 결과 기준입니다.
# =============================================================================

# -----------------------------------------------------------------------------
# PostgreSQL 연결 설정
# -----------------------------------------------------------------------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "dbname":   "pqc_experiment",
    "user":     "leeseungwon",
    "password": "",
}

# -----------------------------------------------------------------------------
# 알고리즘 설정
#
# sig_size  : 실제 서명 크기 (bytes)
# pk_size   : 실제 공개키 크기 (bytes)
# level     : NIST 보안 레벨 (1, 3, 5)
# family    : 알고리즘 계열
# -----------------------------------------------------------------------------
ALGORITHMS = {
    # -------------------------------------------------------------------------
    # Classical (기존 암호체계)
    # -------------------------------------------------------------------------
    # Level 1 (128-bit)
    "ecdsa-256":  {"sig_size": 72,   "pk_size": 65,  "level": 1, "family": "classical"},  # DER 가변 (71-72)
    "ed25519":    {"sig_size": 64,   "pk_size": 32,  "level": 1, "family": "classical"},
    "rsa-2048":   {"sig_size": 256,  "pk_size": 294, "level": 1, "family": "classical"},
    # Level 3 (192-bit)
    "ecdsa-384":  {"sig_size": 104,  "pk_size": 97,  "level": 3, "family": "classical"},  # DER 가변 (102-104)
    "ed448":      {"sig_size": 114,  "pk_size": 57,  "level": 3, "family": "classical"},
    "rsa-3072":   {"sig_size": 384,  "pk_size": 422, "level": 3, "family": "classical"},
    # Level 5 (256-bit)
    "ecdsa-521":  {"sig_size": 139,  "pk_size": 133, "level": 5, "family": "classical"},  # DER 가변 (138-139)
    "rsa-4096":   {"sig_size": 512,  "pk_size": 550, "level": 5, "family": "classical"},
    "rsa-7680":   {"sig_size": 960,  "pk_size": 998, "level": 5, "family": "classical"},

    # -------------------------------------------------------------------------
    # AIMer (MPC-in-the-Head, KpqC 후보)
    # KPQClean WG0/dsa-AIMer_revised0623, param1 기준 실측값
    # s/f 변종은 내부 MPC 파라미터만 다르고 서명 크기 동일 → 레벨별 1종
    # -------------------------------------------------------------------------
    "aimer-l1":  {"sig_size": 5904,  "pk_size": 33,  "level": 1, "family": "aimer"},
    "aimer-l3":  {"sig_size": 13080, "pk_size": 49,  "level": 3, "family": "aimer"},
    "aimer-l5":  {"sig_size": 25152, "pk_size": 65,  "level": 5, "family": "aimer"},

    # -------------------------------------------------------------------------
    # HAETAE (Lattice 기반, KpqC 후보)
    # KPQClean WG0/dsa-HAETAE_revised0502 기준 실측값
    # -------------------------------------------------------------------------
    "haetae-2":  {"sig_size": 1463,  "pk_size": 992,  "level": 1, "family": "haetae"},
    "haetae-3":  {"sig_size": 2337,  "pk_size": 1472, "level": 3, "family": "haetae"},
    "haetae-5":  {"sig_size": 2908,  "pk_size": 2080, "level": 5, "family": "haetae"},

    # -------------------------------------------------------------------------
    # ML-DSA (CRYSTALS-Dilithium, NIST FIPS 204)
    # -------------------------------------------------------------------------
    "ml-dsa-44": {"sig_size": 2420,  "pk_size": 1312, "level": 2, "family": "ml-dsa"},
    "ml-dsa-65": {"sig_size": 3309,  "pk_size": 1952, "level": 3, "family": "ml-dsa"},
    "ml-dsa-87": {"sig_size": 4627,  "pk_size": 2592, "level": 5, "family": "ml-dsa"},

    # -------------------------------------------------------------------------
    # SPHINCS+ / SLH-DSA (Hash 기반, NIST FIPS 205)
    # -------------------------------------------------------------------------
    "sphincs-shake-128s": {"sig_size": 7856,  "pk_size": 32, "level": 1, "family": "sphincs"},
    "sphincs-shake-128f": {"sig_size": 17088, "pk_size": 32, "level": 1, "family": "sphincs"},
    "sphincs-shake-192s": {"sig_size": 16224, "pk_size": 48, "level": 3, "family": "sphincs"},
    "sphincs-shake-192f": {"sig_size": 35664, "pk_size": 48, "level": 3, "family": "sphincs"},
    "sphincs-shake-256s": {"sig_size": 29792, "pk_size": 64, "level": 5, "family": "sphincs"},
    "sphincs-shake-256f": {"sig_size": 49856, "pk_size": 64, "level": 5, "family": "sphincs"},
    "sphincs-sha2-128s":  {"sig_size": 7856,  "pk_size": 32, "level": 1, "family": "sphincs"},
    "sphincs-sha2-128f":  {"sig_size": 17088, "pk_size": 32, "level": 1, "family": "sphincs"},
    "sphincs-sha2-192s":  {"sig_size": 16224, "pk_size": 48, "level": 3, "family": "sphincs"},
    "sphincs-sha2-192f":  {"sig_size": 35664, "pk_size": 48, "level": 3, "family": "sphincs"},
    "sphincs-sha2-256s":  {"sig_size": 29792, "pk_size": 64, "level": 5, "family": "sphincs"},
    "sphincs-sha2-256f":  {"sig_size": 49856, "pk_size": 64, "level": 5, "family": "sphincs"},
}

# -----------------------------------------------------------------------------
# 실험 스케일 설정
# -----------------------------------------------------------------------------
SCALES = [1_000_000]

# -----------------------------------------------------------------------------
# DB 전략 목록
# -----------------------------------------------------------------------------
STRATEGIES = ["A", "B", "C", "D", "E"]

# -----------------------------------------------------------------------------
# 벤치마크 설정
# -----------------------------------------------------------------------------
POINT_QUERY_COUNT = 100
RANGE_SCAN_COUNT  = 10
DELETE_COUNT      = 1_000
RANGE_RATIO       = 0.1    # 전체의 10% 범위 조회

# -----------------------------------------------------------------------------
# 서명 풀 설정 (사전 서명 생성 방식)
# SPHINCS+처럼 서명이 느린 알고리즘은 풀 크기를 작게 유지
# -----------------------------------------------------------------------------
SIG_POOL_SIZE     = 10_000  # 사전 생성 서명 수 (DB 삽입 시 순환 사용)
SIG_POOL_DIR      = "data/sig_pool"

