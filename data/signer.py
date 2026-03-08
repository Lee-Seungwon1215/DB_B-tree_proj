# =============================================================================
# data/signer.py - 통합 서명 생성기
# =============================================================================
# 모든 알고리즘에 대해 실제 암호 서명을 생성합니다.
#
# 지원 라이브러리:
#   - classical (9종)  : cryptography 라이브러리
#   - ml-dsa, sphincs+ : liboqs
#   - haetae (3종)     : KPQClean ctypes (libs/libhaetae{2,3,5}.dylib)
#   - aimer  (3종)     : KPQClean ctypes (libs/libaimer_{l1,l3,l5}.dylib)
# =============================================================================

import ctypes
import os
import warnings

warnings.filterwarnings("ignore")

# 프로젝트 루트 경로
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_LIBS = os.path.join(_ROOT, "libs")

# =============================================================================
# Classical 서명 생성 (cryptography 라이브러리)
# =============================================================================

def _sign_classical(algo: str, message: bytes) -> tuple[bytes, bytes]:
    """(public_key_bytes, signature_bytes) 반환"""
    from cryptography.hazmat.primitives.asymmetric import ec, ed25519, ed448, padding, rsa
    from cryptography.hazmat.primitives import hashes, serialization

    if algo == "ecdsa-256":
        key = ec.generate_private_key(ec.SECP256R1())
        pk = key.public_key().public_bytes(
            serialization.Encoding.X962,
            serialization.PublicFormat.UncompressedPoint)
        sig = key.sign(message, ec.ECDSA(hashes.SHA256()))
        return pk, sig

    elif algo == "ecdsa-384":
        key = ec.generate_private_key(ec.SECP384R1())
        pk = key.public_key().public_bytes(
            serialization.Encoding.X962,
            serialization.PublicFormat.UncompressedPoint)
        sig = key.sign(message, ec.ECDSA(hashes.SHA384()))
        return pk, sig

    elif algo == "ecdsa-521":
        key = ec.generate_private_key(ec.SECP521R1())
        pk = key.public_key().public_bytes(
            serialization.Encoding.X962,
            serialization.PublicFormat.UncompressedPoint)
        sig = key.sign(message, ec.ECDSA(hashes.SHA512()))
        return pk, sig

    elif algo == "ed25519":
        key = ed25519.Ed25519PrivateKey.generate()
        pk = key.public_key().public_bytes_raw()
        sig = key.sign(message)
        return pk, sig

    elif algo == "ed448":
        key = ed448.Ed448PrivateKey.generate()
        pk = key.public_key().public_bytes_raw()
        sig = key.sign(message)
        return pk, sig

    elif algo in ("rsa-2048", "rsa-3072", "rsa-4096", "rsa-7680"):
        bits = int(algo.split("-")[1])
        key = rsa.generate_private_key(public_exponent=65537, key_size=bits)
        pk = key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo)
        sig = key.sign(message, padding.PKCS1v15(), hashes.SHA256())
        return pk, sig

    else:
        raise ValueError(f"Unknown classical algorithm: {algo}")


# =============================================================================
# liboqs 서명 생성 (ML-DSA, SPHINCS+)
# =============================================================================

_oqs_lib = None

def _get_oqs():
    global _oqs_lib
    if _oqs_lib is None:
        import oqs
        _oqs_lib = oqs
    return _oqs_lib

LIBOQS_NAME_MAP = {
    "ml-dsa-44":          "ML-DSA-44",
    "ml-dsa-65":          "ML-DSA-65",
    "ml-dsa-87":          "ML-DSA-87",
    "sphincs-shake-128s": "SPHINCS+-SHAKE-128s-simple",
    "sphincs-shake-128f": "SPHINCS+-SHAKE-128f-simple",
    "sphincs-shake-192s": "SPHINCS+-SHAKE-192s-simple",
    "sphincs-shake-192f": "SPHINCS+-SHAKE-192f-simple",
    "sphincs-shake-256s": "SPHINCS+-SHAKE-256s-simple",
    "sphincs-shake-256f": "SPHINCS+-SHAKE-256f-simple",
    "sphincs-sha2-128s":  "SPHINCS+-SHA2-128s-simple",
    "sphincs-sha2-128f":  "SPHINCS+-SHA2-128f-simple",
    "sphincs-sha2-192s":  "SPHINCS+-SHA2-192s-simple",
    "sphincs-sha2-192f":  "SPHINCS+-SHA2-192f-simple",
    "sphincs-sha2-256s":  "SPHINCS+-SHA2-256s-simple",
    "sphincs-sha2-256f":  "SPHINCS+-SHA2-256f-simple",
}

def _sign_liboqs(algo: str, message: bytes) -> tuple[bytes, bytes]:
    oqs = _get_oqs()
    oqs_name = LIBOQS_NAME_MAP[algo]
    signer = oqs.Signature(oqs_name)
    pk = signer.generate_keypair()
    sig = signer.sign(message)
    return bytes(pk), bytes(sig)


# =============================================================================
# HAETAE ctypes 바인딩
# =============================================================================

_haetae_libs = {}

# HAETAE 파라미터 (params.h 기준)
_HAETAE_PARAMS = {
    2: {"pk": 32 + 2*480,  "sk": 32 + 2*480 + 3*64 + 2*96 + 32, "sig": 1463},
    3: {"pk": 32 + 3*480,  "sk": 32 + 3*480 + 5*64 + 3*96 + 32, "sig": 2337},
    5: {"pk": 32 + 4*512,  "sk": 32 + 4*512 + 6*64 + 4*64 + 32, "sig": 2908},
}

def _get_haetae_lib(mode: int):
    if mode not in _haetae_libs:
        path = os.path.join(_LIBS, f"libhaetae{mode}.dylib")
        lib = ctypes.CDLL(path)
        _haetae_libs[mode] = lib
    return _haetae_libs[mode]

def _sign_haetae(mode: int, message: bytes) -> tuple[bytes, bytes]:
    lib = _get_haetae_lib(mode)
    p = _HAETAE_PARAMS[mode]

    keypair_fn = getattr(lib, f"cryptolab_haetae{mode}_keypair")
    sign_fn    = getattr(lib, f"cryptolab_haetae{mode}_signature")

    pk_buf = ctypes.create_string_buffer(p["pk"])
    sk_buf = ctypes.create_string_buffer(p["sk"])
    keypair_fn(pk_buf, sk_buf)

    sig_buf = ctypes.create_string_buffer(p["sig"])
    siglen  = ctypes.c_size_t(p["sig"])
    ret = sign_fn(sig_buf, ctypes.byref(siglen), message, len(message), sk_buf)
    if ret != 0:
        raise RuntimeError(f"HAETAE-{mode} sign failed: {ret}")

    return bytes(pk_buf.raw), bytes(sig_buf.raw[:siglen.value])


# =============================================================================
# AIMer ctypes 바인딩
# =============================================================================

_aimer_libs = {}

# AIMer 파라미터 (api.h 기준, param1 사용)
_AIMER_PARAMS = {
    "l1": {"pk": 33,  "sk": 49,  "sig": 5904},
    "l3": {"pk": 49,  "sk": 73,  "sig": 13080},
    "l5": {"pk": 65,  "sk": 97,  "sig": 25152},
}

# algo 이름 → aimer_level_key
_AIMER_LEVEL_MAP = {
    "aimer-l1": "l1",
    "aimer-l3": "l3",
    "aimer-l5": "l5",
}

def _get_aimer_lib(level: str):
    if level not in _aimer_libs:
        path = os.path.join(_LIBS, f"libaimer_{level}.dylib")
        lib = ctypes.CDLL(path)
        _aimer_libs[level] = lib
    return _aimer_libs[level]

def _sign_aimer(algo: str, message: bytes) -> tuple[bytes, bytes]:
    level = _AIMER_LEVEL_MAP[algo]
    lib = _get_aimer_lib(level)
    p = _AIMER_PARAMS[level]

    pk_buf = ctypes.create_string_buffer(p["pk"])
    sk_buf = ctypes.create_string_buffer(p["sk"])
    lib.crypto_sign_keypair(pk_buf, sk_buf)

    # crypto_sign: sm = sig || msg
    sm_buf = ctypes.create_string_buffer(p["sig"] + len(message))
    smlen  = ctypes.c_ulonglong(0)
    ret = lib.crypto_sign(sm_buf, ctypes.byref(smlen), message, len(message), sk_buf)
    if ret != 0:
        raise RuntimeError(f"AIMer-{level} sign failed: {ret}")

    sig = bytes(sm_buf.raw[:smlen.value - len(message)])
    return bytes(pk_buf.raw), sig


# =============================================================================
# 통합 인터페이스
# =============================================================================

def sign(algo: str, message: bytes) -> tuple[bytes, bytes]:
    """
    주어진 알고리즘으로 메시지에 서명합니다.

    Returns:
        (public_key_bytes, signature_bytes)
    """
    family = _get_family(algo)

    if family == "classical":
        return _sign_classical(algo, message)
    elif family in ("ml-dsa", "sphincs"):
        return _sign_liboqs(algo, message)
    elif family == "haetae":
        mode = int(algo.split("-")[1])
        return _sign_haetae(mode, message)
    elif family == "aimer":
        return _sign_aimer(algo, message)
    else:
        raise ValueError(f"Unknown algorithm family for: {algo}")


def _get_family(algo: str) -> str:
    if algo.startswith("ecdsa") or algo.startswith("ed") or algo.startswith("rsa"):
        return "classical"
    elif algo.startswith("ml-dsa"):
        return "ml-dsa"
    elif algo.startswith("sphincs"):
        return "sphincs"
    elif algo.startswith("haetae"):
        return "haetae"
    elif algo.startswith("aimer"):
        return "aimer"
    else:
        raise ValueError(f"Cannot determine family for: {algo}")
