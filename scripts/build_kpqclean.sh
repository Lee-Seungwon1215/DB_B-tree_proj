#!/bin/bash
# =============================================================================
# scripts/build_kpqclean.sh - KPQClean HAETAE/AIMer 공유 라이브러리 빌드 (macOS)
# =============================================================================
set -e

PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
KPQCLEAN_DIR="$PROJECT_DIR/KPQClean"
LIBS_DIR="$PROJECT_DIR/libs"
mkdir -p "$LIBS_DIR"

OPENSSL_INC="$(brew --prefix openssl@3)/include"
OPENSSL_LIB="$(brew --prefix openssl@3)/lib"

echo "=== KPQClean 공유 라이브러리 빌드 (macOS) ==="
echo ""

# =============================================================================
# HAETAE (Mode 2, 3, 5)
# - config.h의 HAETAE_MODE 5 하드코딩 → ifndef 가드로 패치됨
# - randombytes.c → randombytes_macos.c (getentropy 사용) 대체
# =============================================================================
HAETAE_DIR="$KPQCLEAN_DIR/WG0/dsa-HAETAE_revised0502"

# randombytes.c를 제외한 소스 목록
HAETAE_SRCS=$(ls "$HAETAE_DIR"/src/*.c | grep -v "randombytes" | tr '\n' ' ')

for MODE in 2 3 5; do
    echo "--- HAETAE Mode $MODE 빌드 중 ---"
    OUT="$LIBS_DIR/libhaetae${MODE}.dylib"

    gcc -shared -fPIC \
        -DHAETAE_MODE=$MODE \
        -march=native -O3 -fomit-frame-pointer \
        -Wno-unused-function -Wno-deprecated-declarations \
        -I"$HAETAE_DIR/inc" \
        -I"$HAETAE_DIR/include" \
        $HAETAE_SRCS \
        "$HAETAE_DIR/src/randombytes_macos.c" \
        -I"$OPENSSL_INC" -L"$OPENSSL_LIB" \
        -lcrypto -lm \
        -o "$OUT"

    echo "  -> $OUT"
done

# =============================================================================
# AIMer (L1, L3, L5) - param1 사용
# - rng.c 제외, macOS 호환 randombytes 사용
# =============================================================================
AIMER_RNG="$LIBS_DIR/randombytes_macos_aimer.c"

build_aimer() {
    local LEVEL=$1   # l1, l3, l5
    local LFLAG=$2   # 1, 3, 5
    local AIM_C=$3   # aim128.c, aim192.c, aim256.c
    local FIELD_C=$4 # field128.c, field192.c, field256.c

    local SRC_DIR="$KPQCLEAN_DIR/WG0/dsa-AIMer_revised0623/AIMer-${LEVEL}-param1"
    local OUT="$LIBS_DIR/libaimer_${LEVEL}.dylib"

    echo "--- AIMer-${LEVEL} 빌드 중 ---"

    SHAKE_SRCS=$(ls "$SRC_DIR"/shake/*.c 2>/dev/null | tr '\n' ' ')

    gcc -shared -fPIC \
        -D_AIMER_L=$LFLAG \
        -march=native -O3 -fomit-frame-pointer \
        -Wno-unused-function -Wno-deprecated-declarations \
        -I"$SRC_DIR" \
        "$SRC_DIR/api.c" \
        "$SRC_DIR/aimer.c" \
        "$SRC_DIR/aimer_instances.c" \
        "$SRC_DIR/aimer_internal.c" \
        "$SRC_DIR/hash.c" \
        "$SRC_DIR/tree.c" \
        "$SRC_DIR/$AIM_C" \
        "$SRC_DIR/aes.c" \
        "$SRC_DIR/field/$FIELD_C" \
        $SHAKE_SRCS \
        "$AIMER_RNG" \
        -I"$OPENSSL_INC" -L"$OPENSSL_LIB" \
        -o "$OUT"

    echo "  -> $OUT"
}

build_aimer "l1" "1" "aim128.c" "field128.c"
build_aimer "l3" "3" "aim192.c" "field192.c"
build_aimer "l5" "5" "aim256.c" "field256.c"

echo ""
echo "=== 빌드 완료 ==="
ls -lh "$LIBS_DIR/"*.dylib
