/* macOS용 AIMer randombytes - getentropy 직접 사용 */
#include <sys/random.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* AIMer rng.h 정의와 호환 */
#define RNG_SUCCESS      0
#define RNG_BAD_MAXLEN  -1
#define RNG_BAD_OUTBUF  -2
#define RNG_BAD_REQ_LEN -3

void randombytes_init(unsigned char *entropy_input,
                      unsigned char *personalization_string,
                      int security_strength) {
    (void)entropy_input; (void)personalization_string; (void)security_strength;
}

int randombytes(unsigned char *x, unsigned long long xlen) {
    while (xlen > 0) {
        size_t chunk = (xlen > 256) ? 256 : (size_t)xlen;
        if (getentropy(x, chunk) != 0) abort();
        x += chunk;
        xlen -= chunk;
    }
    return RNG_SUCCESS;
}
