#define rdtscll(val)                                    \
    do {                                                \
        uint32_t hi, lo;                                \
        __asm volatile("rdtsc" : "=a" (lo), "=d" (hi)); \
        val = ((uint64_t)hi << 32) | lo;                \
    } while (0)
