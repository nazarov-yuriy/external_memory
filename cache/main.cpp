#include <iostream>
#include <chrono>
#include <vector>
#include <algorithm>

size_t scratch; // Prevent optimization

std::chrono::duration<double> estimate_cache_timings(size_t buf_size, size_t iters = 10000) {
    size_t buf_len = buf_size / sizeof(void *);
    std::vector<void *> buf(buf_len);

    // Prepare
    for (size_t i = 0; i < buf_len; i++) {
        buf[i] = &buf[i];
    }
    std::random_shuffle(buf.begin(), buf.end());

    // Measure
    std::chrono::high_resolution_clock::time_point t1 = std::chrono::high_resolution_clock::now();
    void **pos = &buf[0];
    for (size_t i = 0; i < iters; i++) {
        pos = (void **) *pos;
    }
    scratch = (size_t) pos;
    std::chrono::high_resolution_clock::time_point t2 = std::chrono::high_resolution_clock::now();

    return std::chrono::duration_cast<std::chrono::duration<double>>(t2 - t1) * 1000000000. / iters;
}

int main(int argc, char **argv) {
    size_t iters = 100000000;
    std::vector<size_t> buf_lens = {
            2 * 1024, 4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024,
            64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024,
            2 * 1048576, 4 * 1048576, 8 * 1048576, 16 * 1048576, 32 * 1048576,
            64 * 1048576, 128 * 1048576, 256 * 1048576, 512 * 1048576, 1024 * 1048576,
    };
    if (argc > 1) {
        buf_lens.clear();
        buf_lens.push_back(atol(argv[1]));
    }
    for (size_t buf_len : buf_lens) {
        std::chrono::duration<double> time_span = estimate_cache_timings(buf_len, iters);
        std::cout << buf_len << " " << time_span.count() << std::endl;
    }

    if (scratch) return 0;
    return 0;
}
