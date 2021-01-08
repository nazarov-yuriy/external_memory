#include "grader.h"

#include <iostream>
#include <string>
#include <fstream>
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <fcntl.h>
#include <chrono>
#include <random>
#include <sys/mman.h>
#include <sys/stat.h>
#include <string.h>
#include <assert.h>
#include <vector>
#include <iomanip>

using namespace std;

struct Result {
    double value;
    double variation;
};

#define DEFAULT_FILE_SIZE 1024*1024*1024
#define DEFAULT_CHUNK_SIZE 64*1024
#define DEFAULT_OP_COUNT 10000
#define SYNC_OP_COUNT 1000
#define MEASUREMENTS_COUNT 5
#define PAGE_SIZE 4096lu
#define PAGE_ROUND_DOWN(x) (((uint64_t)(x)) & (~(PAGE_SIZE-1)))
#define PAGE_ROUND_UP(x) ( (((uint64_t)(x)) + PAGE_SIZE-1)  & (~(PAGE_SIZE-1)) )


uint64_t rand64() {
    auto hi = (unsigned) rand();
    auto lo = (unsigned) rand();
    uint64_t res = (((uint64_t) hi) << (unsigned) 32) | lo;
    return res;
}

struct operation_stats {
    string name;
    double throughput;
    double latency;
    double duration;
};

struct file_stats {
    string path;
    off_t size;
    off_t pages;
    off_t in_core;
};

class BaseOperations {
public:
    virtual operation_stats write_speed(const string &file_path, off_t len, off_t chunk_len, bool invalidate) = 0;

    virtual operation_stats write_latency(const string &file_path, off_t len, off_t count, bool invalidate) = 0;

    virtual operation_stats read_speed(const string &file_path, off_t len, off_t chunk_len, bool invalidate) = 0;

    virtual operation_stats read_latency(const string &file_path, off_t len, off_t count, bool invalidate) = 0;

    virtual ~BaseOperations() = default;

    file_stats get_file_stats(const string &file_path) {
        struct stat st = {};
        int fd = open(file_path.c_str(), O_RDONLY);
        fstat(fd, &st);

        void *pp = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);

        int howmuch = (st.st_size + PAGE_SIZE - 1) / PAGE_SIZE;
        unsigned char here[howmuch];
        memset(here, 0, howmuch);
        mincore(pp, st.st_size, here);
        munmap(pp, st.st_size);
        close(fd);

        int sum = 0, i = 0;
        for (i = 0; i < howmuch; i++) {
            sum += here[i] & 1u;
        }
        return {
                .path = file_path,
                .size = st.st_size,
                .pages = howmuch,
                .in_core = sum,
        };
    }

    void allocate_file(const string &file_path, off_t len, bool write_zeros = true) {
        int fd = open(file_path.c_str(), O_RDWR | O_CREAT, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        int res;
        res = fallocate(fd, 0, 0, len);
        if (write_zeros) {
            char buf[DEFAULT_CHUNK_SIZE];
            for (off_t pos = 0; pos + DEFAULT_CHUNK_SIZE <= len; pos += DEFAULT_CHUNK_SIZE) {
                ssize_t wsz = write(fd, buf, DEFAULT_CHUNK_SIZE);
                if (wsz != DEFAULT_CHUNK_SIZE) throw ifstream::failure("Failed to write");
            }
            fdatasync(fd);
        }
        close(fd);
        if (res < 0) {
            throw ifstream::failure("Failed to fallocate");
        }
    }
};

class StandardIOOperations : public BaseOperations {
public:
    operation_stats write_speed(const string &file_path, off_t len, off_t chunk_len, bool invalidate) {
        int fd = open(file_path.c_str(), O_WRONLY, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[chunk_len];

        auto start = std::chrono::high_resolution_clock::now();
        for (off_t pos = 0; pos + chunk_len <= len; pos += chunk_len) {
            ssize_t res = write(fd, buf, chunk_len);
            if (res != chunk_len) throw ifstream::failure("Failed to write");
        }
        fdatasync(fd);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "sequential write",
                .throughput = len / diff.count(),
                .latency = -1,
                .duration = diff.count(),
        };
    }

    operation_stats write_latency(const string &file_path, off_t len, off_t count, bool invalidate) {
        int fd = open(file_path.c_str(), O_WRONLY, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[1];

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < count; i++) {
            ssize_t res = pwrite(fd, buf, 1, rand64() % len);
            if (res != 1) throw ifstream::failure("Failed to write");
            if (0 == i % SYNC_OP_COUNT) fdatasync(fd);
        }
        fdatasync(fd);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "random write",
                .throughput = -1,
                .latency = diff.count() / count,
                .duration = diff.count(),
        };
    }

    operation_stats read_speed(const string &file_path, off_t len, off_t chunk_len, bool invalidate) {
        int fd = open(file_path.c_str(), O_RDONLY, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[chunk_len];
        if (invalidate) {
            posix_fadvise(fd, 0, len, POSIX_FADV_DONTNEED);
        }

        auto start = std::chrono::high_resolution_clock::now();
        for (off_t pos = 0; pos + chunk_len <= len; pos += chunk_len) {
            ssize_t res = read(fd, buf, chunk_len);
            if (res != chunk_len) throw ifstream::failure("Failed to read");
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "sequential read",
                .throughput = len / diff.count(),
                .latency = -1,
                .duration = diff.count(),
        };
    }

    operation_stats read_latency(const string &file_path, off_t len, off_t count, bool invalidate) {
        int fd = open(file_path.c_str(), O_RDONLY, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        if (invalidate) {
            posix_fadvise(fd, 0, len, POSIX_FADV_DONTNEED);
        }
        char buf[1];

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < count; i++) {
            ssize_t res = pread(fd, buf, 1, rand64() % len);
            if (res != 1) throw ifstream::failure("Failed to read");
        }
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "random read",
                .throughput = -1,
                .latency = diff.count() / count,
                .duration = diff.count(),
        };
    }

    ~StandardIOOperations() override = default;
};

class DirectIOOperations : public BaseOperations {
public:
    operation_stats write_speed(const string &file_path, off_t len, off_t chunk_len, bool invalidate) {
        int fd = open(file_path.c_str(), O_WRONLY | O_DIRECT, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[chunk_len + PAGE_SIZE];

        auto start = std::chrono::high_resolution_clock::now();
        for (off_t pos = 0; pos + chunk_len <= len; pos += chunk_len) {
            ssize_t res = write(fd, (char *) PAGE_ROUND_UP(buf), chunk_len);
            if (res != chunk_len) throw ifstream::failure("Failed to write");
        }
        fdatasync(fd);
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "sequential write",
                .throughput = len / diff.count(),
                .latency = -1,
                .duration = diff.count(),
        };
    }

    operation_stats write_latency(const string &file_path, off_t len, off_t count, bool invalidate) {
        int fd = open(file_path.c_str(), O_WRONLY | O_DIRECT, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[PAGE_SIZE * 2];

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < count; i++) {
            ssize_t res = pwrite(fd, (char *) PAGE_ROUND_UP(buf), PAGE_SIZE, PAGE_ROUND_DOWN(rand64() % len));
            if (res != PAGE_SIZE) throw ifstream::failure("Failed to write");
        }
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "random write",
                .throughput = -1,
                .latency = diff.count() / count,
                .duration = diff.count(),
        };
    }

    operation_stats read_speed(const string &file_path, off_t len, off_t chunk_len, bool invalidate) {
        int fd = open(file_path.c_str(), O_RDONLY | O_DIRECT, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[chunk_len + PAGE_SIZE];

        auto start = std::chrono::high_resolution_clock::now();
        for (off_t pos = 0; pos + chunk_len <= len; pos += chunk_len) {
            ssize_t res = read(fd, (char *) PAGE_ROUND_UP(buf), chunk_len);
            if (res != chunk_len) throw ifstream::failure("Failed to read");
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "sequential read",
                .throughput = len / diff.count(),
                .latency = -1,
                .duration = diff.count(),
        };
    }

    operation_stats read_latency(const string &file_path, off_t len, off_t count, bool invalidate) {
        int fd = open(file_path.c_str(), O_RDONLY | O_DIRECT, 0660);
        if (fd < 0) {
            throw ifstream::failure("Failed to open");
        }
        char buf[PAGE_SIZE * 2];

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < count; i++) {
            ssize_t res = pread(fd, (char *) PAGE_ROUND_UP(buf), PAGE_SIZE, PAGE_ROUND_DOWN(rand64() % len));
            if (res != PAGE_SIZE) throw ifstream::failure("Failed to read");
        }
        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> diff = end - start;

        close(fd);
        return {
                .name = "random read",
                .throughput = -1,
                .latency = diff.count() / count,
                .duration = diff.count(),
        };
    }

    ~DirectIOOperations() override = default;
};

BaseOperations *operations;

std::ostream &operator<<(std::ostream &os, const Result &result) {
    os << result.value << " +- " << fixed << setprecision(1) << result.variation;
    return os;
}

float mean(vector<float> &data) {
    float sum = 0.0;
    for (auto &x : data) {
        sum += x;
    }
    return sum / data.size();
}

float stddev(vector<float> &data) {
    float standard_deviation = 0.0;
    float m = mean(data);
    for (auto &x : data) {
        standard_deviation += pow(x - m, 2);
    }
    return sqrt(standard_deviation / (data.size() - 1));
}

Result TestSequentialRead(const std::string &filename) {
    if (operations == NULL) operations = new StandardIOOperations();
    operations->allocate_file(filename, DEFAULT_FILE_SIZE);
    vector<float> measurements;
    for (int i = 0; i < MEASUREMENTS_COUNT; i++) {
        auto op_stats = operations->read_speed(filename, DEFAULT_FILE_SIZE, DEFAULT_CHUNK_SIZE, true);
        measurements.push_back(op_stats.throughput / 1e6);
    }
    return Result{mean(measurements), stddev(measurements)};
}

Result TestSequentialWrite(const std::string &filename) {
    if (operations == NULL) operations = new StandardIOOperations();
    operations->allocate_file(filename, DEFAULT_FILE_SIZE);
    vector<float> measurements;
    for (int i = 0; i < MEASUREMENTS_COUNT; i++) {
        auto op_stats = operations->write_speed(filename, DEFAULT_FILE_SIZE, DEFAULT_CHUNK_SIZE, true);
        measurements.push_back(op_stats.throughput / 1e6);
    }
    return Result{mean(measurements), stddev(measurements)};
}

Result TestRandomRead(const std::string &filename) {
    if (operations == NULL) operations = new StandardIOOperations();
    operations->allocate_file(filename, DEFAULT_FILE_SIZE);
    vector<float> measurements;
    for (int i = 0; i < MEASUREMENTS_COUNT; i++) {
        auto op_stats = operations->read_latency(filename, DEFAULT_FILE_SIZE, DEFAULT_OP_COUNT, true);
        measurements.push_back(op_stats.latency * 1e6);
    }
    return Result{mean(measurements), stddev(measurements)};
}

Result TestRandomWrite(const std::string &filename) {
    if (operations == NULL) operations = new StandardIOOperations();
    operations->allocate_file(filename, DEFAULT_FILE_SIZE);
    vector<float> measurements;
    for (int i = 0; i < MEASUREMENTS_COUNT; i++) {
        auto op_stats = operations->write_latency(filename, DEFAULT_FILE_SIZE, DEFAULT_OP_COUNT, true);
        measurements.push_back(op_stats.latency * 1e6);
    }
    return Result{mean(measurements), stddev(measurements)};
}

int main(int argc, char **argv) {
    if (argc < 3) {
        cerr << "Incorrect number of arguments: " << argc << ", expected at least 3 arguments: ./" << argv[0]
             << " {mode} {filename}" << endl;
        return 1;
    }

    string mode = argv[1];
    string filename = argv[2];
    if (argc > 3 && "direct_io" == string(argv[3])) {
        operations = new DirectIOOperations();
    } else {
        operations = new StandardIOOperations();
    }

    if (mode == "seq-read") {
        auto result = TestSequentialRead(filename);
        cout << result << " MB/s" << endl;
    } else if (mode == "seq-write") {
        auto result = TestSequentialWrite(filename);
        cout << result << " MB/s" << endl;
    } else if (mode == "rnd-read") {
        auto result = TestRandomRead(filename);
        cout << result << " mcs" << endl;
    } else if (mode == "rnd-write") {
        auto result = TestRandomWrite(filename);
        cout << result << " mcs" << endl;
    } else if (mode == "allocate") {
        operations->allocate_file(filename, DEFAULT_FILE_SIZE);
    } else if (mode == "show-cache") {
        auto stats = operations->get_file_stats(filename);
        cout << "file path: " << stats.path << endl;
        cout << "file size: " << stats.size / 1e6 << " MB" << endl;
        cout << "file cached pages: " << stats.in_core << "/" << stats.pages << endl;
    } else {
        cerr << "Incorrect mode: " << mode << endl;
        return 1;
    }

    return 0;
}
