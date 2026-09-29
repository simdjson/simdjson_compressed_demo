// Exhaustive test of the chunking logic: the NDJSON input is fed through
// buffers of every size from tiny (smaller than one line, forcing the buffer
// to grow) to large, with readers that return data in pieces of various
// sizes, so that chunk boundaries fall on every possible byte: inside
// strings, right before/after '\n', inside "\r\n", inside numbers, ...
// The multithreaded versions (parallel_ndjson.h) are tested the same way.

#include "ndjson_stream.h"
#include "parallel_ndjson.h"
#include "records.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <vector>

struct memory_reader {
  std::string_view data;
  size_t max_piece; // returns at most this many bytes per read()
  size_t pos{0};
  size_t read(char *out, size_t capacity) {
    size_t n = std::min({capacity, max_piece, data.size() - pos});
    std::memcpy(out, data.data() + pos, n);
    pos += n;
    return n;
  }
};

static query_result run(std::string_view input, size_t capacity, size_t piece) {
  memory_reader reader{input, piece};
  query_result r;
  for_each_ndjson_document(reader, [&](simdjson::ondemand::document_reference doc) { accumulate(doc, r); },
                           capacity);
  return r;
}

static bool test_chunking(const std::string &input, const query_result &expected) {
  size_t runs = 0;
  for (size_t capacity = 1; capacity <= 2048; capacity++) {
    for (size_t piece : {size_t(1), size_t(7), size_t(4096)}) {
      if (piece == 1 && capacity % 16 != 0) { continue; } // byte-at-a-time is slow; sample it
      query_result r;
      try {
        r = run(input, capacity, piece);
      } catch (const std::exception &e) {
        std::printf("FAIL capacity=%zu piece=%zu: %s\n", capacity, piece, e.what());
        return false;
      }
      if (!(r == expected)) {
        std::printf("FAIL capacity=%zu piece=%zu: count=%llu (expected %llu)\n", capacity, piece,
                    (unsigned long long)r.count, (unsigned long long)expected.count);
        return false;
      }
      runs++;
    }
  }
  std::printf("chunking: %zu runs OK (%zu-byte input, %llu documents)\n", runs, input.size(),
              (unsigned long long)expected.count);
  return true;
}

static bool test_edge_cases() {
  struct { const char *input; size_t count; } good[] = {
      {"", 0}, {"\n\n  \n", 0}, {"{}", 1}, {"{}\n", 1}, {"{}\n{}", 2}, {"{}\r\n{}\r\n", 2},
      {"1\n22\n333\n\"s\"\ntrue\nnull\n[1,[2]]\n", 7}, {"12345", 1},
  };
  for (auto &g : good) {
    for (size_t capacity : {size_t(1), size_t(2), size_t(3), size_t(64)}) {
      for (size_t piece : {size_t(1), size_t(2), size_t(64)}) {
        memory_reader reader{g.input, piece};
        size_t n = 0;
        try {
          for_each_ndjson_document(reader, [&](simdjson::ondemand::document_reference) { n++; }, capacity);
        } catch (const std::exception &e) {
          std::printf("FAIL: '%s' capacity=%zu: %s\n", g.input, capacity, e.what());
          return false;
        }
        if (n != g.count) {
          std::printf("FAIL: '%s' capacity=%zu: %zu documents, expected %zu\n", g.input, capacity, n, g.count);
          return false;
        }
      }
    }
  }
  const char *bad[] = {"{\"a\":1}\n{\"a\":", "{\"a\":1}\n{\"a\":]}\n", "{\"a\":1}\n}\n"};
  for (const char *b : bad) {
    for (size_t capacity : {size_t(4), size_t(64)}) {
      memory_reader reader{b, 3};
      bool thrown = false;
      try {
        // A document may fail only when it is accessed, so touch every value.
        for_each_ndjson_document(reader, [](simdjson::ondemand::document_reference doc) {
          std::string_view json;
          if (doc.raw_json().get(json)) { throw std::runtime_error("bad"); }
        }, capacity);
      } catch (const std::exception &) { thrown = true; }
      if (!thrown) {
        std::printf("FAIL: malformed input accepted: '%s'\n", b);
        return false;
      }
    }
  }
  std::printf("edge cases: OK\n");
  return true;
}

// Runs the query with one of the multithreaded functions; `process` receives
// the per-thread callback. Returns the merged result.
static query_result run_parallel(size_t threads,
                                 const std::function<void(std::function<void(size_t, simdjson::ondemand::document_reference)>)> &process) {
  std::vector<query_result> results(threads);
  process([&](size_t t, simdjson::ondemand::document_reference doc) { accumulate(doc, results[t]); });
  query_result r;
  for (auto &x : results) {
    r.count += x.count;
    r.active += x.active;
    r.admin_score_sum += x.admin_score_sum;
  }
  return r;
}

static std::string write_temp_file(const std::string &content) {
  char path[] = "/tmp/ndjson_test_XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0 || write(fd, content.data(), content.size()) != ssize_t(content.size())) {
    std::perror("temp file");
    std::exit(EXIT_FAILURE);
  }
  close(fd);
  return path;
}

// Every chunk size (the slices of the file, or the chunks read from the
// stream) with several thread counts.
static bool test_parallel(const std::string &input, const query_result &expected, size_t max_chunk) {
  std::string path = write_temp_file(input);
  bool ok = true;
  size_t runs = 0;
  for (size_t chunk = 1; chunk <= max_chunk && ok; chunk++) {
    for (size_t threads : {size_t(1), size_t(3), size_t(8)}) {
      query_result r;
      const char *which = "file";
      try {
        r = run_parallel(threads, [&](auto cb) { for_each_ndjson_document_in_file(path.c_str(), cb, threads, chunk); });
        if (r == expected) {
          which = "stream";
          memory_reader reader{input, 4096};
          r = run_parallel(threads, [&](auto cb) { for_each_ndjson_document_parallel(reader, cb, threads, chunk); });
        }
      } catch (const std::exception &e) {
        std::printf("FAIL parallel %s chunk=%zu threads=%zu: %s\n", which, chunk, threads, e.what());
        ok = false;
        break;
      }
      if (!(r == expected)) {
        std::printf("FAIL parallel %s chunk=%zu threads=%zu: count=%llu (expected %llu)\n", which, chunk, threads,
                    (unsigned long long)r.count, (unsigned long long)expected.count);
        ok = false;
        break;
      }
      runs += 2;
    }
  }
  std::remove(path.c_str());
  if (ok) { std::printf("parallel: %zu runs OK (%zu-byte input)\n", runs, input.size()); }
  return ok;
}

static bool test_parallel_edge_cases() {
  struct { const char *input; size_t count; } good[] = {
      {"", 0}, {"\n\n  \n", 0}, {"{}", 1}, {"{}\n", 1}, {"{}\n{}", 2}, {"{}\r\n{}\r\n", 2},
      {"1\n22\n333\n\"s\"\ntrue\nnull\n[1,[2]]\n", 7}, {"12345", 1},
  };
  for (auto &g : good) {
    std::string path = write_temp_file(g.input);
    for (size_t chunk : {size_t(1), size_t(2), size_t(3), size_t(64)}) {
      for (size_t threads : {size_t(1), size_t(4)}) {
        std::atomic<size_t> n{0}, m{0};
        try {
          for_each_ndjson_document_in_file(path.c_str(), [&](size_t, simdjson::ondemand::document_reference) { n++; },
                                           threads, chunk);
          memory_reader reader{g.input, 2};
          for_each_ndjson_document_parallel(reader, [&](size_t, simdjson::ondemand::document_reference) { m++; },
                                            threads, chunk);
        } catch (const std::exception &e) {
          std::printf("FAIL parallel: '%s' chunk=%zu: %s\n", g.input, chunk, e.what());
          return false;
        }
        if (n != g.count || m != g.count) {
          std::printf("FAIL parallel: '%s' chunk=%zu: %zu/%zu documents, expected %zu\n", g.input, chunk,
                      size_t(n), size_t(m), g.count);
          return false;
        }
      }
    }
    std::remove(path.c_str());
  }
  const char *bad[] = {"{\"a\":1}\n{\"a\":", "{\"a\":1}\n{\"a\":]}\n", "{\"a\":1}\n}\n"};
  auto touch = [](size_t, simdjson::ondemand::document_reference doc) {
    std::string_view json;
    if (doc.raw_json().get(json)) { throw std::runtime_error("bad"); }
  };
  for (const char *b : bad) {
    std::string path = write_temp_file(b);
    for (size_t chunk : {size_t(4), size_t(64)}) {
      bool thrown_file = false, thrown_stream = false;
      try { for_each_ndjson_document_in_file(path.c_str(), touch, 2, chunk); } catch (const std::exception &) { thrown_file = true; }
      memory_reader reader{b, 3};
      try { for_each_ndjson_document_parallel(reader, touch, 2, chunk); } catch (const std::exception &) { thrown_stream = true; }
      if (!thrown_file || !thrown_stream) {
        std::printf("FAIL parallel: malformed input accepted: '%s'\n", b);
        return false;
      }
    }
    std::remove(path.c_str());
  }
  std::printf("parallel edge cases: OK\n");
  return true;
}

int main() {
  bool ok = test_edge_cases();
  ok = test_parallel_edge_cases() && ok;
  {
    query_result expected;
    std::string input = make_ndjson(60, 42, expected);
    ok = test_chunking(input, expected) && ok;
    ok = test_parallel(input, expected, 1024) && ok;
  }
  {
    query_result expected;
    std::string input = make_ndjson(50000, 7, expected);
    for (size_t capacity : {size_t(100), size_t(4096), size_t(65536), size_t(1 << 22)}) {
      if (!(run(input, capacity, 1 << 16) == expected)) {
        std::printf("FAIL large input, capacity=%zu\n", capacity);
        ok = false;
      }
    }
    for (size_t chunk : {size_t(100), size_t(4096), size_t(65536)}) {
      std::string path = write_temp_file(input);
      memory_reader reader{input, 1 << 16};
      if (!(run_parallel(16, [&](auto cb) { for_each_ndjson_document_in_file(path.c_str(), cb, 16, chunk); }) == expected) ||
          !(run_parallel(16, [&](auto cb) { for_each_ndjson_document_parallel(reader, cb, 16, chunk); }) == expected)) {
        std::printf("FAIL large input, parallel, chunk=%zu\n", chunk);
        ok = false;
      }
      std::remove(path.c_str());
    }
    if (ok) { std::printf("large input: OK\n"); }
  }
  std::printf(ok ? "ALL TESTS PASSED\n" : "SOME TESTS FAILED\n");
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
