// Exhaustive test of the chunking logic: the NDJSON input is fed through
// buffers of every size from tiny (smaller than one line, forcing the buffer
// to grow) to large, with readers that return data in pieces of various
// sizes, so that chunk boundaries fall on every possible byte: inside
// strings, right before/after '\n', inside "\r\n", inside numbers, ...

#include "ndjson_stream.h"
#include "records.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

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

int main() {
  bool ok = test_edge_cases();
  {
    query_result expected;
    std::string input = make_ndjson(60, 42, expected);
    ok = test_chunking(input, expected) && ok;
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
    if (ok) { std::printf("large input: OK\n"); }
  }
  std::printf(ok ? "ALL TESTS PASSED\n" : "SOME TESTS FAILED\n");
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
