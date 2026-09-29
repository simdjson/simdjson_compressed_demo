// Tests of the multi-frame zstd and lz4 reader (parallel_frames.h): the
// NDJSON input is compressed into frames of every size, from one line per
// frame to a single frame, in both formats, and read with several thread
// counts. Also: empty and skippable frames, mixed formats, and bad input
// (corrupted or truncated data, a frame that does not end on a newline,
// malformed JSON), which must be reported with an exception.

#include "frame_writer.h"
#include "parallel_frames.h"
#include "records.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

static std::string write_temp_file(const std::string &content) {
  char path[] = "/tmp/frames_test_XXXXXX";
  int fd = mkstemp(path);
  if (fd < 0 || write(fd, content.data(), content.size()) != ssize_t(content.size())) {
    std::perror("temp file");
    std::exit(EXIT_FAILURE);
  }
  close(fd);
  return path;
}

// Counts the documents in a compressed file; throws on error.
static size_t count_documents(const std::string &compressed, size_t threads) {
  std::string path = write_temp_file(compressed);
  ndjson_stream_stats stats;
  try {
    stats = for_each_ndjson_document_in_frames(path.c_str(), [](size_t, simdjson::ondemand::document_reference) {},
                                               threads);
  } catch (...) {
    std::remove(path.c_str());
    throw;
  }
  std::remove(path.c_str());
  return stats.documents;
}

// Runs the query over a compressed file; throws on error.
static query_result run(const std::string &compressed, size_t threads) {
  std::string path = write_temp_file(compressed);
  struct alignas(64) padded { query_result r; };
  std::vector<padded> results(threads);
  try {
    for_each_ndjson_document_in_frames(
        path.c_str(), [&](size_t t, simdjson::ondemand::document_reference doc) { accumulate(doc, results[t].r); },
        threads);
  } catch (...) {
    std::remove(path.c_str());
    throw;
  }
  std::remove(path.c_str());
  query_result r;
  for (auto &p : results) {
    r.count += p.r.count;
    r.active += p.r.active;
    r.admin_score_sum += p.r.admin_score_sum;
  }
  return r;
}

static const char *name(frame_format f) { return f == frame_format::zstd ? "zstd" : "lz4"; }

static bool expect_result(const std::string &compressed, const query_result &expected, const char *what) {
  for (size_t threads : {size_t(1), size_t(3), size_t(8)}) {
    try {
      query_result r = run(compressed, threads);
      if (!(r == expected)) {
        std::printf("FAIL %s threads=%zu: count=%llu (expected %llu)\n", what, threads,
                    (unsigned long long)r.count, (unsigned long long)expected.count);
        return false;
      }
    } catch (const std::exception &e) {
      std::printf("FAIL %s threads=%zu: %s\n", what, threads, e.what());
      return false;
    }
  }
  return true;
}

static bool expect_error(const std::string &compressed, const char *what) {
  for (size_t threads : {size_t(1), size_t(4)}) {
    bool thrown = false;
    try {
      run(compressed, threads);
    } catch (const std::exception &) { thrown = true; }
    if (!thrown) {
      std::printf("FAIL %s threads=%zu: bad input accepted\n", what, threads);
      return false;
    }
  }
  return true;
}

// Every frame size from one line per frame to a single frame.
static bool test_frame_sizes(const std::string &input, const query_result &expected, size_t max_frame) {
  size_t runs = 0;
  for (frame_format format : {frame_format::zstd, frame_format::lz4}) {
    for (size_t frame = 1; frame <= max_frame; frame += (frame < 256 ? 1 : 97)) {
      std::string what = std::string(name(format)) + " frame=" + std::to_string(frame);
      if (!expect_result(compress_frames(input, format, frame), expected, what.c_str())) { return false; }
      runs += 3;
    }
  }
  std::printf("frame sizes: %zu runs OK (%zu-byte input, %llu documents)\n", runs, input.size(),
              (unsigned long long)expected.count);
  return true;
}

static bool test_edge_cases() {
  struct { const char *input; size_t count; } good[] = {
      {"", 0}, {"\n\n  \n", 0}, {"{}", 1}, {"{}\n", 1}, {"{}\n{}", 2}, {"{}\r\n{}\r\n", 2},
      {"1\n22\n333\n\"s\"\ntrue\nnull\n[1,[2]]\n", 7}, {"12345", 1},
  };
  for (frame_format format : {frame_format::zstd, frame_format::lz4}) {
    for (auto &g : good) {
      for (size_t frame : {size_t(1), size_t(2), size_t(64)}) {
        for (size_t threads : {size_t(1), size_t(4)}) {
          std::string compressed = compress_frames(g.input, format, frame);
          size_t documents = 0;
          try {
            documents = count_documents(compressed, threads);
          } catch (const std::exception &e) {
            std::printf("FAIL %s: '%s' frame=%zu: %s\n", name(format), g.input, frame, e.what());
            return false;
          }
          if (documents != g.count) {
            std::printf("FAIL %s: '%s' frame=%zu: %zu documents, expected %zu\n", name(format), g.input, frame,
                        documents, g.count);
            return false;
          }
        }
      }
    }
  }

  query_result expected;
  std::string input = make_ndjson(200, 3, expected);
  size_t half = input.find('\n', input.size() / 2) + 1;
  // Empty frames, skippable frames, and both formats in one file.
  std::string skippable("\x50\x2A\x4D\x18\x03\x00\x00\x00xyz", 11);
  std::string mixed;
  append_frame(mixed, frame_format::zstd, "");
  mixed += skippable;
  mixed += compress_frames(input.substr(0, half), frame_format::lz4, 100);
  append_frame(mixed, frame_format::lz4, "");
  mixed += compress_frames(input.substr(half), frame_format::zstd, 100);
  mixed += skippable;
  if (!expect_result(mixed, expected, "mixed zstd/lz4 with empty and skippable frames")) { return false; }

  // Bad input.
  for (frame_format format : {frame_format::zstd, frame_format::lz4}) {
    std::string good_frames = compress_frames(input, format, 1000);
    std::string what = name(format);
    // A line split across two frames.
    std::string split;
    append_frame(split, format, input.substr(0, half - 5));
    append_frame(split, format, input.substr(half - 5));
    if (!expect_error(split, (what + ": line split across frames").c_str())) { return false; }
    // Truncated file.
    if (!expect_error(good_frames.substr(0, good_frames.size() - 3), (what + ": truncated").c_str())) { return false; }
    // Garbage after the frames.
    if (!expect_error(good_frames + "garbage!", (what + ": trailing garbage").c_str())) { return false; }
    // Corrupted compressed data (in the middle of the first frame).
    std::string corrupt = good_frames;
    for (size_t i = 20; i < 60; i++) { corrupt[i] = char(corrupt[i] ^ 0x5A); }
    if (!expect_error(corrupt, (what + ": corrupted").c_str())) { return false; }
    // Malformed JSON.
    for (const char *bad : {"{\"a\":1}\n{\"a\":", "{\"a\":1}\n{\"a\":]}\n", "{\"a\":1}\n}\n"}) {
      std::string compressed = compress_frames(bad, format, 4);
      for (size_t threads : {size_t(1), size_t(4)}) {
        bool thrown = false;
        std::string path = write_temp_file(compressed);
        try {
          for_each_ndjson_document_in_frames(path.c_str(), [](size_t, simdjson::ondemand::document_reference doc) {
            std::string_view json;
            if (doc.raw_json().get(json)) { throw std::runtime_error("bad"); }
          }, threads);
        } catch (const std::exception &) { thrown = true; }
        std::remove(path.c_str());
        if (!thrown) {
          std::printf("FAIL %s: malformed JSON accepted: '%s'\n", name(format), bad);
          return false;
        }
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
    ok = test_frame_sizes(input, expected, input.size() + 1) && ok;
  }
  {
    query_result expected;
    std::string input = make_ndjson(50000, 7, expected);
    for (frame_format format : {frame_format::zstd, frame_format::lz4}) {
      for (size_t frame : {size_t(100), size_t(4096), size_t(65536), size_t(1 << 22)}) {
        std::string what = std::string("large input, ") + name(format) + " frame=" + std::to_string(frame);
        ok = expect_result(compress_frames(input, format, frame), expected, what.c_str()) && ok;
      }
    }
    if (ok) { std::printf("large input: OK\n"); }
  }
  std::printf(ok ? "ALL TESTS PASSED\n" : "SOME TESTS FAILED\n");
  return ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
