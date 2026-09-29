// Streams a gzip-compressed NDJSON file through simdjson in bounded chunks.
// Multi-frame zstd and lz4 files (see make_data) are recognized by their
// first bytes and are decompressed and parsed in parallel.
//
//   gz_stream_demo data.ndjson.gz [chunk_bytes]
//   gz_stream_demo - < data.ndjson.gz                 (gzip on stdin)
//   zcat data.ndjson.gz | gz_stream_demo --raw -      (already decompressed stdin)
//   gz_stream_demo --decompress-only data.ndjson.gz   (baseline: zlib alone, no parsing)
//   gz_stream_demo --threads 16 data.ndjson.gz        (parse with 16 threads)
//   gz_stream_demo --raw --threads 16 data.ndjson     (uncompressed file, 16 threads)
//   gz_stream_demo --threads 16 data.ndjson.zst       (zstd or lz4 frames, 16 threads)
//
// Memory use is bounded by the chunk size (the buffer only grows if a single
// line is longer than it) times a small multiple of the number of threads,
// independently of the input size.

#include "gzip_reader.h"
#include "ndjson_stream.h"
#include "parallel_frames.h"
#include "parallel_ndjson.h"
#include "records.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

// For input that is already decompressed (e.g., `zcat file.gz | demo --raw -`).
struct raw_reader {
  FILE *in;
  size_t read(char *out, size_t capacity) { return fread(out, 1, capacity, in); }
};

int main(int argc, char **argv) {
  bool raw = false;
  bool decompress_only = false;
  const char *path = nullptr;
  size_t chunk = 0;   // 0: default (64 KiB for a file read by several threads, 1 MiB otherwise)
  size_t threads = 0; // 0: the single-threaded code in ndjson_stream.h
  for (int i = 1; i < argc; i++) {
    if (std::strcmp(argv[i], "--raw") == 0) { raw = true; }
    else if (std::strcmp(argv[i], "--threads") == 0 && i + 1 < argc) { threads = std::strtoull(argv[++i], nullptr, 10); }
    else if (std::strcmp(argv[i], "--decompress-only") == 0) { decompress_only = true; }
    else if (!path) { path = argv[i]; }
    else { chunk = std::strtoull(argv[i], nullptr, 10); }
  }
  if (!path) {
    std::fprintf(stderr, "usage: %s [--raw|--decompress-only] [--threads N] file.ndjson.gz|- [chunk_bytes]\n", argv[0]);
    return EXIT_FAILURE;
  }
  const bool from_stdin = std::strcmp(path, "-") == 0;
  if (chunk == 0) { chunk = threads > 0 && raw && !from_stdin ? 1 << 16 : 1 << 20; }
  FILE *in = from_stdin ? stdin : std::fopen(path, "rb");
  if (!in) { std::perror(path); return EXIT_FAILURE; }
  // zstd or lz4 frames? (A file only: we need to look at the first bytes.)
  bool frames = false;
  if (!from_stdin && !raw) {
    unsigned char magic[4] = {0, 0, 0, 0};
    size_t got = std::fread(magic, 1, 4, in);
    uint32_t m = uint32_t(magic[0]) | uint32_t(magic[1]) << 8 | uint32_t(magic[2]) << 16 | uint32_t(magic[3]) << 24;
    frames = got == 4 && (m == frames_detail::zstd_magic || m == frames_detail::lz4_magic);
    std::rewind(in);
    if (frames && threads == 0) { threads = 1; }
  }

  query_result result;
  auto on_document = [&](simdjson::ondemand::document_reference doc) { accumulate(doc, result); };
  // With threads, each thread accumulates into its own result (on its own
  // cache line) and we add them up at the end.
  struct alignas(64) padded_result { query_result r; };
  std::vector<padded_result> results(threads);
  auto on_document_mt = [&](size_t t, simdjson::ondemand::document_reference doc) { accumulate(doc, results[t].r); };
  auto start = std::chrono::steady_clock::now();
  ndjson_stream_stats stats;
  try {
    if (decompress_only && frames) {
      stats.bytes = for_each_frame(path, [](size_t, const char *, size_t, bool) {}, threads, &stats.chunks);
      double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      std::printf("decompress only (%zu threads): %.1f MB decompressed, %zu frames, %.2f s (%.0f MB/s)\n", threads,
                  stats.bytes / 1e6, stats.chunks, secs, stats.bytes / 1e6 / secs);
      return EXIT_SUCCESS;
    }
    if (decompress_only) {
      gzip_reader reader(in);
      std::unique_ptr<char[]> buf(new char[chunk]);
      size_t n;
      while ((n = reader.read(buf.get(), chunk)) > 0) { stats.bytes += n; }
      double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
      std::printf("decompress only: %.1f MB decompressed, %.2f s (%.0f MB/s)\n", stats.bytes / 1e6, secs,
                  stats.bytes / 1e6 / secs);
      return EXIT_SUCCESS;
    }
    if (frames) {
      // Each thread decompresses and parses whole frames.
      stats = for_each_ndjson_document_in_frames(path, on_document_mt, threads);
    } else if (threads > 0 && raw && !from_stdin) {
      // A regular file: each thread reads (pread) and parses slices of it.
      stats = for_each_ndjson_document_in_file(path, on_document_mt, threads, chunk);
    } else if (threads > 0 && raw) {
      raw_reader reader{in};
      stats = for_each_ndjson_document_parallel(reader, on_document_mt, threads, chunk);
    } else if (threads > 0) {
      // This thread decompresses while the others parse.
      gzip_reader reader(in);
      stats = for_each_ndjson_document_parallel(reader, on_document_mt, threads, chunk);
    } else if (raw) {
      raw_reader reader{in};
      stats = for_each_ndjson_document(reader, on_document, chunk);
    } else {
      gzip_reader reader(in);
      stats = for_each_ndjson_document(reader, on_document, chunk);
    }
  } catch (const std::exception &e) {
    std::fprintf(stderr, "error: %s\n", e.what());
    return EXIT_FAILURE;
  }
  double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();
  if (in != stdin) { std::fclose(in); }
  for (auto &p : results) {
    result.count += p.r.count;
    result.active += p.r.active;
    result.admin_score_sum += p.r.admin_score_sum;
  }

  std::printf("count=%llu active=%llu admin_score_sum=%lld\n",
              (unsigned long long)result.count, (unsigned long long)result.active,
              (long long)result.admin_score_sum);
  std::printf("%zu documents, %.1f MB decompressed, %zu chunks, buffer %zu bytes, %.2f s (%.0f MB/s)\n",
              stats.documents, stats.bytes / 1e6, stats.chunks, stats.max_buffer_size, secs,
              stats.bytes / 1e6 / secs);
  return EXIT_SUCCESS;
}
