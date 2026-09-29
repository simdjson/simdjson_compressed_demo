// Streams a gzip-compressed NDJSON file through simdjson in bounded chunks.
//
//   gz_stream_demo data.ndjson.gz [chunk_bytes]
//   gz_stream_demo - < data.ndjson.gz                 (gzip on stdin)
//   zcat data.ndjson.gz | gz_stream_demo --raw -      (already decompressed stdin)
//   gz_stream_demo --decompress-only data.ndjson.gz   (baseline: zlib alone, no parsing)
//
// Memory use is bounded by the chunk size (the buffer only grows if a single
// line is longer than it), independently of the input size.

#include "gzip_reader.h"
#include "ndjson_stream.h"
#include "records.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>

// For input that is already decompressed (e.g., `zcat file.gz | demo --raw -`).
struct raw_reader {
  FILE *in;
  size_t read(char *out, size_t capacity) { return fread(out, 1, capacity, in); }
};

int main(int argc, char **argv) {
  bool raw = false;
  bool decompress_only = false;
  const char *path = nullptr;
  size_t chunk = 1 << 20;
  for (int i = 1; i < argc; i++) {
    if (std::strcmp(argv[i], "--raw") == 0) { raw = true; }
    else if (std::strcmp(argv[i], "--decompress-only") == 0) { decompress_only = true; }
    else if (!path) { path = argv[i]; }
    else { chunk = std::strtoull(argv[i], nullptr, 10); }
  }
  if (!path) {
    std::fprintf(stderr, "usage: %s [--raw|--decompress-only] file.ndjson.gz|- [chunk_bytes]\n", argv[0]);
    return EXIT_FAILURE;
  }
  FILE *in = std::strcmp(path, "-") == 0 ? stdin : std::fopen(path, "rb");
  if (!in) { std::perror(path); return EXIT_FAILURE; }

  query_result result;
  auto on_document = [&](simdjson::ondemand::document_reference doc) { accumulate(doc, result); };
  auto start = std::chrono::steady_clock::now();
  ndjson_stream_stats stats;
  try {
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
    if (raw) {
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

  std::printf("count=%llu active=%llu admin_score_sum=%lld\n",
              (unsigned long long)result.count, (unsigned long long)result.active,
              (long long)result.admin_score_sum);
  std::printf("%zu documents, %.1f MB decompressed, %zu chunks, buffer %zu bytes, %.2f s (%.0f MB/s)\n",
              stats.documents, stats.bytes / 1e6, stats.chunks, stats.max_buffer_size, secs,
              stats.bytes / 1e6 / secs);
  return EXIT_SUCCESS;
}
