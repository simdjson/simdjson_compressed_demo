// Writes a compressed NDJSON file (one record per line) and prints the
// expected answer to the query, so the demo's output can be checked.
// The format follows the extension:
//
//   make_data data.ndjson.gz 5000000              gzip (one stream)
//   make_data data.ndjson.zst 5000000 [frame]     zstd, independent frames of ~frame bytes (default 256 KiB)
//   make_data data.ndjson.lz4 5000000 [frame]     lz4, independent frames of ~frame bytes (default 256 KiB)
//
// Every zstd/lz4 frame ends with a newline, so the frames can be
// decompressed and parsed in parallel (see parallel_frames.h).

#include "frame_writer.h"
#include "records.h"

#include <zlib.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>

static bool ends_with(const char *s, const char *suffix) {
  size_t n = std::strlen(s), m = std::strlen(suffix);
  return n >= m && std::strcmp(s + n - m, suffix) == 0;
}

int main(int argc, char **argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s out.ndjson.{gz,zst,lz4} [record_count] [frame_bytes]\n", argv[0]);
    return EXIT_FAILURE;
  }
  size_t n = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1000000;
  size_t frame_bytes = argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 1 << 18;
  const char *path = argv[1];

  // write(chunk) compresses a block of complete lines.
  std::function<void(const std::string &)> write;
  gzFile gz = nullptr;
  FILE *out = nullptr;
  if (ends_with(path, ".zst") || ends_with(path, ".lz4")) {
    frame_format format = ends_with(path, ".zst") ? frame_format::zstd : frame_format::lz4;
    out = std::fopen(path, "wb");
    if (!out) { std::perror(path); return EXIT_FAILURE; }
    write = [out, format](const std::string &chunk) {
      std::string frame;
      append_frame(frame, format, chunk);
      std::fwrite(frame.data(), 1, frame.size(), out);
    };
  } else {
    gz = gzopen(path, "wb6");
    if (!gz) { std::perror(path); return EXIT_FAILURE; }
    write = [gz](const std::string &chunk) { gzwrite(gz, chunk.data(), unsigned(chunk.size())); };
    frame_bytes = 1 << 20; // just the size of the blocks we hand to zlib
  }

  std::mt19937_64 rng(1234);
  query_result expected;
  std::string chunk;
  size_t raw = 0;
  for (size_t i = 0; i < n; i++) {
    append_record(chunk, i, rng, expected);
    chunk += '\n';
    if (chunk.size() >= frame_bytes) {
      write(chunk);
      raw += chunk.size();
      chunk.clear();
    }
  }
  if (!chunk.empty()) { write(chunk); }
  raw += chunk.size();
  if (gz) { gzclose(gz); }
  if (out) { std::fclose(out); }
  std::printf("wrote %zu records (%.1f MB uncompressed) to %s\n", n, raw / 1e6, path);
  std::printf("expected: count=%llu active=%llu admin_score_sum=%lld\n",
              (unsigned long long)expected.count, (unsigned long long)expected.active,
              (long long)expected.admin_score_sum);
  return EXIT_SUCCESS;
}
