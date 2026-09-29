// Writes a gzip-compressed NDJSON file (one record per line) and prints the
// expected answer to the query, so the demo's output can be checked.
//
//   make_data data.ndjson.gz 5000000

#include "records.h"

#include <zlib.h>

#include <cstdio>
#include <cstdlib>

int main(int argc, char **argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: %s out.ndjson.gz [record_count]\n", argv[0]);
    return EXIT_FAILURE;
  }
  size_t n = argc > 2 ? std::strtoull(argv[2], nullptr, 10) : 1000000;
  gzFile out = gzopen(argv[1], "wb6");
  if (!out) { std::perror(argv[1]); return EXIT_FAILURE; }
  std::mt19937_64 rng(1234);
  query_result expected;
  std::string chunk;
  size_t raw = 0;
  for (size_t i = 0; i < n; i++) {
    append_record(chunk, i, rng, expected);
    chunk += '\n';
    if (chunk.size() > (1 << 20)) {
      gzwrite(out, chunk.data(), unsigned(chunk.size()));
      raw += chunk.size();
      chunk.clear();
    }
  }
  gzwrite(out, chunk.data(), unsigned(chunk.size()));
  raw += chunk.size();
  gzclose(out);
  std::printf("wrote %zu records (%.1f MB uncompressed) to %s\n", n, raw / 1e6, argv[1]);
  std::printf("expected: count=%llu active=%llu admin_score_sum=%lld\n",
              (unsigned long long)expected.count, (unsigned long long)expected.active,
              (long long)expected.admin_score_sum);
  return EXIT_SUCCESS;
}
