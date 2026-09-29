#pragma once
// Minimal streaming gzip decompressor on top of zlib.
//
// Reads compressed bytes from a FILE* (a file or stdin) and hands out
// decompressed bytes on demand. Nothing is ever fully decompressed in memory.
// Concatenated gzip members (e.g., produced by `pigz` or `cat a.gz b.gz`)
// are supported.

#include <zlib.h>

#include <cstdio>
#include <stdexcept>
#include <string>
#include <vector>

class gzip_reader {
public:
  explicit gzip_reader(FILE *input, size_t compressed_buffer_size = 1 << 16)
      : in(input), compressed(compressed_buffer_size) {
    // 16 + MAX_WBITS: expect a gzip header (use 32 + MAX_WBITS to auto-detect zlib/gzip).
    if (inflateInit2(&zs, 16 + MAX_WBITS) != Z_OK) {
      throw std::runtime_error("inflateInit2 failed");
    }
  }
  ~gzip_reader() { inflateEnd(&zs); }
  gzip_reader(const gzip_reader &) = delete;
  gzip_reader &operator=(const gzip_reader &) = delete;

  // Decompresses up to `capacity` bytes into `out`. Returns the number of
  // bytes written. Returns 0 only when the input is exhausted.
  size_t read(char *out, size_t capacity) {
    zs.next_out = reinterpret_cast<Bytef *>(out);
    zs.avail_out = static_cast<uInt>(capacity);
    while (zs.avail_out > 0 && !done) {
      if (zs.avail_in == 0) {
        size_t n = fread(compressed.data(), 1, compressed.size(), in);
        if (n == 0) {
          if (ferror(in)) { throw std::runtime_error("read error"); }
          if (!stream_ended) { throw std::runtime_error("truncated gzip input"); }
          done = true;
          break;
        }
        zs.next_in = compressed.data();
        zs.avail_in = static_cast<uInt>(n);
      }
      if (stream_ended) {
        // More bytes after the end of a gzip member: start a new member.
        inflateReset(&zs);
        stream_ended = false;
      }
      int ret = inflate(&zs, Z_NO_FLUSH);
      if (ret == Z_STREAM_END) {
        stream_ended = true;
      } else if (ret != Z_OK && ret != Z_BUF_ERROR) {
        throw std::runtime_error(std::string("zlib error: ") + (zs.msg ? zs.msg : "unknown"));
      }
    }
    return capacity - zs.avail_out;
  }

private:
  FILE *in;
  std::vector<Bytef> compressed;
  z_stream zs{};
  bool stream_ended{false};
  bool done{false};
};
