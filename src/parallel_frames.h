#pragma once
// Multithreaded decompression and parsing of NDJSON stored as a sequence of
// independent zstd or lz4 frames (see frame_writer.h).
//
// Unlike a gzip stream, a file made of many frames can be decompressed in
// parallel: each frame is self-contained, and its compressed size can be
// found from its block headers without decompressing it. The threads take
// the frames one at a time. Each thread decompresses a frame into its own
// buffer and parses it with its own parser.
//
// Requirement: every frame except the last must end with a newline, so that
// no line spans two frames. The reader checks this.
//
// Frames of both formats may be mixed in one file, and skippable frames
// (e.g., metadata) are ignored.

#include "parallel_ndjson.h"

#include <lz4frame.h>
#include <zstd.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace frames_detail {

constexpr uint32_t zstd_magic = 0xFD2FB528;
constexpr uint32_t lz4_magic = 0x184D2204;
inline bool is_skippable(uint32_t magic) { return (magic & 0xFFFFFFF0) == 0x184D2A50; } // both formats

inline uint32_t read_le32(const uint8_t *p) {
  return uint32_t(p[0]) | uint32_t(p[1]) << 8 | uint32_t(p[2]) << 16 | uint32_t(p[3]) << 24;
}

// Compressed size of the lz4 frame at p, found by walking its block headers.
// Returns 0 if the frame is malformed or truncated.
inline size_t lz4_frame_size(const uint8_t *p, size_t n) {
  if (n < 7) { return 0; }
  const uint8_t flg = p[4];
  if ((flg >> 6) != 1) { return 0; } // version
  const bool block_checksum = flg & 0x10, content_checksum = flg & 0x04;
  // magic, FLG, BD, [content size], [dictionary ID], header checksum
  size_t pos = 6 + ((flg & 0x08) ? 8 : 0) + ((flg & 0x01) ? 4 : 0) + 1;
  while (true) {
    if (pos + 4 > n) { return 0; }
    uint32_t block = read_le32(p + pos);
    pos += 4;
    if (block == 0) { break; } // end mark
    pos += (block & 0x7FFFFFFF) + (block_checksum ? 4 : 0);
  }
  pos += content_checksum ? 4 : 0;
  return pos <= n ? pos : 0;
}

struct frame {
  const uint8_t *data;
  size_t size;
  bool zstd;
  bool last; // no data frame follows (the only one that may end without a newline)
};

// Hands out the frames of a file one at a time, to any thread.
class frame_cursor {
public:
  frame_cursor(const uint8_t *data, size_t size) : data(data), size(size) {}

  // Returns false when there are no more frames.
  bool next(frame &f) {
    std::lock_guard<std::mutex> lock(mutex);
    if (!have_next) {
      if (!advance(pending)) { return false; }
    }
    f = pending;
    have_next = advance(pending); // look ahead to know whether f is the last frame
    f.last = !have_next;
    return true;
  }

private:
  // Finds the next data frame at pos, skipping skippable frames.
  bool advance(frame &f) {
    while (pos < size) {
      const uint8_t *p = data + pos;
      const size_t left = size - pos;
      if (left < 4) { throw std::runtime_error("truncated frame"); }
      const uint32_t magic = read_le32(p);
      size_t n = 0;
      if (magic == zstd_magic) {
        n = ZSTD_findFrameCompressedSize(p, left);
        if (ZSTD_isError(n)) { throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
      } else if (magic == lz4_magic) {
        n = lz4_frame_size(p, left);
        if (n == 0) { throw std::runtime_error("lz4: malformed or truncated frame"); }
      } else if (is_skippable(magic)) {
        if (left < 8 || read_le32(p + 4) > left - 8) { throw std::runtime_error("truncated skippable frame"); }
        pos += 8 + read_le32(p + 4);
        continue;
      } else {
        throw std::runtime_error("not a zstd or lz4 frame");
      }
      f = {p, n, magic == zstd_magic, false};
      pos += n;
      return true;
    }
    return false;
  }

  std::mutex mutex;
  const uint8_t *data;
  size_t size;
  size_t pos{0};
  frame pending{};
  bool have_next{false};
};

// Decompresses frames, reusing its context and output buffer.
class frame_decoder {
public:
  frame_decoder() : zstd(ZSTD_createDCtx()) {
    if (!zstd || LZ4F_isError(LZ4F_createDecompressionContext(&lz4, LZ4F_VERSION))) {
      ZSTD_freeDCtx(zstd);
      throw std::runtime_error("cannot create decompression context");
    }
  }
  ~frame_decoder() {
    ZSTD_freeDCtx(zstd);
    LZ4F_freeDecompressionContext(lz4);
  }
  frame_decoder(const frame_decoder &) = delete;
  frame_decoder &operator=(const frame_decoder &) = delete;

  // Decompresses f into buffer() and returns its size. The buffer has
  // SIMDJSON_PADDING readable bytes past the end.
  size_t decode(const frame &f) {
    return f.zstd ? decode_zstd(f) : decode_lz4(f);
  }
  const char *buffer() const { return buf.get(); }
  size_t capacity() const { return cap; }

private:
  void reserve(size_t n) {
    if (n <= cap && buf) { return; }
    cap = std::max(n, 2 * cap);
    buf.reset(new char[cap + simdjson::SIMDJSON_PADDING]);
  }

  size_t decode_zstd(const frame &f) {
    unsigned long long content = ZSTD_getFrameContentSize(f.data, f.size);
    if (content == ZSTD_CONTENTSIZE_UNKNOWN || content == ZSTD_CONTENTSIZE_ERROR) {
      throw std::runtime_error("zstd frame without a content size");
    }
    reserve(size_t(content));
    size_t n = ZSTD_decompressDCtx(zstd, buf.get(), cap, f.data, f.size);
    if (ZSTD_isError(n)) { throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
    if (n != content) { throw std::runtime_error("zstd: wrong content size"); }
    return n;
  }

  size_t decode_lz4(const frame &f) {
    LZ4F_resetDecompressionContext(lz4);
    LZ4F_frameInfo_t info;
    size_t in = f.size;
    size_t ret = LZ4F_getFrameInfo(lz4, &info, f.data, &in);
    if (LZ4F_isError(ret)) { throw std::runtime_error(std::string("lz4: ") + LZ4F_getErrorName(ret)); }
    if (info.contentSize == 0 && ret != 0) {
      // Either empty or of unknown size: we require the size, except for an
      // empty frame (a single end mark).
      if (f.size - in > 4 + (info.contentChecksumFlag ? 4 : 0)) {
        throw std::runtime_error("lz4 frame without a content size");
      }
      return 0;
    }
    // LZ4F_decompress decodes a block directly into our buffer only when a
    // whole block (up to the frame's maximum block size) fits in what is
    // left of it; otherwise it goes through an internal buffer and copies.
    reserve(size_t(info.contentSize) + lz4_max_block_size(info.blockSizeID));
    size_t out = 0;
    while (ret != 0) { // ret == 0: the frame is complete
      size_t dst = cap - out, src = f.size - in;
      ret = LZ4F_decompress(lz4, buf.get() + out, &dst, f.data + in, &src, nullptr);
      if (LZ4F_isError(ret)) { throw std::runtime_error(std::string("lz4: ") + LZ4F_getErrorName(ret)); }
      in += src;
      out += dst;
      if (ret != 0 && src == 0 && dst == 0) { throw std::runtime_error("lz4: truncated frame"); }
    }
    if (out != info.contentSize) { throw std::runtime_error("lz4: wrong content size"); }
    return out;
  }

  static size_t lz4_max_block_size(LZ4F_blockSizeID_t id) {
    switch (id) {
      case LZ4F_max64KB: return size_t(64) << 10;
      case LZ4F_max256KB: return size_t(256) << 10;
      case LZ4F_max1MB: return size_t(1) << 20;
      default: return size_t(4) << 20;
    }
  }

  ZSTD_DCtx *zstd{nullptr};
  LZ4F_dctx *lz4{nullptr};
  std::unique_ptr<char[]> buf;
  size_t cap{0};
};

} // namespace frames_detail

// Calls frame_callback(thread_index, data, length, is_last) for the
// decompressed content of every frame in the file at `path`, using `threads`
// threads. The data is followed by SIMDJSON_PADDING readable bytes and is
// valid only during the call. Returns the total decompressed size.
template <typename FrameCallback>
size_t for_each_frame(const char *path, FrameCallback &&frame_callback, size_t threads, size_t *frames = nullptr,
                      size_t *max_buffer = nullptr) {
  int fd = open(path, O_RDONLY);
  if (fd < 0) { throw std::runtime_error(std::string("cannot open ") + path); }
  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    throw std::runtime_error(std::string("cannot stat ") + path);
  }
  const size_t size = size_t(st.st_size);
  if (size == 0) {
    close(fd);
    return 0;
  }
  // The compressed file is mapped; only the frames' decompressed contents
  // are copied, one frame per thread at a time.
  void *map = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
  close(fd);
  if (map == MAP_FAILED) { throw std::runtime_error(std::string("cannot map ") + path); }
  frames_detail::frame_cursor cursor(static_cast<const uint8_t *>(map), size);
  std::atomic<size_t> bytes{0}, count{0}, max_cap{0};
  std::atomic<bool> failed{false};
  try {
    ndjson_detail::thread_group(threads, [&](size_t t) {
      frames_detail::frame_decoder decoder;
      size_t my_bytes = 0, my_count = 0;
      try {
        frames_detail::frame f;
        while (!failed.load(std::memory_order_relaxed) && cursor.next(f)) {
          size_t n = decoder.decode(f);
          frame_callback(t, decoder.buffer(), n, f.last);
          my_bytes += n;
          my_count++;
        }
      } catch (...) {
        failed = true;
        throw;
      }
      bytes += my_bytes;
      count += my_count;
      size_t prev = max_cap.load();
      while (prev < decoder.capacity() && !max_cap.compare_exchange_weak(prev, decoder.capacity())) {}
    }).join();
  } catch (...) {
    munmap(map, size);
    throw;
  }
  munmap(map, size);
  if (frames) { *frames = count; }
  if (max_buffer) { *max_buffer = max_cap; }
  return bytes;
}

// Processes a multi-frame zstd/lz4 NDJSON file with `threads` threads.
// callback(thread_index, document_reference) is called concurrently.
template <typename Callback>
ndjson_stream_stats for_each_ndjson_document_in_frames(const char *path, Callback &&callback, size_t threads) {
  using namespace simdjson;
  struct alignas(64) thread_state {
    ondemand::parser parser;
    size_t documents{0};
  };
  std::vector<thread_state> state(threads);
  ndjson_stream_stats stats;
  stats.bytes = for_each_frame(
      path,
      [&](size_t t, const char *data, size_t len, bool last) {
        if (!last && len > 0 && data[len - 1] != '\n') {
          throw std::runtime_error("a frame does not end with a newline");
        }
        auto cb = [&](ondemand::document_reference doc) { callback(t, doc); };
        state[t].documents += ndjson_detail::parse_chunk(state[t].parser, data, len, cb);
      },
      threads, &stats.chunks, &stats.max_buffer_size);
  for (auto &s : state) { stats.documents += s.documents; }
  return stats;
}
