#pragma once
// Compresses NDJSON as a sequence of independent zstd or lz4 frames. Each
// frame holds whole lines, so the frames can be decompressed and parsed in
// parallel (see parallel_frames.h). The output is an ordinary .zst or .lz4
// file: `zstd -d` and `lz4 -d` decompress concatenated frames.
//
// Every frame records its decompressed size in its header, which the
// reader uses to size its buffer, and a checksum of its content (like the
// CRC-32 in gzip), which the decompressor verifies.

#include <lz4frame.h>
#include <zstd.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <string_view>

enum class frame_format { zstd, lz4 };

// Appends one frame holding `data` to `out`.
inline void append_frame(std::string &out, frame_format format, std::string_view data, int zstd_level = 3) {
  size_t old_size = out.size();
  if (format == frame_format::zstd) {
    out.resize(old_size + ZSTD_compressBound(data.size()));
    // The content size goes in the frame header (the default).
    thread_local std::unique_ptr<ZSTD_CCtx, size_t (*)(ZSTD_CCtx *)> cctx(ZSTD_createCCtx(), ZSTD_freeCCtx);
    ZSTD_CCtx_reset(cctx.get(), ZSTD_reset_session_and_parameters);
    ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, zstd_level);
    ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_checksumFlag, 1);
    size_t n = ZSTD_compress2(cctx.get(), &out[old_size], out.size() - old_size, data.data(), data.size());
    if (ZSTD_isError(n)) { throw std::runtime_error(std::string("zstd: ") + ZSTD_getErrorName(n)); }
    out.resize(old_size + n);
  } else {
    LZ4F_preferences_t prefs = LZ4F_INIT_PREFERENCES;
    prefs.frameInfo.contentSize = data.size();
    prefs.frameInfo.blockSizeID = LZ4F_max4MB;
    prefs.frameInfo.blockMode = LZ4F_blockIndependent;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    out.resize(old_size + LZ4F_compressFrameBound(data.size(), &prefs));
    size_t n = LZ4F_compressFrame(&out[old_size], out.size() - old_size, data.data(), data.size(), &prefs);
    if (LZ4F_isError(n)) { throw std::runtime_error(std::string("lz4: ") + LZ4F_getErrorName(n)); }
    out.resize(old_size + n);
  }
}

// Compresses `ndjson` into frames of about `frame_bytes` bytes each (at
// least one line each), always cut after a newline.
inline std::string compress_frames(std::string_view ndjson, frame_format format, size_t frame_bytes) {
  std::string out;
  size_t start = 0;
  while (start < ndjson.size()) {
    size_t end = ndjson.size();
    if (ndjson.size() - start > frame_bytes) {
      size_t nl = ndjson.find('\n', start + frame_bytes - (frame_bytes > 0));
      if (nl != std::string_view::npos) { end = nl + 1; }
    }
    append_frame(out, format, ndjson.substr(start, end - start));
    start = end;
  }
  return out;
}
