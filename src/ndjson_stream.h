#pragma once
// Process a huge newline-delimited JSON stream (NDJSON / JSON Lines) that
// arrives as a stream of bytes (e.g., from a gzip decompressor), one bounded
// chunk at a time, with simdjson's iterate_many.
//
// The key question is where to cut each chunk so that no document is split.
// In NDJSON a document never contains a raw newline (newlines inside strings
// must be escaped as \n), so the answer is simple and exact: cut right after
// the last '\n' in the buffer. Everything before the cut is a sequence of
// complete documents that we hand to iterate_many; the partial line after
// the cut is moved to the front of the buffer and completed by the next read.
// We never need to guess from document_stream::truncated_bytes().
//
// Reader requirements: `size_t read(char *dst, size_t n)` returns the number
// of bytes written, and 0 only at end of input.

#include "simdjson.h"

#include <cstring>
#include <memory>
#include <stdexcept>
#include <string>

struct ndjson_stream_stats {
  size_t chunks{0};          // number of calls to iterate_many
  size_t documents{0};       // number of documents processed
  size_t bytes{0};           // decompressed bytes read
  size_t max_buffer_size{0}; // largest buffer we needed
};

template <typename Reader, typename Callback>
ndjson_stream_stats for_each_ndjson_document(Reader &reader, Callback &&callback,
                                             size_t initial_capacity = 1 << 20) {
  using namespace simdjson;
  ndjson_stream_stats stats;
  size_t capacity = initial_capacity;
  // simdjson requires SIMDJSON_PADDING readable bytes past the end of the input.
  std::unique_ptr<char[]> buf(new char[capacity + SIMDJSON_PADDING]);
  size_t len = 0; // bytes currently in buf (the carry-over is at the front)
  bool eof = false;
  ondemand::parser parser;

  while (true) {
    // 1. Top up the buffer.
    while (len < capacity && !eof) {
      size_t n = reader.read(buf.get() + len, capacity - len);
      if (n == 0) { eof = true; }
      len += n;
      stats.bytes += n;
    }

    // 2. Find the cut: after the last newline, or the whole buffer at end of
    //    input (the last line need not end with a newline).
    size_t cut = len;
    if (!eof) {
      size_t newline = len;
      while (newline > 0 && buf[newline - 1] != '\n') { newline--; }
      if (newline == 0) {
        // A single line is longer than the buffer: grow it and read more.
        std::unique_ptr<char[]> bigger(new char[2 * capacity + SIMDJSON_PADDING]);
        std::memcpy(bigger.get(), buf.get(), len);
        buf = std::move(bigger);
        capacity *= 2;
        continue;
      }
      cut = newline;
    }

    // 3. Parse the complete documents in [0, cut).
    if (cut > 0) {
      // Bytes past the cut (the partial next line, then SIMDJSON_PADDING
      // spare bytes) are readable, which is all that simdjson requires.
      ondemand::document_stream stream;
      // batch_size = cut: the whole chunk is indexed in one pass.
      auto error = parser.iterate_many(buf.get(), cut, cut).get(stream);
      if (error) { throw std::runtime_error(std::string("iterate_many: ") + error_message(error)); }
      stats.chunks++;
      for (auto doc : stream) {
        if (doc.error()) { throw std::runtime_error(std::string("bad document: ") + error_message(doc.error())); }
        callback(doc.value_unsafe());
        stats.documents++;
      }
      // All documents are complete by construction: anything left over means
      // the last line of the chunk is not valid JSON.
      if (stream.truncated_bytes() != 0) { throw std::runtime_error("incomplete or malformed document"); }
    }
    if (eof) {
      stats.max_buffer_size = capacity;
      return stats;
    }

    // 4. Carry over the partial last line.
    std::memmove(buf.get(), buf.get() + cut, len - cut);
    len -= cut;
  }
}
