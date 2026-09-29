#pragma once
// Multithreaded NDJSON processing with simdjson.
//
// Each worker thread owns an ondemand::parser and processes whole chunks of
// complete lines (cut after a '\n', as in ndjson_stream.h). Documents are
// therefore handed to the callback out of order and concurrently: the
// callback receives the index of the calling thread so that it can
// accumulate into per-thread state, which the caller merges at the end.
//
// Two entry points:
//
//  - for_each_ndjson_document_in_file: a regular file. Workers claim
//    fixed-size slices of the file, read them with pread() and parse them.
//
//  - for_each_ndjson_document_parallel: any Reader (a gzip stream, a pipe...).
//    The calling thread reads chunks (e.g., decompresses) while the workers
//    parse the previous chunks. Memory use is bounded by
//    (2 * threads + 2) * chunk size.

#include "ndjson_stream.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace ndjson_detail {

// Parses the complete documents in [data, data + len). The bytes in
// [data + len, data + len + SIMDJSON_PADDING) must be readable.
template <typename Callback>
size_t parse_chunk(simdjson::ondemand::parser &parser, const char *data, size_t len, Callback &callback) {
  using namespace simdjson;
  if (len == 0) { return 0; }
  ondemand::document_stream stream;
  auto error = parser.iterate_many(data, len, len).get(stream);
  if (error) { throw std::runtime_error(std::string("iterate_many: ") + error_message(error)); }
  size_t documents = 0;
  for (auto doc : stream) {
    if (doc.error()) { throw std::runtime_error(std::string("bad document: ") + error_message(doc.error())); }
    callback(doc.value_unsafe());
    documents++;
  }
  if (stream.truncated_bytes() != 0) { throw std::runtime_error("incomplete or malformed document"); }
  return documents;
}

// Runs body(thread_index) on `threads` new threads. join() waits for them
// and rethrows the first exception, if any.
class thread_group {
public:
  template <typename Body>
  thread_group(size_t threads, Body body) : errors(threads) {
    for (size_t t = 0; t < threads; t++) {
      pool.emplace_back([this, body, t] {
        try { body(t); } catch (...) { errors[t] = std::current_exception(); }
      });
    }
  }
  ~thread_group() { wait(); }
  void join() {
    wait();
    for (auto &e : errors) {
      if (e) { std::rethrow_exception(e); }
    }
  }

private:
  void wait() {
    for (auto &th : pool) {
      if (th.joinable()) { th.join(); }
    }
  }
  std::vector<std::thread> pool;
  std::vector<std::exception_ptr> errors;
};

} // namespace ndjson_detail

// Processes a regular NDJSON file with `threads` threads. The file is cut
// into slices of about `chunk_size` bytes, which the threads claim one at a
// time and read with pread() into their own buffer. Slice k starts right
// after the first newline at or after offset k * chunk_size - 1 (slice 0
// starts at 0), and ends where slice k + 1 starts. So every thread finds
// its slice's bounds on its own, and the slices tile the file exactly (a
// line longer than chunk_size makes a slice larger and the next one empty).
//
// We use pread() rather than mmap(): copying from the page cache into a
// buffer that stays in cache is cheap, while unmapping a large file is not.
//
// callback(thread_index, document_reference) is called concurrently.
template <typename Callback>
ndjson_stream_stats for_each_ndjson_document_in_file(const char *path, Callback &&callback, size_t threads,
                                                     size_t chunk_size = 1 << 16) {
  using namespace simdjson;
  int fd = open(path, O_RDONLY);
  if (fd < 0) { throw std::runtime_error(std::string("cannot open ") + path); }
  struct stat st;
  if (fstat(fd, &st) != 0) {
    close(fd);
    throw std::runtime_error(std::string("cannot stat ") + path);
  }
  const size_t size = size_t(st.st_size);
  const size_t slices = (size + chunk_size - 1) / chunk_size;
  std::atomic<size_t> next{0};
  std::atomic<size_t> documents{0};
  std::atomic<size_t> chunks{0};
  std::atomic<size_t> max_buffer{0};
  std::atomic<bool> failed{false};

  // Reads up to n bytes at offset; returns fewer only at end of file.
  auto read_at = [fd](char *dst, size_t n, size_t offset) {
    size_t done = 0;
    while (done < n) {
      ssize_t r = pread(fd, dst + done, n - done, off_t(offset + done));
      if (r < 0) { throw std::runtime_error("read error"); }
      if (r == 0) { break; }
      done += size_t(r);
    }
    return done;
  };

  try {
    ndjson_detail::thread_group(threads, [&](size_t t) {
      ondemand::parser parser;
      auto cb = [&](ondemand::document_reference doc) { callback(t, doc); };
      // We read a little past the slice to find where its last line ends.
      size_t capacity = chunk_size + (chunk_size / 16) + 1;
      std::unique_ptr<char[]> buf(new char[capacity + SIMDJSON_PADDING]);
      size_t my_documents = 0, my_chunks = 0;
      try {
        for (size_t k; !failed.load(std::memory_order_relaxed) && (k = next.fetch_add(1)) < slices;) {
          // buf[i] holds the byte at offset base + i.
          const size_t base = k == 0 ? 0 : k * chunk_size - 1;
          const size_t slice_end = (k + 1) * chunk_size - 1; // first offset searched for the end
          size_t filled = read_at(buf.get(), capacity, base);
          size_t begin = 0;
          if (k > 0) {
            const void *nl = memchr(buf.get(), '\n', std::min(filled, slice_end - base));
            if (!nl) { continue; } // no line starts in this slice
            begin = size_t(static_cast<const char *>(nl) - buf.get()) + 1;
          }
          size_t end = filled; // end of file, unless a newline is found
          for (size_t from = slice_end - base; from < filled;) {
            const void *nl = memchr(buf.get() + from, '\n', filled - from);
            if (nl) {
              end = size_t(static_cast<const char *>(nl) - buf.get()) + 1;
              break;
            }
            if (filled < capacity) { break; } // end of file
            // The last line is long: grow the buffer and read more.
            std::unique_ptr<char[]> bigger(new char[2 * capacity + SIMDJSON_PADDING]);
            std::memcpy(bigger.get(), buf.get(), filled);
            buf = std::move(bigger);
            capacity *= 2;
            from = filled;
            filled += read_at(buf.get() + filled, capacity - filled, base + filled);
            end = filled;
          }
          if (begin < end) {
            my_documents += ndjson_detail::parse_chunk(parser, buf.get() + begin, end - begin, cb);
            my_chunks++;
          }
        }
      } catch (...) {
        failed = true;
        throw;
      }
      documents += my_documents;
      chunks += my_chunks;
      size_t prev = max_buffer.load();
      while (prev < capacity && !max_buffer.compare_exchange_weak(prev, capacity)) {}
    }).join();
  } catch (...) {
    close(fd);
    throw;
  }
  close(fd);
  ndjson_stream_stats stats;
  stats.bytes = size;
  stats.documents = documents;
  stats.chunks = chunks;
  stats.max_buffer_size = max_buffer;
  return stats;
}

// Processes an NDJSON stream from any Reader (see ndjson_stream.h) with
// `threads` parsing threads. The calling thread reads chunks, cut after
// their last newline, and queues them; the workers parse them.
//
// callback(thread_index, document_reference) is called concurrently.
template <typename Reader, typename Callback>
ndjson_stream_stats for_each_ndjson_document_parallel(Reader &reader, Callback &&callback, size_t threads,
                                                      size_t chunk_size = 1 << 20) {
  using namespace simdjson;
  struct chunk {
    std::unique_ptr<char[]> data;
    size_t capacity{0};
    size_t len{0}; // complete lines to parse
  };
  std::mutex mutex;
  std::condition_variable cond;
  std::deque<chunk> work, free_chunks;
  bool done = false;   // no more work will be queued
  bool failed = false; // a worker threw
  for (size_t i = 0; i < 2 * threads + 2; i++) {
    free_chunks.push_back({std::unique_ptr<char[]>(new char[chunk_size + SIMDJSON_PADDING]), chunk_size, 0});
  }
  auto get_free = [&]() -> chunk {
    std::unique_lock<std::mutex> lock(mutex);
    cond.wait(lock, [&] { return !free_chunks.empty() || failed; });
    if (failed) { throw std::runtime_error("worker failed"); }
    chunk c = std::move(free_chunks.front());
    free_chunks.pop_front();
    return c;
  };

  ndjson_stream_stats stats;
  std::atomic<size_t> documents{0};
  std::exception_ptr producer_error;

  ndjson_detail::thread_group workers(threads, [&](size_t t) {
    ondemand::parser parser;
    auto cb = [&](ondemand::document_reference doc) { callback(t, doc); };
    size_t my_documents = 0;
    while (true) {
      chunk c;
      {
        std::unique_lock<std::mutex> lock(mutex);
        cond.wait(lock, [&] { return !work.empty() || done || failed; });
        if (failed || work.empty()) { break; }
        c = std::move(work.front());
        work.pop_front();
      }
      try {
        my_documents += ndjson_detail::parse_chunk(parser, c.data.get(), c.len, cb);
      } catch (...) {
        std::lock_guard<std::mutex> lock(mutex);
        failed = true;
        cond.notify_all();
        throw;
      }
      {
        std::lock_guard<std::mutex> lock(mutex);
        free_chunks.push_back(std::move(c));
      }
      cond.notify_all();
    }
    documents += my_documents;
  });

  try {
    chunk current = get_free();
    size_t len = 0; // bytes in current (the carry-over is at the front)
    bool eof = false;
    while (true) {
      while (len < current.capacity && !eof) {
        size_t n = reader.read(current.data.get() + len, current.capacity - len);
        if (n == 0) { eof = true; }
        len += n;
        stats.bytes += n;
      }
      size_t cut = len;
      if (!eof) {
        size_t newline = len;
        while (newline > 0 && current.data[newline - 1] != '\n') { newline--; }
        if (newline == 0) {
          // A single line is longer than the chunk: grow this chunk.
          size_t capacity = 2 * current.capacity;
          std::unique_ptr<char[]> bigger(new char[capacity + SIMDJSON_PADDING]);
          std::memcpy(bigger.get(), current.data.get(), len);
          current.data = std::move(bigger);
          current.capacity = capacity;
          stats.max_buffer_size = std::max(stats.max_buffer_size, capacity);
          continue;
        }
        cut = newline;
      }
      chunk next;
      if (!eof) {
        // Carry the partial last line over to the next chunk before handing
        // this one to a worker (a worker only reads the bytes past `cut`).
        next = get_free();
        len -= cut;
        if (next.capacity < len) {
          next.capacity = current.capacity;
          next.data.reset(new char[next.capacity + SIMDJSON_PADDING]);
        }
        std::memcpy(next.data.get(), current.data.get() + cut, len);
      }
      current.len = cut;
      stats.chunks += cut > 0;
      {
        std::lock_guard<std::mutex> lock(mutex);
        work.push_back(std::move(current));
      }
      cond.notify_one();
      if (eof) { break; }
      current = std::move(next);
    }
  } catch (...) {
    producer_error = std::current_exception();
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    done = true;
    if (producer_error) { failed = true; }
  }
  cond.notify_all();
  workers.join(); // a worker's exception takes precedence
  if (producer_error) { std::rethrow_exception(producer_error); }
  stats.max_buffer_size = std::max(stats.max_buffer_size, chunk_size);
  stats.documents = documents;
  return stats;
}
