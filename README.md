# Streaming gcompressed NDJSON with simdjson

This demo processes a compressed NDJSON (JSON Lines) file of any size with
bounded memory. It decompresses a chunk, parses the complete documents in that
chunk with simdjson's `iterate_many`, and carries the partial last line over
to the next chunk. It never decompresses the whole file in memory, and it
never needs `truncated_bytes()`.

## How documents are kept whole

In NDJSON a document never contains a raw newline (a newline inside a string
must be escaped as `\n`). So a chunk can always be cut **right after its last
`'\n'`**:

```
buffer:  {"id":1,...}\n{"id":2,...}\n{"id":3,"na
                                     ^ cut
         [ complete docs -> iterate_many ][ carry over ]
```

Because the cut is exact, a chunk boundary can fall anywhere (inside a
string, between `}` and `\n`, inside `\r\n`, inside a number) and nothing
goes wrong. If a single line is larger than the buffer, the buffer doubles
in size. At end of input, the rest of the buffer is parsed, so the last line
does not need a trailing newline.

The logic is in `src/ndjson_stream.h` (about 100 lines). It works with any
reader that has a `size_t read(char*, size_t)` method. `src/gzip_reader.h`
is a small zlib wrapper that handles files, stdin and multi-member gzip
(for example, output from `pigz`).

## The simdjson code

Here is a sketch of the main loop in `src/ndjson_stream.h`, without the error
handling and statistics:

```cpp
simdjson::ondemand::parser parser;               // reused for every chunk
std::unique_ptr<char[]> buf(new char[capacity + simdjson::SIMDJSON_PADDING]);
size_t len = 0;                                  // carry-over sits at the front
bool eof = false;

while (true) {
  // 1. Fill the buffer with decompressed bytes.
  while (len < capacity && !eof) {
    size_t n = reader.read(buf.get() + len, capacity - len);
    eof = (n == 0);
    len += n;
  }
  // 2. Cut after the last '\n' (or take everything at end of input).
  size_t cut = eof ? len : last_newline(buf.get(), len) + 1;
  //    (no newline at all: double the buffer and go back to step 1)

  // 3. Parse the complete documents in [0, cut).
  simdjson::ondemand::document_stream stream;
  parser.iterate_many(buf.get(), cut, /*batch_size=*/cut).get(stream);
  for (auto doc : stream) {
    callback(doc.value_unsafe());                // one NDJSON line
  }
  if (eof) { break; }

  // 4. Move the partial last line to the front.
  std::memmove(buf.get(), buf.get() + cut, len - cut);
  len -= cut;
}
```

Things to know:

- **Padding.** simdjson reads up to `SIMDJSON_PADDING` bytes past the end of
  its input, so the buffer is allocated with that much extra space. Whatever
  is in those bytes (here, the start of the carried-over line) is ignored.
  Nothing is copied or zeroed.
- **`batch_size = cut`.** Each chunk is indexed in one pass. The batch size
  must be at least as large as the largest document, and a chunk always is.
- **One parser for the whole stream.** Its memory is allocated once and then
  reused for every chunk.
- **`truncated_bytes()` is only a sanity check.** Since every chunk ends on a
  newline, it must be 0 after the loop. If it isn't, the last line is not
  valid JSON and the code reports an error.
- **Lifetimes.** A `document_reference`, and any `std::string_view` you got
  from it, is only valid until the stream moves to the next document. Copy
  anything you need to keep.

Each document is handled with On-Demand (`accumulate` in `src/records.h`).
The code visits the fields once, in the order they appear, and parses only
the values it needs:

```cpp
void accumulate(simdjson::ondemand::document_reference doc, query_result &r) {
  bool active = false, admin = false;
  int64_t score = 0;
  for (auto field : doc.get_object()) {
    std::string_view key = field.unescaped_key();
    if (key == "active") {
      active = field.value().get_bool();
    } else if (key == "user") {
      for (auto tag : field.value()["tags"].get_array()) {
        if (std::string_view(tag.get_string()) == "admin") { admin = true; }
      }
    } else if (key == "score") {
      score = field.value().get_int64();
    }                                            // other fields ("note", ...) are skipped
  }
  r.count++;
  if (active) { r.active++; if (admin) { r.admin_score_sum += score; } }
}
```

## Multithreading

`src/parallel_ndjson.h` has two multithreaded versions. Each thread has its
own parser and parses whole chunks, cut after a newline as above. So the
callback is called concurrently and out of order. It gets the thread's index,
so each thread can add to its own result, and the results are added
up at the end:

- **`for_each_ndjson_document_in_file(path, callback, threads, chunk)`** for
  a regular file. Slice *k* of the file starts just after the first newline
  at or after offset *k* × `chunk` − 1 and ends where slice *k*+1 starts.
  Each thread can find these bounds without talking to the others, and the
  slices cover the file exactly, even when a line is longer than a slice.
  The threads take slices from an atomic counter, read each one with
  `pread()` into their own padded buffer, and parse it.
  We tried `mmap()` first, and it was slower: unmapping an 812 MB file took
  18 ms, about half of the total time at 64 threads.
  Copying a 64 KiB slice into a buffer that stays in L2 costs little.
- **`for_each_ndjson_document_parallel(reader, callback, threads, chunk)`**
  for any reader (gzip, a pipe). The calling thread reads or decompresses
  chunks and puts them in a queue. The parsing threads take them from the
  queue. Memory is bounded: (2 × `threads` + 2) chunk buffers.

```sh
./build/gz_stream_demo --raw --threads 64 data.ndjson   # 64 threads, 64 KiB slices
./build/gz_stream_demo --threads 2 data.ndjson.gz       # 1 thread inflates, 2 parse
```

## zstd and lz4 in many frames: parallel decompression

A gzip stream must be decompressed from start to end, so a gzip file can
use at most one decompressing thread. zstd and lz4 files may instead be
made of many independent *frames*, one after the other. The file is still an
ordinary `.zst` or `.lz4` file: `zstd -d` and `lz4 -d` decompress it
as usual. `make_data` writes such files (`src/frame_writer.h`):

- each frame holds about 256 KiB of whole lines, so it ends with a newline;
- each frame records its decompressed size and a checksum of its content
  (like gzip's CRC-32).

`for_each_ndjson_document_in_frames(path, callback, threads)` in
`src/parallel_frames.h` maps the compressed file in memory. The threads take
the frames one at a time. Finding where the next frame ends only requires
reading block headers (`ZSTD_findFrameCompressedSize`, or a short walk over
the lz4 block headers), not decompressing. Each thread decompresses a frame
into its own buffer and parses it with its own parser. The reader checks
that every frame but the last ends with a newline. It skips skippable frames,
accepts zstd and lz4 frames in the same file, and reports corrupted or
truncated input with an exception. `src/frames_test.cpp` tests all of this
(every frame size from one line per frame to a single frame, 1, 3 and 8
threads, and bad input).

```sh
./build/make_data data.ndjson.zst 5000000       # zstd, 256 KiB frames (optional 3rd arg: frame size)
./build/make_data data.ndjson.lz4 5000000       # lz4, 256 KiB frames
./build/gz_stream_demo --threads 64 data.ndjson.zst                    # format detected from the file
./build/gz_stream_demo --decompress-only --threads 64 data.ndjson.lz4  # decompression alone
```

## Build and run

Requires CMake and zlib. simdjson v5.0.1, zstd v1.5.7 and lz4 v1.10.0 are
fetched and built automatically.

```sh
cmake -B build && cmake --build build
./build/boundary_test                       # exhaustive chunk-boundary tests
./build/frames_test                         # multi-frame zstd and lz4 tests
./build/make_data data.ndjson.gz 5000000    # 812 MB of NDJSON, 57 MB gzipped
./build/gz_stream_demo data.ndjson.gz       # optional 2nd arg: chunk size in bytes (default 1 MiB)
./build/gz_stream_demo - < data.ndjson.gz   # gzip on stdin
gzip -dc data.ndjson.gz | ./build/gz_stream_demo --raw -   # decompressed stdin
./build/gz_stream_demo --decompress-only data.ndjson.gz     # zlib alone, no parsing
./bench.sh                                  # benchmarks (see below)
```

The query counts the records and the active records, and sums `score` over
active records whose `user.tags` contains `"admin"`.
`make_data` prints the expected answer so you can check the demo's output.

## Benchmarks


See 
* Daniel Lemire, "Parsing compressed JSON at 40 GB/s," in Daniel Lemire's blog, October 1, 2026, https://lemire.me/blog/2026/10/01/parsing-compressed-json-at-40-gb-s/.
