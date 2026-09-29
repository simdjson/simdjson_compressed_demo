# Streaming gzip-compressed NDJSON with simdjson

This demo processes a gzip-compressed NDJSON (JSON Lines) file of any size with
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

## Build and run

Requires CMake and zlib. simdjson v5.0.1 is fetched automatically.

```sh
cmake -B build && cmake --build build
./build/boundary_test                       # exhaustive chunk-boundary tests
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

Run `./bench.sh` after building. It generates the data if needed, runs each
configuration 5 times, and reports the median throughput in MB/s of
*decompressed* NDJSON. The data is 5,000,000 records: 812 MB of NDJSON,
57 MB gzipped (a 14:1 ratio).

Machine: big4, Intel Xeon Gold 6548N (Emerald Rapids, up to 4.1 GHz),
Linux 6.12 (RHEL 10), GCC 14.3, zlib-ng-compat 2.2.3, simdjson 5.0.1.

| configuration | MB/s |
|---|---:|
| zlib decompression only (no parsing) | 2366 |
| gzip file, chunk 64 KiB | 1159 |
| gzip file, chunk 256 KiB | 1211 |
| gzip file, chunk 1024 KiB | 1126 |
| gzip file, chunk 4096 KiB | 1119 |
| gzip file, chunk 16384 KiB | 1125 |
| uncompressed file, chunk 1024 KiB (parse only) | 1830 |
| `gzip -dc \| demo --raw` (2 processes) | 293 |
| `pigz -dc \| demo --raw` (2+ processes) | 790 |

What the numbers show:

- **Streaming gzip + simdjson runs at about 1.1–1.2 GB/s** in a single thread,
  with only a 64 KiB–16 MiB buffer. That is about what you get from running
  decompression (2.4 GB/s) and parsing (1.8 GB/s) one after the other, so
  chunking adds essentially no overhead.
- **Chunk size barely matters.** 64 KiB–256 KiB is as fast as larger chunks,
  because it keeps the working set in cache.
- **Decompressing in-process is 1.5–4x faster than piping.** The `gzip`
  command-line tool (classic zlib) caps a `gzip -dc |` pipeline at about
  300 MB/s. `pigz` does better but is still slower than calling zlib-ng
  directly in the same process.
- Decompression speed depends on the data. These synthetic records are very
  repetitive, which makes inflate fast. Real data with a lower compression
  ratio will decompress more slowly.
