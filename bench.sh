#!/usr/bin/env bash
# Benchmarks the demo. Usage: ./bench.sh [records]  (run after building in ./build)
# Each configuration runs 5 times; the median throughput (decompressed MB/s) is reported.
set -euo pipefail
N=${1:-5000000}
B=./build
[ -f data.ndjson.gz ] || $B/make_data data.ndjson.gz "$N"
[ -f data.ndjson ] || gzip -dc data.ndjson.gz > data.ndjson
[ -f data.ndjson.zst ] || $B/make_data data.ndjson.zst "$N"   # 256 KiB frames
[ -f data.ndjson.lz4 ] || $B/make_data data.ndjson.lz4 "$N"   # 256 KiB frames
mbs() { grep -oE '\(([0-9]+) MB/s\)' | tr -dc '0-9'; }
median() { sort -n | awk '{a[NR]=$1} END {print a[int((NR+1)/2)]}'; }
run() { # label, command
  local label=$1; shift
  for i in 1 2 3 4 5; do bash -c "$*" | mbs; echo; done | grep . | median | xargs printf "| %-48s | %6s |\n" "$label"
}
echo "| configuration | MB/s |"
echo "|---|---:|"
run "zlib decompression only (no parsing)"   "$B/gz_stream_demo --decompress-only data.ndjson.gz"
for c in 65536 262144 1048576 4194304 16777216; do
  run "gzip file, chunk $((c / 1024)) KiB"   "$B/gz_stream_demo data.ndjson.gz $c"
done
run "uncompressed file, chunk 1024 KiB (parse only)" "$B/gz_stream_demo --raw data.ndjson 1048576"
T=$(nproc)
for t in 1 2 4 8 16 32 64; do
  [ "$t" -le "$T" ] || break
  run "uncompressed file, threads=$t, chunk 64 KiB" "$B/gz_stream_demo --raw --threads $t data.ndjson 65536"
done
for t in 1 2 4; do
  run "gzip file, threads=$t (+1 inflating), chunk 256 KiB" "$B/gz_stream_demo --threads $t data.ndjson.gz 262144"
done
for f in zst lz4; do
  for t in 1 64; do
    run "$f frames, decompression only, threads=$t" "$B/gz_stream_demo --decompress-only --threads $t data.ndjson.$f"
  done
  for t in 1 2 4 8 16 32 64; do
    [ "$t" -le "$T" ] || break
    run "$f frames, threads=$t" "$B/gz_stream_demo --threads $t data.ndjson.$f"
  done
done
run "gzip -dc | demo --raw (2 processes)"     "gzip -dc data.ndjson.gz | $B/gz_stream_demo --raw - 1048576"
if command -v pigz >/dev/null; then
  run "pigz -dc | demo --raw (2+ processes)"  "pigz -dc data.ndjson.gz | $B/gz_stream_demo --raw - 1048576"
fi
