#pragma once
// Synthetic records shared by the data generator and the tests, plus the
// "query" we answer over them.

#include "simdjson.h"

#include <cstdint>
#include <random>
#include <string>

// The query: how many records, how many are active, and the sum of the
// scores of the active records whose user has the "admin" tag.
struct query_result {
  uint64_t count{0};
  uint64_t active{0};
  int64_t admin_score_sum{0};
  bool operator==(const query_result &o) const {
    return count == o.count && active == o.active && admin_score_sum == o.admin_score_sum;
  }
};

// Appends one record to `out` and updates the expected answer.
// Strings deliberately contain "}", "]", escaped quotes and escaped
// newlines (\n): only raw newlines separate documents.
inline void append_record(std::string &out, uint64_t id, std::mt19937_64 &rng, query_result &expected) {
  bool active = rng() % 3 != 0;
  bool admin = rng() % 5 == 0;
  int64_t score = int64_t(rng() % 20001) - 10000;
  size_t note_len = rng() % 64;
  out += "{\"id\":" + std::to_string(id);
  out += ",\"active\":";
  out += active ? "true" : "false";
  out += ",\"user\":{\"name\":\"user_" + std::to_string(id) + "\",\"tags\":[";
  out += admin ? "\"staff\",\"admin\"" : "\"guest\"";
  out += "]},\"score\":" + std::to_string(score);
  out += ",\"note\":\"tricky },{ \\\"quoted\\\" ]\\n{\\n ";
  out.append(note_len, 'x');
  out += "\"}";
  expected.count++;
  if (active) {
    expected.active++;
    if (admin) { expected.admin_score_sum += score; }
  }
}

// Builds an NDJSON string: one record per line. Some lines use CRLF,
// some have leading/trailing spaces, and there are a few blank lines.
inline std::string make_ndjson(size_t n, uint64_t seed, query_result &expected) {
  std::mt19937_64 rng(seed);
  std::string out;
  for (size_t i = 0; i < n; i++) {
    if (rng() % 7 == 0) { out += "  "; }
    append_record(out, i, rng, expected);
    switch (rng() % 8) {
      case 0: out += "\r\n"; break;
      case 1: out += " \n\n"; break;
      default: out += "\n";
    }
  }
  return out;
}

// Answers the query for one document, with On-Demand.
inline void accumulate(simdjson::ondemand::document_reference doc, query_result &r) {
  using namespace simdjson;
  ondemand::object obj = doc.get_object();
  bool active = false;
  bool admin = false;
  int64_t score = 0;
  // Fields are visited in document order, once each: the fastest way with On-Demand.
  for (auto field : obj) {
    std::string_view key = field.unescaped_key();
    if (key == "active") {
      active = field.value().get_bool();
    } else if (key == "user") {
      ondemand::array tags = field.value()["tags"].get_array();
      for (auto tag : tags) {
        if (std::string_view(tag.get_string()) == "admin") { admin = true; }
      }
    } else if (key == "score") {
      score = field.value().get_int64();
    }
  }
  r.count++;
  if (active) {
    r.active++;
    if (admin) { r.admin_score_sum += score; }
  }
}
