#include "twitter_trace_reader.h"

#include <functional>
#include <string_view>

TwttrTraceReader::TwttrTraceReader(std::string trace_file, size_t max_len)
    : idx_(0) {
  size_t idx = 0;
  std::hash<std::string_view> hasher;

  csv::CSVReader reader(trace_file.c_str(), csv::CSVFormat().no_header());
  for (auto& row : reader) {
    trace_.push_back(static_cast<TwttrHashedKey>(hasher(row[1].get_sv())));
    if (++idx >= max_len) {
      break;
    }
  }
}

TwttrHashedKey TwttrTraceReader::GetNumber() {
  auto ret = trace_.at(idx_);
  idx_++;
  if (idx_ == trace_.size()) {
    idx_ = 0;
  }
  return ret;
}
