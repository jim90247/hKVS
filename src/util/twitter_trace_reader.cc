#include "twitter_trace_reader.h"

#include <algorithm>
#include <functional>
#include <string_view>

TwttrTraceReader::TwttrTraceReader(std::string trace_file,
                                   TwttrTraceFilterOption option,
                                   size_t max_len)
    : idx_(0) {
  size_t idx = 0;
  std::hash<std::string_view> hasher;

  csv::CSVReader reader(trace_file.c_str(), csv::CSVFormat().no_header());
  for (auto& row : reader) {
    /**
     * Field in row:
     * 1. timestamp
     * 2. anonymized key
     * 3. key size
     * 4. value size
     * 5. client id
     * 6. operation
     * 7. TTL
     */
    if (!option.ops.empty() && std::find(option.ops.begin(), option.ops.end(),
                                         row[5].get()) == option.ops.end()) {
      continue;
    }
    trace_.push_back(static_cast<TraceKey>(hasher(row[1].get_sv())));
    if (++idx >= max_len) {
      break;
    }
  }
}

TraceKey TwttrTraceReader::GetNumber() {
  auto ret = trace_.at(idx_);
  idx_++;
  if (idx_ == trace_.size()) {
    idx_ = 0;
  }
  return ret;
}
