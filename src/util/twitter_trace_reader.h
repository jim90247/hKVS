#pragma once
#include <string>

#include "csv.hpp"
#include "trace_provider.h"

struct TwttrTraceFilterOption {
  std::vector<std::string> ops;
};

/// A reader for https://github.com/twitter/cache-trace.
class TwttrTraceReader : public TraceProvider {
 public:
  TwttrTraceReader(std::string trace_file, TwttrTraceFilterOption option,
                   size_t max_len = std::numeric_limits<size_t>::max());
  TwttrTraceReader(const TwttrTraceReader&) = delete;
  /**
   * @brief Gets the next fixed-size hashed key.
   *
   * Wraps around if reaching max_len or the end of trace file.
   *
   * @return the fixed-size hashed key
   */
  virtual TraceKey GetNumber() override;

 private:
  size_t idx_;
  std::vector<TraceKey> trace_;
};
