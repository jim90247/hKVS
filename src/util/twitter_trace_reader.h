#pragma once
#include <string>

#include "csv.hpp"

using TwttrHashedKey = int;

/// A reader for https://github.com/twitter/cache-trace.
class TwttrTraceReader {
 public:
  TwttrTraceReader(std::string trace_file,
                   size_t max_len = std::numeric_limits<size_t>::max());
  TwttrTraceReader(const TwttrTraceReader&) = delete;
  /**
   * @brief Gets the next fixed-size hashed key.
   * 
   * Wraps around if reaching max_len or the end of trace file.
   * 
   * @return the fixed-size hashed key
   */
  TwttrHashedKey GetNumber();

 private:
  size_t idx_;
  std::vector<TwttrHashedKey> trace_;
};
