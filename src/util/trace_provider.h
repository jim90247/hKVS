#pragma once

using TraceKey = int;

class TraceProvider {
 public:
  /**
   * @brief Returns the next fixed-size key in the trace.
   *
   * @return the key
   */
  virtual TraceKey GetNumber() = 0;
};
