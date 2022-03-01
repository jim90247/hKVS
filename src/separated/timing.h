#pragma once

#include <x86intrin.h>

#include <chrono>
#include <thread>

inline unsigned long Rdtscp() {
  unsigned int foo;
  return __rdtscp(&foo);
}

// Returns cycles per microseconds
inline unsigned long MeasureClockFreq() {
  static unsigned long freq = 0;
  if (freq > 0) {
    return freq;
  }
  auto a = Rdtscp();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  auto b = Rdtscp();
  freq = (b - a) / 1'000'000;
  return freq;
}
