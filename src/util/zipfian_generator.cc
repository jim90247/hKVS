#include "util/zipfian_generator.h"

#include <cmath>

ZipfianGenerator::ZipfianGenerator(int n, double alpha, int seed)
    : gen_(seed), distrib_(0, 1), n_(n) {
  double c = 0.0;  // normalization constant
  for (int i = 1; i <= n_; i++) {
    c += 1.0 / pow(static_cast<double>(i), alpha);
  }
  c = 1.0 / c;

  sum_probs_ = new double[n_ + 1];
  sum_probs_[0] = 0;
  for (int i = 1; i <= n_; i++) {
    sum_probs_[i] = sum_probs_[i - 1] + c / pow(static_cast<double>(i), alpha);
  }
}

ZipfianGenerator::~ZipfianGenerator() { delete[] sum_probs_; }

int ZipfianGenerator::GetNumber() {
  double z = distrib_(gen_);

  int l = 0, r = n_;
  while (r - l > 1) {
    int m = (l + r) / 2;
    if (sum_probs_[m] > z) {
      r = m;
    } else {
      l = m;
    }
  }

  return l;
}