#include <random>

class ZipfianGenerator {
 public:
  /**
   * @brief Constructs a new Zipfian Generator with range 0 ~ n - 1.
   *
   * @param n the range specifier. Range will be [0, n)
   * @param alpha the Zipfian parameter
   * @param seed the random seed
   */
  ZipfianGenerator(int n, double alpha, int seed = 42);
  ZipfianGenerator(const ZipfianGenerator &) = delete;
  ZipfianGenerator &operator=(const ZipfianGenerator &) = delete;
  virtual ~ZipfianGenerator();

  /**
   * @brief Gets a random number.
   * 
   * @return an integer in interval [0, n)
   */
  int GetNumber();

 private:
  std::mt19937_64 gen_;
  std::uniform_real_distribution<double> distrib_;
  const int n_;
  /// Pre-computed prefix sum of probabilities
  double *sum_probs_;
};
