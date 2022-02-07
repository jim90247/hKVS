#include <folly/concurrency/DynamicBoundedQueue.h>
#include <folly/logging/xlog.h>

#include <chrono>
#include <thread>

struct MockCloverRequest {
  unsigned long key;
  void* buf;
  int len;
  unsigned int id;
  unsigned int op;
  unsigned int from;
  unsigned int reply_opt;
};

using ReqQueue = folly::DMPSCQueue<MockCloverRequest, false>;
constexpr unsigned int kWindow = 64;
constexpr unsigned int kNumProducers = 8;
constexpr unsigned long kEnqueuesPerThread = 1'000'000;

void Producer(ReqQueue& req_queue, unsigned int tid) {
  using clock = std::chrono::steady_clock;
  MockCloverRequest req;
  req.from = tid;
  auto start = clock::now();
  for (unsigned long i = 0; i < kEnqueuesPerThread; i++) {
    req.key = i;
    req_queue.enqueue(req);
  }
  auto end = clock::now();
  double sec = std::chrono::duration<double>(end - start).count();
  XLOGF(INFO, "Producer {}: {:.3e} enqueues/s", tid, kEnqueuesPerThread / sec);
}

void Consumer(ReqQueue& req_queue) {
  unsigned long completed = 0;
  MockCloverRequest tmp;
  while (completed < kEnqueuesPerThread * kNumProducers) {
    req_queue.dequeue(tmp);
    completed++;
  }
}

int main() {
  ReqQueue queue(kWindow * kNumProducers);
  std::vector<std::thread> threads;
  for (unsigned int i = 0; i < kNumProducers; i++) {
    threads.emplace_back(Producer, std::ref(queue), i);
  }
  threads.emplace_back(Consumer, std::ref(queue));
  for (auto& t : threads) {
    t.join();
  }
  return 0;
}
