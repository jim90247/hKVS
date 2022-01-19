#include <gflags/gflags.h>
#include <glog/logging.h>
#include <moodycamel/concurrentqueue.h>

#include <chrono>
#include <forward_list>
#include <thread>
#include <vector>

#include "combined/clover_worker.h"

DEFINE_int32(producer, 1, "number of producer threads");
DEFINE_int32(consumer, 1, "number of consumer threads");
DEFINE_int32(batch, 1, "batch size");
DEFINE_int32(batch_consumer, 1, "batch size of consumer threads");
DEFINE_bool(reply, true, "need reply");
DEFINE_int32(concurrent_batch, 1, "max concurrent batches");
DEFINE_int32(spin_cycle, 0, "spin cycle");

void bar() {
  thread_local static volatile int foo = 0;
  while (++foo < FLAGS_spin_cycle)
    ;
  foo = 0;
}

void ProducerMain(SharedRequestQueue& req_queue,
                  SharedResponseQueuePtr resp_queue_ptr, int id) {
  using clock = std::chrono::steady_clock;
  std::vector<CloverRequest> reqbuf(FLAGS_batch);
  CloverResponse* respbuf = new CloverResponse[FLAGS_concurrent_batch];
  moodycamel::ProducerToken ptok(req_queue);
  long iterations = 0;
  auto start = clock::now();
  int concurrent_batch = 0;

  while (true) {
    if (iterations >= 4 << 20) {
      auto end = clock::now();
      double sec = std::chrono::duration<double>(end - start).count();
      LOG(INFO) << "Producer " << id << ", " << iterations / sec << " op/s";
      iterations = 0;
      start = clock::now();
    }
    for (auto& req : reqbuf) {
      req.from = id;
      req.reply_opt = CloverReplyOption::kNever;
    }
    if (FLAGS_reply) {
      reqbuf.back().reply_opt = CloverReplyOption::kAlways;
    }

    while (!req_queue.try_enqueue_bulk(
        ptok, std::make_move_iterator(reqbuf.begin()), FLAGS_batch))
      ;
    concurrent_batch++;
    if (concurrent_batch == FLAGS_concurrent_batch) {
      int comps = 0;
      if (FLAGS_reply) {
        while (comps == 0) {
          comps +=
              resp_queue_ptr->try_dequeue_bulk(respbuf, FLAGS_concurrent_batch);
        }
        for (int i = 0; i < comps; i++) {
          CHECK_EQ(respbuf[i].rc, MITSUME_SUCCESS);
        }
      } else {
        comps = 1;
      }
      concurrent_batch -= comps;
    }
    iterations += FLAGS_batch;
  }
  delete[] respbuf;
}

void ConsumerMain(SharedRequestQueue& req_queue,
                  const std::vector<SharedResponseQueuePtr>& resp_queues) {
  std::forward_list<CloverRequest> reqs(FLAGS_batch_consumer);
  moodycamel::ConsumerToken ctok(req_queue);
  while (true) {
    reqs.resize(FLAGS_batch_consumer);
    int new_reqs =
        req_queue.try_dequeue_bulk(ctok, reqs.begin(), FLAGS_batch_consumer);
    reqs.resize(new_reqs);
    for (auto& req : reqs) {
      CloverResponse resp{req.key, req.id, req.op, MITSUME_SUCCESS};
      bar();
      if (req.reply_opt == CloverReplyOption::kAlways) {
        while (!resp_queues.at(req.from)->try_enqueue(resp))
          ;
      }
    }
  }
}

int main(int argc, char** argv) {
  FLAGS_colorlogtostderr = true;
  FLAGS_logtostderr = true;
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  SharedRequestQueue req_queue(FLAGS_concurrent_batch * FLAGS_batch,
                               FLAGS_producer, 0);
  std::vector<SharedResponseQueuePtr> resp_queues;
  for (int i = 0; i < FLAGS_producer; i++) {
    resp_queues.emplace_back(std::make_shared<SharedResponseQueue>(
        FLAGS_concurrent_batch * FLAGS_batch, 0, FLAGS_consumer));
  }

  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_producer; i++) {
    threads.emplace_back(
        std::thread(ProducerMain, std::ref(req_queue), resp_queues.at(i), i));
  }
  for (int i = 0; i < FLAGS_consumer; i++) {
    threads.emplace_back(
        std::thread(ConsumerMain, std::ref(req_queue), std::cref(resp_queues)));
  }

  for (auto& t : threads) {
    t.join();
  }

  return 0;
}
