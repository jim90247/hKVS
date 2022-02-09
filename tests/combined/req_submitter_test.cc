#include "combined/req_submitter.h"

#include <catch2/catch.hpp>
#include <random>
#include <vector>

class MockCloverWorker {
 public:
  MockCloverWorker(SharedRequestQueuePtr req_queue_ptr,
                   std::vector<SharedResponseQueuePtr> resp_queues)
      : req_queue_ptr_(req_queue_ptr),
        resp_queue_ptrs_(resp_queues),
        gen_(42),
        dis_(0, 9) {}

  void Process() {
    CloverRequest req;
    while (req_queue_ptr_->try_dequeue(req)) {
      bool reply;
      switch (req.reply_opt) {
        case CloverReplyOption::kOnFailure:
          reply = dis_(gen_) == 0;
          break;
        case CloverReplyOption::kAlways:
          reply = true;
          break;
        case CloverReplyOption::kNever:
          reply = false;
          break;
        default:
          reply = false;
          break;
      }
      if (reply) {
        CloverResponse resp;
        resp.key = req.key;
        resp.id = req.id;
        resp.rc = 0;
        resp_queue_ptrs_.at(req.from)->enqueue(resp);
      }
    }
  }

 private:
  SharedRequestQueuePtr req_queue_ptr_;
  std::vector<SharedResponseQueuePtr> resp_queue_ptrs_;

  std::default_random_engine gen_;
  std::uniform_int_distribution<unsigned int> dis_;
};

TEST_CASE("Clover request queue handler (small concurrency)",
          "[req_submitter]") {
  constexpr int kThreadId = 0;
  constexpr int kCncr = 2;
  constexpr unsigned int kFlushCutoff = kCncr;
  constexpr size_t kWBufSize = 1024;
  constexpr size_t kQueueSize = 1000;
  char wbuf[kWBufSize] = {'\0'};
  auto req_queue_ptr = std::make_shared<SharedRequestQueue>(kQueueSize, 1, 0);
  CloverRequestQueueHandler handler(req_queue_ptr, kCncr, kThreadId);

  SECTION("Enqueue one read") {
    mitsume_key key = 1;

    auto err = handler.TrySubmitRead(key);
    handler.Flush();

    REQUIRE(err == kSuccess);
    REQUIRE(req_queue_ptr->size_approx() == 1);
    REQUIRE(handler.GetBuffered() == 0);
    REQUIRE(handler.GetPending() == 1);
    CloverRequest req;
    REQUIRE(req_queue_ptr->try_dequeue(req));
    REQUIRE(req.key == key);
    REQUIRE(req.len == CloverRequestQueueHandler::kReadBufLen);
  }

  SECTION("Enqueue one write") {
    mitsume_key key = 2;

    auto err =
        handler.TrySubmitWrite(key, CloverRequestType::kWrite, wbuf, kWBufSize);
    handler.Flush();

    REQUIRE(err == kSuccess);
    REQUIRE(req_queue_ptr->size_approx() == 1);
    REQUIRE(handler.GetBuffered() == 0);
    REQUIRE(handler.GetPending() == 1);
    CloverRequest req;
    REQUIRE(req_queue_ptr->try_dequeue(req));
    REQUIRE(req.key == key);
    REQUIRE(req.buf == wbuf);
    REQUIRE(req.len == kWBufSize);
  }

  SECTION("Auto flush") {
    for (unsigned int i = 0; i < kFlushCutoff - 1; i++) {
      handler.TrySubmitRead(i);
    }
    REQUIRE(req_queue_ptr->size_approx() == 0);  // not flushed yet
    REQUIRE(handler.GetBuffered() == kFlushCutoff - 1);

    handler.TrySubmitRead(kFlushCutoff - 1);
    REQUIRE(req_queue_ptr->size_approx() == kFlushCutoff);
    REQUIRE(handler.GetBuffered() == 0);
    REQUIRE(handler.GetPending() == kFlushCutoff);
  }

  SECTION("Too many requests") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i);
    }
    auto err = handler.TrySubmitRead(kCncr);

    REQUIRE(err == kTooManyReqs);
  }

  SECTION("Reclaim") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i);
      handler.ReclaimSlot(i);
    }
    auto err = handler.TrySubmitRead(kCncr);

    REQUIRE(err == kSuccess);
    REQUIRE(handler.GetBuffered() == 1);
    REQUIRE(handler.GetPending() == 0);
  }

  SECTION("Reclaim (only representative request)") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i);
      if (i % kFlushCutoff == kFlushCutoff - 1) {
        handler.ReclaimSlot(i);
      }
    }
    auto err = handler.TrySubmitRead(kCncr);

    REQUIRE(err == kSuccess);
  }
}

TEST_CASE("Clover request queue handler (large concurrency)",
          "[req_submitter]") {
  constexpr int kThreadId = 0;
  constexpr int kCncr = 128;
  constexpr unsigned int kFlushCutoff =
      CloverRequestQueueHandler::kInternalMaxBatch;
  constexpr size_t kWBufSize = 1024;
  constexpr size_t kQueueSize = 1000;
  char wbuf[kWBufSize] = {'\0'};
  auto req_queue_ptr = std::make_shared<SharedRequestQueue>(kQueueSize, 1, 0);
  CloverRequestQueueHandler handler(req_queue_ptr, kCncr, kThreadId);

  SECTION("Enqueue one read") {
    mitsume_key key = 1;

    auto err = handler.TrySubmitRead(key);
    handler.Flush();

    REQUIRE(err == kSuccess);
    REQUIRE(req_queue_ptr->size_approx() == 1);
    REQUIRE(handler.GetBuffered() == 0);
    REQUIRE(handler.GetPending() == 1);
    CloverRequest req;
    REQUIRE(req_queue_ptr->try_dequeue(req));
    REQUIRE(req.key == key);
    REQUIRE(req.len == CloverRequestQueueHandler::kReadBufLen);
  }

  SECTION("Enqueue one write") {
    mitsume_key key = 2;

    auto err =
        handler.TrySubmitWrite(key, CloverRequestType::kWrite, wbuf, kWBufSize);
    handler.Flush();

    REQUIRE(err == kSuccess);
    REQUIRE(req_queue_ptr->size_approx() == 1);
    REQUIRE(handler.GetBuffered() == 0);
    REQUIRE(handler.GetPending() == 1);
    CloverRequest req;
    REQUIRE(req_queue_ptr->try_dequeue(req));
    REQUIRE(req.key == key);
    REQUIRE(req.buf == wbuf);
    REQUIRE(req.len == kWBufSize);
  }

  SECTION("Auto flush") {
    static_assert(kCncr > CloverRequestQueueHandler::kInternalMaxBatch);

    for (unsigned int i = 0; i < kFlushCutoff - 1; i++) {
      handler.TrySubmitRead(i);
    }
    REQUIRE(req_queue_ptr->size_approx() == 0);  // not flushed yet
    REQUIRE(handler.GetBuffered() == kFlushCutoff - 1);

    handler.TrySubmitRead(kFlushCutoff - 1);
    REQUIRE(req_queue_ptr->size_approx() == kFlushCutoff);
    REQUIRE(handler.GetBuffered() == 0);
    REQUIRE(handler.GetPending() == kFlushCutoff);
  }

  SECTION("Too many requests") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i);
    }
    auto err = handler.TrySubmitRead(kCncr);

    REQUIRE(err == kTooManyReqs);
  }

  SECTION("Reclaim") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i);
      handler.ReclaimSlot(i);
    }
    auto err = handler.TrySubmitRead(kCncr);

    REQUIRE(err == kSuccess);
    REQUIRE(handler.GetBuffered() == 1);
    REQUIRE(handler.GetPending() == 0);
  }

  SECTION("Reclaim (only representative request)") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i);
      if (i % kFlushCutoff == kFlushCutoff - 1) {
        handler.ReclaimSlot(i);
      }
    }
    auto err = handler.TrySubmitRead(kCncr);

    REQUIRE(err == kSuccess);
  }
}

TEST_CASE("Clover request submitter", "[req_submitter]") {
  constexpr unsigned int kProducer = 2;
  constexpr unsigned int kConsumer = 2;
  constexpr unsigned int kCncr = 2;
  constexpr size_t kWBufSize = 1024;
  constexpr size_t kQueueSize = 1000;
  char wbuf[kWBufSize] = {'\0'};
  std::vector<SharedRequestQueuePtr> req_queue_ptrs;
  std::vector<SharedResponseQueuePtr> resp_queue_ptrs;
  std::vector<MockCloverWorker> clover_workers;
  std::vector<CloverRequestSubmitter> submitters;
  for (unsigned int i = 0; i < kProducer; i++) {
    resp_queue_ptrs.push_back(
        std::make_shared<SharedResponseQueue>(kQueueSize, 0, 2));
  }
  for (unsigned int i = 0; i < kConsumer; i++) {
    req_queue_ptrs.push_back(
        std::make_shared<SharedRequestQueue>(kQueueSize, 1, 0));
    clover_workers.emplace_back(req_queue_ptrs[i], resp_queue_ptrs);
  }
  for (unsigned int i = 0; i < kProducer; i++) {
    submitters.emplace_back(kCncr, req_queue_ptrs, resp_queue_ptrs[i], i);
  }

  auto process_all = [&]() {
    for (auto& worker : clover_workers) {
      worker.Process();
    }
  };

  SECTION("Enqueue one read and one write to different queues") {
    submitters[0].TrySubmitRead(0);
    submitters[0].TrySubmitWrite(1, CloverRequestType::kWrite, wbuf, kWBufSize);
    submitters[0].Flush();
    submitters[1].TrySubmitRead(2);
    submitters[1].TrySubmitWrite(3, CloverRequestType::kWrite, wbuf, kWBufSize);
    submitters[1].Flush();
    REQUIRE(req_queue_ptrs[0]->size_approx() == 2);
    REQUIRE(req_queue_ptrs[1]->size_approx() == 2);
    REQUIRE(submitters[0].GetPending() == 2);
    REQUIRE(submitters[1].GetPending() == 2);
    process_all();
    auto resps_0 = submitters[0].GetResponses();
    auto resps_1 = submitters[1].GetResponses();
    REQUIRE(resps_0.size() == 2);
    REQUIRE(resps_1.size() == 2);
    REQUIRE(submitters[0].GetPending() == 0);
    REQUIRE(submitters[1].GetPending() == 0);
  }

  SECTION("Auto flush") {
    constexpr unsigned int kNumRequests = 4 * kCncr;

    for (unsigned int i = 0; i < kNumRequests; i++) {
      auto err = submitters[0].TrySubmitRead(i);
      if (err == kTooManyReqs) {
        process_all();
        auto resps = submitters[0].GetResponses();
        REQUIRE(resps.size() > 0);
        err = submitters[0].TrySubmitRead(i);
      }
      REQUIRE(err == kSuccess);
    }
  }
}
