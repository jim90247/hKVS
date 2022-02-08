#include "combined/req_submitter.h"

#include <catch2/catch.hpp>

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

    auto err = handler.TrySubmitRead(key, CloverRequestType::kRead);
    handler.Flush();

    REQUIRE(err == kSuccess);
    REQUIRE(req_queue_ptr->size_approx() == 1);
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
    CloverRequest req;
    REQUIRE(req_queue_ptr->try_dequeue(req));
    REQUIRE(req.key == key);
    REQUIRE(req.buf == wbuf);
    REQUIRE(req.len == kWBufSize);
  }

  SECTION("Auto flush") {
    for (unsigned int i = 0; i < kFlushCutoff - 1; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
    }
    REQUIRE(req_queue_ptr->size_approx() == 0);  // not flushed yet

    handler.TrySubmitRead(kFlushCutoff - 1, CloverRequestType::kRead);
    REQUIRE(req_queue_ptr->size_approx() == kFlushCutoff);
  }

  SECTION("Too many requests") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
    }
    auto err = handler.TrySubmitRead(kCncr, CloverRequestType::kRead);

    REQUIRE(err == kTooManyReqs);
  }

  SECTION("Reclaim") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
      handler.ReclaimSlot(i);
    }
    auto err = handler.TrySubmitRead(kCncr, CloverRequestType::kRead);

    REQUIRE(err == kSuccess);
  }

  SECTION("Reclaim (only representative request)") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
      if (i % kFlushCutoff == kFlushCutoff - 1) {
        handler.ReclaimSlot(i);
      }
    }
    auto err = handler.TrySubmitRead(kCncr, CloverRequestType::kRead);

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

    auto err = handler.TrySubmitRead(key, CloverRequestType::kRead);
    handler.Flush();

    REQUIRE(err == kSuccess);
    REQUIRE(req_queue_ptr->size_approx() == 1);
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
    CloverRequest req;
    REQUIRE(req_queue_ptr->try_dequeue(req));
    REQUIRE(req.key == key);
    REQUIRE(req.buf == wbuf);
    REQUIRE(req.len == kWBufSize);
  }

  SECTION("Auto flush") {
    static_assert(kCncr > CloverRequestQueueHandler::kInternalMaxBatch);

    for (unsigned int i = 0; i < kFlushCutoff - 1; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
    }
    REQUIRE(req_queue_ptr->size_approx() == 0);  // not flushed yet

    handler.TrySubmitRead(kFlushCutoff - 1, CloverRequestType::kRead);
    REQUIRE(req_queue_ptr->size_approx() == kFlushCutoff);
  }

  SECTION("Too many requests") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
    }
    auto err = handler.TrySubmitRead(kCncr, CloverRequestType::kRead);

    REQUIRE(err == kTooManyReqs);
  }

  SECTION("Reclaim") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
      handler.ReclaimSlot(i);
    }
    auto err = handler.TrySubmitRead(kCncr, CloverRequestType::kRead);

    REQUIRE(err == kSuccess);
  }

  SECTION("Reclaim (only representative request)") {
    for (unsigned int i = 0; i < kCncr; i++) {
      handler.TrySubmitRead(i, CloverRequestType::kRead);
      if (i % kFlushCutoff == kFlushCutoff - 1) {
        handler.ReclaimSlot(i);
      }
    }
    auto err = handler.TrySubmitRead(kCncr, CloverRequestType::kRead);

    REQUIRE(err == kSuccess);
  }
}
