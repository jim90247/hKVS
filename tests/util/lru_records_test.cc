#include "util/lru_records.h"

#include <catch2/catch.hpp>

TEMPLATE_TEST_CASE("LRU record (integer types)", "[lru_record]", int, long, unsigned int, unsigned long) {
  LruRecords<TestType> record(3);

  SECTION("empty LRU record should not contain anything") {
    REQUIRE_FALSE(record.Contain(0));
    REQUIRE_FALSE(record.Contain(1));
  }

  SECTION("all items should exist when they fits in the size") {
    record.Put(0);
    record.Put(123);
    record.Put(1);
    record.Put(0);
    REQUIRE(record.Contain(0));
    REQUIRE(record.Contain(123));
    REQUIRE(record.Contain(1));
    REQUIRE_FALSE(record.Contain(2));
  }

  SECTION("old items should be removed when there are too many items") {
    record.Put(1);
    record.Put(2);
    record.Put(1);
    record.Put(3);
    record.Put(4);
    REQUIRE(record.Contain(1));
    REQUIRE(record.Contain(3));
    REQUIRE(record.Contain(4));
    REQUIRE_FALSE(record.Contain(2));
    REQUIRE_FALSE(record.Contain(5));
  }
}
