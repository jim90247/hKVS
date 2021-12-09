#include "util/lru_records.h"

#include <catch2/catch.hpp>

TEMPLATE_TEST_CASE("LRU records (integer types)", "[lru_record]", int, long,
                   unsigned int, unsigned long) {
  LruRecords<TestType> records(3);

  SECTION("empty LRU records should not contain anything") {
    REQUIRE_FALSE(records.Contain(0));
    REQUIRE_FALSE(records.Contain(1));
  }

  SECTION("all items should exist when they fits in the size") {
    records.Put(0);
    records.Put(123);
    records.Put(1);
    auto rec = records.Put(0);
    REQUIRE(records.Contain(0));
    REQUIRE(records.Contain(123));
    REQUIRE(records.Contain(1));
    REQUIRE_FALSE(records.Contain(2));
    REQUIRE_FALSE(rec.has_value());
  }

  SECTION("old items should be removed when there are too many items") {
    records.Put(1);
    records.Put(2);
    records.Put(1);
    records.Put(3);
    auto rec = records.Put(4);
    REQUIRE(records.Contain(1));
    REQUIRE(records.Contain(3));
    REQUIRE(records.Contain(4));
    REQUIRE_FALSE(records.Contain(2));
    REQUIRE_FALSE(records.Contain(5));
    REQUIRE(rec.value() == 2);
  }
}
