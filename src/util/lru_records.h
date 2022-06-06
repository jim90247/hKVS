#pragma once
#include <folly/container/F14Map.h>

#include <list>
#include <optional>
#include <queue>

template <typename Record>
class LruRecords {
 public:
  typedef typename std::list<Record>::iterator ListIterator;
  /**
   * @brief Constructs an empty LRU Records object with a fixed capacity.
   *
   * @param cap the capacity
   */
  LruRecords(size_t cap) : capacity_(cap) {}
  LruRecords(const LruRecords &) = delete;
  LruRecords &operator=(const LruRecords &) = delete;
  /**
   * @brief Inserts a record into the LRU record. May remove the oldest record
   * to meet the capacity restriction.
   *
   * @param record the record
   * @return the removed record
   */
  virtual inline std::optional<Record> Put(const Record &record) {
    items_.push_front(record);
    auto it = map_.find(record);
    if (it != map_.end()) {
      // remove old copy
      items_.erase(it->second);
      it->second = items_.begin();
    } else {
      map_[record] = items_.begin();
    }
    if (items_.size() > capacity_) {
      ListIterator last = std::prev(items_.end());
      Record popped = *last;
      map_.erase(*last);
      items_.pop_back();
      return std::make_optional(popped);
    } else {
      return std::nullopt;
    }
  }
  /**
   * @brief Checks if the record exists in the LRU Records.
   *
   * @param record the record
   * @return true if the record exists
   */
  virtual inline bool Contain(const Record &record) const {
    return map_.find(record) != map_.end();
  }

  virtual inline size_t EstimatedMemoryUsage() const {
    auto map_size =
        static_cast<size_t>(map_.bucket_count() * map_.max_load_factor()) *
        (sizeof(Record) + sizeof(ListIterator));
    auto items_size = items_.size() * sizeof(Record);
    return map_size + items_size;
  }

 private:
  const size_t capacity_;
  folly::F14FastMap<Record, ListIterator> map_;
  std::list<Record> items_;
};

/**
 * @brief An extended LRU record with an additional constraint: records are
 * included only if they appear in a sliding window multiple times.
 *
 * @tparam T the LRU record type
 */
template <typename T>
class LruRecordsWithMinCount : public LruRecords<T> {
 public:
  /**
   * @brief Construct an empty LRU Records With a fixed capacity and threshold
   * for minimum occurrences in a sliding window.
   *
   * @param cap the LRU record capacity
   * @param win the size of the sliding window
   * @param min_cnt the minimum occurrences in a sliding window
   */
  LruRecordsWithMinCount(size_t cap, size_t win, int min_cnt)
      : LruRecords<T>(cap), window_size_(win), min_count_(min_cnt) {}
  LruRecordsWithMinCount(const LruRecordsWithMinCount &) = delete;
  LruRecordsWithMinCount &operator=(const LruRecordsWithMinCount &) = delete;
  /**
   * @brief Inserts a record into the LRU record. May remove the oldest record
   * to meet the capacity restriction. A record will be actually inserted only
   * if it appears not less than min_cnt times in current window.
   *
   * @param record the record
   * @return the removed record
   */
  virtual inline std::optional<T> Put(const T &record) override {
    window_.push(record);
    IncrementAppearance(record);
    if (window_.size() >= window_size_) {
      DecrementAppearance(window_.front());
      window_.pop();
    }

    if (counts_[record] >= min_count_) {
      return LruRecords<T>::Put(record);
    } else {
      return std::nullopt;
    }
  }

  virtual inline size_t EstimatedMemoryUsage() const override {
    auto counts_size = static_cast<size_t>(counts_.bucket_count() *
                                           counts_.max_load_factor()) *
                       (sizeof(T) + sizeof(int));
    auto window_size = window_.size() * sizeof(T);
    return counts_size + window_size + LruRecords<T>::EstimatedMemoryUsage();
  }

 private:
  const size_t window_size_;
  const int min_count_;
  folly::F14FastMap<T, int> counts_;
  std::queue<T> window_;

  void IncrementAppearance(const T &key) {
    auto it = counts_.find(key);
    if (it == counts_.end()) {
      counts_[key] = 1;
    } else {
      it->second++;
    }
  }

  void DecrementAppearance(const T &key) {
    auto it = counts_.find(key);
    if (it == counts_.end()) {
      return;
    }
    if (it->second == 1) {
      counts_.erase(it);
    } else {
      it->second--;
    }
  }
};
