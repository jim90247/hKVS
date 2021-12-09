#pragma once
#include <list>
#include <unordered_map>

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
   */
  inline void Put(const Record &record) {
    auto it = map_.find(record);
    if (it != map_.end()) {
      // remove old copy
      items_.erase(it->second);
      map_.erase(it);
    }
    items_.push_front(record);
    map_[record] = items_.begin();
    if (items_.size() > capacity_) {
      ListIterator last = std::prev(items_.end());
      map_.erase(*last);
      items_.pop_back();
    }
  }
  /**
   * @brief Checks if the record exists in the LRU Records.
   *
   * @param record the record
   * @return true if the record exists
   */
  inline bool Contain(const Record &record) const {
    return map_.find(record) != map_.end();
  }

 private:
  const size_t capacity_;
  std::unordered_map<Record, ListIterator> map_;
  std::list<Record> items_;
};
