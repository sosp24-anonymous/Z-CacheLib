/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <folly/logging/xlog.h>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>

#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/block_cache/EvictionPolicy.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/zone_cache/ZRegionManager.h"
#include "cachelib/shm/SysVShmSegment.h"

namespace facebook {
namespace cachelib {
namespace navy {
// LRU policy with optional deferred LRU insert
class ZLruPolicy final : public EvictionPolicy {
 public:
  // Constructs LRU policy.
  // @expectedNumRegions is a hint how many regions to expect in LRU.
  explicit ZLruPolicy(uint32_t expectedNumRegions);

  ZLruPolicy(const ZLruPolicy&) = delete;
  ZLruPolicy& operator=(const ZLruPolicy&) = delete;

  ~ZLruPolicy() override {}

  void setRegionManager(ZRegionManager *regionManager) {
    this->regionManager_ = std::shared_ptr<ZRegionManager>(regionManager);
  };

  // Records the hit of the region.
  void touch(RegionId rid) override;

  // Adds a new region to the array for tracking.
  void track(const Region& region) override;

  // Evicts the least recently used region and stops tracking.
  RegionId evict() override;

  // Manually, evicts a region and stops tracking.
  RegionId evictAt(const RegionId &regionId) override;

  // Resets LRU policy to the initial state.
  void reset() override;

  // Gets memory used by LRU policy.
  size_t memorySize() const override;

  // Exports LRU policy stats via CounterVisitor.
  void getCounters(const CounterVisitor& v) const override;

  // Persists metadata associated with LRU policy.
  void persist(RecordWriter& rw) const override;

  // Recovers from previously persisted metadata associated with LRU policy.
  void recover(RecordReader& rr) override;

 private:
  static constexpr uint32_t kInvalidIndex = 0xffffffffu;

  // Double linked list with index as a pointer and kInvalidIndex as nullptr
  struct ListNode {
    uint32_t prev{kInvalidIndex};
    uint32_t next{kInvalidIndex};
    // seconds since epoch stamp of time events
    std::chrono::seconds creationTime{};
    std::chrono::seconds lastUpdateTime{};
    uint32_t hits{};

    bool inList() const {
      return prev != kInvalidIndex || next != kInvalidIndex;
    }

    std::chrono::seconds secondsSinceCreation() const {
      return getSteadyClockSeconds() - creationTime;
    }

    std::chrono::seconds secondsSinceAccess() const {
      return getSteadyClockSeconds() - lastUpdateTime;
    }
  };

  void unlink(uint32_t i);
  void linkAtHead(uint32_t i);
  void linkAtTail(uint32_t i);
  void linkAtReorder(uint32_t i);

  void dump(uint32_t n) const;
  void dumpList(const char* tag,
                uint32_t n,
                uint32_t first,
                uint32_t ListNode::*link) const;


  void eraseTSet(uint32_t rid) {
    tset.erase(rid);
    regionManager_->lockZoneMutex();
    regionManager_->deleteTRegion(rid);
    regionManager_->unlockZoneMutex();
  }

  void checkTSet() {
    auto t = tpointer;
    int cnt = 0;
    while (t != kInvalidIndex) {
      t = array_[t].next;
      cnt++;
    }
  }

  void insertTSet() {
    XCHECK_NE(tpointer, head_);
    tpointer = array_[tpointer].prev;

    XCHECK_NE(tpointer, kInvalidIndex);
    XLOGF(DBG, "insert region {}", tpointer);
    regionManager_->lockZoneMutex();
    tset.insert(tpointer);
    regionManager_->pushTRegion(tpointer);

    auto zid = regionManager_->getZoneId(tpointer);
    auto &s = regionManager_->getRegionIds(zid);
    if (zid == kInvalidKey) {
      regionManager_->unlockZoneMutex();
      return;
    }

    // XLOGF(INFO, "tset size {} for zone {}", s.size() + regionManager_->getNrEvcited(zid), zid);
    if (s.size() + regionManager_->getNrEvcited(zid) > top_victim_threshold) {
      // move to next tset item
      auto tp= array_[tpointer].prev;
      // push all entries to tail
      if (s.size() > 1) {
        XLOGF(INFO, "push all entries to tail. zone={} nr={}, ev={}", zid, s.size(), regionManager_->getNrEvcited(zid));
      }
      // XLOGF(DBG, "before: {} -> {} -> {} -> {}", tp, tpointer, array_[tpointer].next, array_[array_[tpointer].next].next);

      for (auto x : s) {
        if (tset.count(x) == 0) {
          XLOGF(DBG, "x is not in tset {}", x);
          throw std::logic_error("tset is wrong in push");
        }
        if (array_[x].inList()) {
          XLOGF(DBG, "push region {}", x);
          // push to back
          unlink(x);
          linkAtTail(x);
          // linkAtReorder(x);
        } else {
          XLOGF(DBG, "region {} is not in list", x);
        }
      }
      s.clear();
      tpointer = array_[tp].next;
      // XLOGF(DBG, "after: {} -> {} -> {} -> {}", tp, tpointer, array_[tpointer].next, array_[array_[tpointer].next].next);
    }
    regionManager_->unlockZoneMutex();
  }

  static constexpr std::chrono::seconds kEstimatorWindow{5};

  static constexpr double top_window_alpha = 0.3;
  static constexpr uint32_t top_victim_threshold = 48;

  std::vector<ListNode> array_;

  uint32_t head_{kInvalidIndex};
  uint32_t tail_{kInvalidIndex};
  // top down
  uint32_t tpointer{kInvalidIndex};
  // bottom up
  uint32_t bpointer{kInvalidIndex};

  // reorder pointer
  uint32_t rpointer{kInvalidIndex};

  std::vector<uint32_t> ttail;

  std::set<uint32_t> tset;

  // based on zone
  std::shared_ptr<ZRegionManager> regionManager_;

  // bottwn up
  mutable std::mutex mutex_;

  // various counters that are populated when we evict a region.
  mutable util::PercentileStats secSinceInsertionEstimator_;
  mutable util::PercentileStats secSinceAccessEstimator_;
  mutable util::PercentileStats hitsEstimator_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook
