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

#include <folly/logging/xlog.h>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <unordered_map>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/navy/block_cache/FifoPolicy.h"
#include "cachelib/navy/block_cache/Types.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_cache/ZLruPolicy.h"
#include "cachelib/navy/zone_cache/ZRegionManager.h"
#include "cachelib/navy/zone_cache/ZonedDevice.h"

namespace facebook {
namespace cachelib {
namespace navy {
ZRegionManager::ZRegionManager(uint32_t numRegions,
                             uint64_t regionSize,
                             uint64_t baseOffset,
                             Device& device,
                             uint32_t numCleanRegions,
                             JobScheduler& scheduler,
                             RegionEvictCallback evictCb,
                             RegionCleanupCallback cleanupCb,
                             std::unique_ptr<EvictionPolicy> policy,
                             uint32_t numInMemBuffers,
                             uint16_t numPriorities,
                             uint16_t inMemBufFlushRetryLimit,
                             const ZnsConfig& znsConfig)
    : numPriorities_{numPriorities},
      inMemBufFlushRetryLimit_{inMemBufFlushRetryLimit},
      numRegions_{numRegions},
      regionSize_{regionSize},
      baseOffset_{baseOffset},
      device_{device},
      policy_{std::move(policy)},
      regions_{std::make_unique<std::unique_ptr<Region>[]>(numRegions)},
      numCleanRegions_{numCleanRegions},
      scheduler_{scheduler},
      evictCb_{evictCb},
      cleanupCb_{cleanupCb},
      numInMemBuffers_{numInMemBuffers},
      zoneOffsetShift_{0x80000000},
      useBottomUpEviction_{znsConfig.getZnsDrop()},
      useZoneAwareLRU_{znsConfig.getZLRU()},
      useZoneAsRegion_{znsConfig.getZnsDirect()} {
  XLOGF(INFO, "{} regions, {} bytes each", numRegions_, regionSize_);
  for (uint32_t i = 0; i < numRegions; i++) {
    regions_[i] = std::make_unique<Region>(RegionId{i}, regionSize_);
  }

  XDCHECK_LT(0u, numInMemBuffers_);
  XLOGF(INFO, "{} clean regions, {} in-mem buffers", numCleanRegions_, numInMemBuffers_);
  XLOGF(INFO, "znsDrop: {}, znsDirect: {}", useBottomUpEviction_, useZoneAsRegion_);

  XCHECK(!(useBottomUpEviction_ && useZoneAsRegion_));

  if (useZoneAwareLRU_) {
    XLOG(INFO, "zLRU enable");
    EvictionPolicy* ptr = policy_.get();
    if (auto zlru = dynamic_cast<ZLruPolicy*>(ptr); zlru != nullptr) {
      zlru->setRegionManager(this);
      auto& dev = (ZonedDevice&) device_;
      dev.setCleanCallBack([this](uint32_t zid) {
        // reset the zone
        std::lock_guard<std::mutex> lk(zoneMutex_);
        XLOGF(INFO, "reset the zone {}, set evicted region = 0", zid);
        nrEvictedRegion_[zid] = 0;
        return true;
      });
      dev.setWriteCallBack([this](uint64_t logOffset, uint64_t physOffset) {
        auto rid = regionOffset(logOffset).rid().index();
        auto zid = physOffset / zoneOffsetShift_;
        std::lock_guard<std::mutex> lk(zoneMutex_);
        // deleteTRegion(rid);
        changeTRegion(rid, zid);
        removeZone(rid);
        setZone(rid, zid);
        return true;
      });
    }
    nrEvictedRegion_.resize(1000);
    zone2regions.resize(1000, std::set<uint32_t>());
    region2zone.resize(numRegions, kInvalidIndex);
  }
  
  for (uint32_t i = 0; i < numInMemBuffers_; i++) {
    buffers_.push_back(
        std::make_unique<Buffer>(device.makeIOBuffer(regionSize_)));
  }

  if (useBottomUpEviction_) {
    auto& dev = (ZonedDevice&) device_;
    // set reset callback
    XLOG(INFO, "setup region claim callback");
    dev.setResetCallBack(bindThis(&ZRegionManager::reclaimRegionFromGC, *this));
  }

  resetEvictionPolicy();
}

RegionId ZRegionManager::evict() {
  auto rid = policy_->evict();
  if (!rid.valid()) {
    XLOG(ERR, "Eviction failed");
  } else {
    if (useZoneAwareLRU_) {
      std::lock_guard<std::mutex> lk(zoneMutex_);
      removeZone(rid.index());
    }
    XLOGF(DBG, "Evict Region {}", rid.index());
  }
  return rid;
}

RegionId ZRegionManager::evictAt(const RegionId &regionId) {
  auto rid = policy_->evictAt(regionId);
  if (!rid.valid()) {
    XLOG(ERR, "Eviction failed");
  } else {
    if (useZoneAwareLRU_) {
      std::lock_guard<std::mutex> lk(zoneMutex_);
      removeZone(regionId.index());
    }
    XLOGF(DBG, "Evict Region {}", rid.index());
  }
  return rid;
}

void ZRegionManager::touch(RegionId rid) {
  auto& region = getRegion(rid);
  XDCHECK_EQ(rid, region.id());
  if (!region.hasBuffer()) {
    policy_->touch(rid);
  }
}

void ZRegionManager::track(RegionId rid) {
  auto& region = getRegion(rid);
  XDCHECK_EQ(rid, region.id());
  policy_->track(region);
}

void ZRegionManager::reset() {
  for (uint32_t i = 0; i < numRegions_; i++) {
    regions_[i]->reset();
  }
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    // Reset is inherently single threaded. All pending jobs, including
    // reclaims, have to be finished first.
    XDCHECK_EQ(reclaimsScheduled_, 0u);
    cleanRegions_.clear();
  }
  seqNumber_.store(0, std::memory_order_release);

  // Reset eviction policy
  resetEvictionPolicy();
}

Region::FlushRes ZRegionManager::flushBuffer(const RegionId& rid) {
  auto& region = getRegion(rid);
  auto callBack = [this, &rid](RelAddress addr, BufferView view) {
    auto writeBuffer = device_.makeIOBuffer(view.size());
    writeBuffer.copyFrom(0, view);
    if (!deviceWrite(addr, std::move(writeBuffer))) {
      return false;
    }
    numInMemBufWaitingFlush_.dec();
    return true;
  };

  // This is no-op if the buffer is already flushed
  return region.flushBuffer(std::move(callBack));
}

bool ZRegionManager::detachBuffer(const RegionId& rid) {
  auto& region = getRegion(rid);
  // detach buffer can return nullptr if there are active readers
  auto buf = region.detachBuffer();
  if (!buf) {
    return false;
  }
  returnBufferToPool(std::move(buf));
  return true;
}

bool ZRegionManager::cleanupBufferOnFlushFailure(const RegionId& regionId) {
  auto& region = getRegion(regionId);
  auto callBack = [this](RegionId rid, BufferView buffer) {
    cleanupCb_(rid, buffer);
    numInMemBufWaitingFlush_.dec();
    numInMemBufFlushFailures_.inc();
  };

  // This is no-op if the buffer is already cleaned up.
  if (!region.cleanupBuffer(std::move(callBack))) {
    return false;
  }

  return detachBuffer(regionId);
}

void ZRegionManager::releaseCleanedupRegion(RegionId rid) {
  auto& region = getRegion(rid);
  // Subtract the wasted bytes in the end
  externalFragmentation_.sub(getRegion(rid).getFragmentationSize());

  // Full barrier because we cannot have seqNumber_.fetch_add() re-ordered
  // below region.reset(). It is similar to the full barrier in openForRead.
  seqNumber_.fetch_add(1, std::memory_order_acq_rel);

  // Reset all region internal state, making it ready to be
  // used by a region allocator.
  region.reset();
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    cleanRegions_.push_back(rid);
  }
}

OpenStatus ZRegionManager::assignBufferToRegion(RegionId rid) {
  XDCHECK(rid.valid());
  auto buf = claimBufferFromPool();
  if (!buf) {
    return OpenStatus::Retry;
  }
  auto& region = getRegion(rid);
  region.attachBuffer(std::move(buf));
  return OpenStatus::Ready;
}

std::unique_ptr<Buffer> ZRegionManager::claimBufferFromPool() {
  std::unique_ptr<Buffer> buf;
  {
    std::lock_guard<std::mutex> bufLock{bufferMutex_};
    if (buffers_.empty()) {
      return nullptr;
    }
    buf = std::move(buffers_.back());
    buffers_.pop_back();
  }
  numInMemBufActive_.inc();
  return buf;
}

OpenStatus ZRegionManager::getCleanRegion(RegionId& rid) {
  auto status = OpenStatus::Retry;
  uint32_t newSched = 0;
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    if (!cleanRegions_.empty()) {
      rid = cleanRegions_.back();
      cleanRegions_.pop_back();
      status = OpenStatus::Ready;
    } else {
      status = OpenStatus::Retry;
    }
    auto plannedClean = cleanRegions_.size() + reclaimsScheduled_;
    if (plannedClean < numCleanRegions_) {
      newSched = numCleanRegions_ - plannedClean;
      reclaimsScheduled_ += newSched;
    }
  }
  for (uint32_t i = 0; i < newSched; i++) {
    scheduler_.enqueue(
        [this] { return startReclaim(); }, "reclaim", JobType::Reclaim);
  }
  if (status == OpenStatus::Ready) {
    status = assignBufferToRegion(rid);
    if (status != OpenStatus::Ready) {
      std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
      cleanRegions_.push_back(rid);
    }
  }
  return status;
}

void ZRegionManager::doFlush(RegionId rid, bool async) {
  // We're wasting the remaining bytes of a region, so track it for stats
  externalFragmentation_.add(getRegion(rid).getFragmentationSize());

  getRegion(rid).setPendingFlush();
  numInMemBufWaitingFlush_.inc();

  Job flushJob = [this, rid, retryAttempts = 0, flushed = false]() mutable {
    if (!flushed) {
      if (retryAttempts >= inMemBufFlushRetryLimit_) {
        // Flush failure reaches retry limit, stop flushing and start to
        // clean up the buffer.
        if (cleanupBufferOnFlushFailure(rid)) {
          releaseCleanedupRegion(rid);
          return JobExitCode::Done;
        }
        numInMemBufCleanupRetries_.inc();
        return JobExitCode::Reschedule;
      }
      auto res = flushBuffer(rid);
      if (res == Region::FlushRes::kSuccess) {
        flushed = true;
      } else {
        // We have a limited retry limit for flush errors due to device
        if (res == Region::FlushRes::kRetryDeviceFailure) {
          retryAttempts++;
          numInMemBufFlushRetries_.inc();
        }
        return JobExitCode::Reschedule;
      }
    }
    // If the buffer has been successfully flushed or the current flush
    // succeeds, detach the buffer until it succeeds
    if (flushed) {
      if (detachBuffer(rid)) {
        // Flush completed, track the region
        track(rid);
        return JobExitCode::Done;
      }
    }
    return JobExitCode::Reschedule;
  };

  if (async) {
    // XLOGF(INFO, "send flush job {}", rid.index());
    scheduler_.enqueue(std::move(flushJob), "flush", JobType::Flush);
  } else {
    while (flushJob() == JobExitCode::Reschedule) {
      // We intentionally sleep here to slow it down since this is only
      // triggered on shutdown. On cleanup failures, we will sleep a bit before
      // retrying to avoid maxing out cpu.
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds{100});
    }
  }
}

bool ZRegionManager::reclaimRegionFromGC(uint64_t offset, uint64_t znsOffset, BufferView view) {
  const auto startTime = getSteadyClock();
  auto rid = regionOffset(offset).rid();
  auto& region = getRegion(rid);
  while (region.isEvicting()) {
    // wait for the region to reclaim
    // and before that the view should be keeped for reclaim

    // sleep, do nothing, just wait
    XLOGF(DBG, "wait for region {} to finish reclaim", rid.index());
    std::this_thread::sleep_for(std::chrono::milliseconds{10});
  }

  // the region is evicted
  // 1. the region is just evicted.
  //    rid is not in LRU
  //    region is not in mapping
  //    * just reset victim
  // 2. the region is flushing/filling.
  //    rid is not in LRU
  //    region is not in mapping
  //    * just reset victim
  // 3. the region is flushed.
  //    rid is in LRU
  //    region is in mapping
  //    a. the data is right
  //       * do eviction
  //    b. the data is old
  //       * just reset victim

  // 2. the region is still in LRU
  // 2.1 the bufferview is old data of the region, just return false
  // first check to avoid evict it from LRU
  auto &zns = (ZonedDevice&) device_;
  auto info = zns.realPhyAddress(physicalOffset(RelAddress{rid, 0}), regionSize_);
  if (info && info->znsOffset != znsOffset) {
    XLOGF(DBG, "data on ZNS of region {} is not new, skip", rid.index());
    return false;
  }

  // 1. check LRU
  auto evictedId = evictAt(rid);
  if (!evictedId.valid()) {
    // do not update mapping
    XLOGF(DBG, "region {} is not in LRU, skip", rid.index());
    return false;
  }

  // 2.2 begin real eviction
  while (!region.readyForReclaim()) {
    XLOGF(DBG, "wait for region {} to ready for reclaim", rid.index());
    std::this_thread::sleep_for(std::chrono::milliseconds{20});
  }

  if (region.getNumItems() != 0) {
    XDCHECK(!region.hasBuffer());
    XLOGF(DBG, "bottom up eviction region: {} start", rid.index());
    auto numEvicted = evictCb_(rid, view);
    evictedCount_.add(numEvicted);
  }

  seqNumber_.fetch_add(1, std::memory_order_acq_rel);
  region.reset();
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    cleanRegions_.push_back(rid);
  }
  XLOGF(DBG, "bottom up eviction region: {} done", rid.index());
  return true;
}

JobExitCode ZRegionManager::startReclaim() {
  auto rid = evict();
  if (!rid.valid()) {
    return JobExitCode::Reschedule;
  }
  // XLOGF(DBG, "begin claiming region: {}", rid.index());
  XLOGF(DBG, "send reclaim job {}", rid.index());
  getRegion(rid).startEvict();

  scheduler_.enqueue(
      [this, rid] {
        const auto startTime = getSteadyClock();
        auto& region = getRegion(rid);
        if (!region.readyForReclaim()) {
          // Once a region is set exclusive, all future accesses will be
          // blocked. However there might still be accesses in-flight,
          // so we would retry if that's the case.
          return JobExitCode::Reschedule;
        }
        XLOGF(DBG, "lock region: {} for reclaiming", rid.index());
        // We know now we're the only thread working with this region.
        // Hence, it's safe to access @Region without lock.
        if (region.getNumItems() != 0) {
          XDCHECK(!region.hasBuffer());
          auto desc = RegionDescriptor::makeReadDescriptor(
              OpenStatus::Ready, RegionId{rid}, true /* physRead */);
          // allocate buffer
          auto sizeToRead = region.getLastEntryEndOffset();
          auto buffer = read(desc, RelAddress{rid, 0}, sizeToRead);
          if (buffer.size() != sizeToRead) {
            // TODO: remove when we fix T95777575
            XLOGF(ERR,
                  "Failed to read region {} during reclaim. Region size to "
                  "read: {}, Actually read: {}",
                  rid.index(),
                  sizeToRead,
                  buffer.size());
            reclaimRegionErrors_.inc();
          } else {
            if (useBottomUpEviction_) {
              // when bottom-up eviction is enable the top-down eviction must be enabled
              // to ensure mapping correction.
              auto trimRes = trimRegion(RelAddress{rid, 0});
              if (!trimRes) {
                XLOGF(ERR, "Failed to trim region {} during reclaim.", rid.index());
              }
            }
            doEviction(rid, buffer.view());
          }
        }
        releaseEvictedRegion(rid, startTime);
        XLOGF(DBG, "regular evict region: {}", rid.index());
        return JobExitCode::Done;
      },
      "reclaim.evict",
      JobType::Reclaim);
  return JobExitCode::Done;
}

RegionDescriptor ZRegionManager::openForRead(RegionId rid, uint64_t seqNumber) {
  auto& region = getRegion(rid);
  auto desc = region.openForRead();
  if (!desc.isReady()) {
    return desc;
  }

  // << Interaction of Region Lock and Sequence Number >>
  //
  // Reader:
  // 1r. Load seq number
  // 2r. Check index
  // 3r. Open region
  // 4r. Load seq number
  //     If hasn't changed, proceed to read and close region.
  //     Otherwise, abort read and close region.
  //
  // Reclaim:
  // 1x. Mark region ready for reclaim
  // 2x. Reclaim and evict entries from index
  // 3x. Store seq number
  // 4x. Reset region
  //
  // In order for these two sequence of operations to not have data race,
  // we must guarantee the following ordering:
  //   3r -> 4r
  //
  // We know that 3r either happens before 1x or happens after 4x, this
  // means with the above ordering, 4r will either:
  // 1. Read the same seq number and proceed to read
  //    (3r -> 4r -> (read item and close region) -> 1x)
  // 2. Or, read a different seq number and abort read (4x -> 3r -> 4r)
  // Either of the above is CORRECT operation.
  //
  // 3r has mutex::lock() at the beginning so, it prevents 4r from being
  // reordered above it.
  //
  // We also need to ensure 3x is not re-ordered below 4x. This is handled
  // by a acq_rel memory order in 3x. See releaseEvictedRegion() for details.
  //
  // Finally, 4r has acquire semantic which will sychronizes-with 3x's acq_rel.
  if (seqNumber_.load(std::memory_order_acquire) != seqNumber) {
    region.close(std::move(desc));
    return RegionDescriptor{OpenStatus::Retry};
  }
  return desc;
}

void ZRegionManager::close(RegionDescriptor&& desc) {
  RegionId rid = desc.id();
  auto& region = getRegion(rid);
  region.close(std::move(desc));
}

void ZRegionManager::releaseEvictedRegion(RegionId rid,
                                         std::chrono::nanoseconds startTime) {
  auto& region = getRegion(rid);
  // Subtract the wasted bytes in the end since we're reclaiming this region now
  externalFragmentation_.sub(getRegion(rid).getFragmentationSize());

  // Full barrier because we cannot have seqNumber_.fetch_add() re-ordered
  // below region.reset(). If it is re-ordered then, we can end up with a data
  // race where a read returns stale data. See openForRead() for details.
  seqNumber_.fetch_add(1, std::memory_order_acq_rel);

  // Reset all region internal state, making it ready to be
  // used by a region allocator.
  region.reset();
  {
    std::lock_guard<std::mutex> lock{cleanRegionsMutex_};
    XLOGF(DBG, "reclaim scheduled changed {}, new reclaimed region {}", reclaimsScheduled_, rid.index());
    reclaimsScheduled_--;
    cleanRegions_.push_back(rid);
  }
  reclaimTimeCountUs_.add(toMicros(getSteadyClock() - startTime).count());
  reclaimCount_.inc();
}

void ZRegionManager::doEviction(RegionId rid, BufferView buffer) const {
  if (buffer.isNull()) {
    XLOGF(ERR, "Error reading region {} on reclamation", rid.index());
  } else {
    const auto evictStartTime = getSteadyClock();
    XLOGF(DBG, "Evict region {} entries", rid.index());
    auto numEvicted = evictCb_(rid, buffer);
    XLOGF(DBG,
          "Evict region {} entries: {} us",
          rid.index(),
          toMicros(getSteadyClock() - evictStartTime).count());
    evictedCount_.add(numEvicted);
  }
}

void ZRegionManager::persist(RecordWriter& rw) const {
  serialization::RegionData regionData;
  *regionData.regionSize() = regionSize_;
  regionData.regions()->resize(numRegions_);
  for (uint32_t i = 0; i < numRegions_; i++) {
    auto& regionProto = regionData.regions()[i];
    *regionProto.regionId() = i;
    *regionProto.lastEntryEndOffset() = regions_[i]->getLastEntryEndOffset();
    regionProto.priority() = regions_[i]->getPriority();
    *regionProto.numItems() = regions_[i]->getNumItems();
  }
  serializeProto(regionData, rw);
}

void ZRegionManager::recover(RecordReader& rr) {
  auto regionData = deserializeProto<serialization::RegionData>(rr);
  if (regionData.regions()->size() != numRegions_ ||
      static_cast<uint32_t>(*regionData.regionSize()) != regionSize_) {
    throw std::invalid_argument(
        "Could not recover ZRegionManager. Invalid RegionData.");
  }

  for (auto& regionProto : *regionData.regions()) {
    uint32_t index = *regionProto.regionId();
    if (index >= numRegions_ ||
        static_cast<uint32_t>(*regionProto.lastEntryEndOffset()) >
            regionSize_) {
      throw std::invalid_argument(
          "Could not recover ZRegionManager. Invalid RegionId.");
    }
    // To handle compatibility between different priorities. If the current
    // setup has fewer priorities than the last run, automatically downgrade
    // all higher priorties to the current max.
    if (numPriorities_ > 0 && regionProto.priority() >= numPriorities_) {
      regionProto.priority() = numPriorities_ - 1;
    }
    regions_[index] =
        std::make_unique<Region>(regionProto, *regionData.regionSize());
  }

  // Reset policy and reinitialize it per the recovered state
  resetEvictionPolicy();
}

void ZRegionManager::resetEvictionPolicy() {
  XDCHECK_GT(numRegions_, 0u);

  policy_->reset();
  externalFragmentation_.set(0);

  // Go through all the regions, restore fragmentation size, and track all empty
  // regions
  for (uint32_t i = 0; i < numRegions_; i++) {
    externalFragmentation_.add(regions_[i]->getFragmentationSize());
    if (regions_[i]->getNumItems() == 0) {
      track(RegionId{i});
    }
  }

  // Now track all non-empty regions. This should ensure empty regions are
  // pushed to the bottom for both LRU and FIFO policies.
  for (uint32_t i = 0; i < numRegions_; i++) {
    if (regions_[i]->getNumItems() != 0) {
      track(RegionId{i});
    }
  }
}

bool ZRegionManager::isValidIORange(uint32_t offset, uint32_t size) const {
  return static_cast<uint64_t>(offset) + size <= regionSize_;
}

bool ZRegionManager::deviceWrite(RelAddress addr, Buffer buf) {
  const auto bufSize = buf.size();
  XDCHECK(isValidIORange(addr.offset(), bufSize));
  auto physOffset = physicalOffset(addr);
  if (useZoneAsRegion_) {
    // reset and open zone
    auto& zns = (DirectZonedDevice&)device_;
    zns.reset(physOffset);
    zns.open(physOffset);
  }

  if (!device_.write(physOffset, std::move(buf))) {
    return false;
  }
  physicalWrittenCount_.add(bufSize);

  if (useZoneAsRegion_) {
    // finish the zone
    auto& zns = (DirectZonedDevice&)device_;
    zns.finish(physOffset);
  }

  return true;
}

void ZRegionManager::write(RelAddress addr, Buffer buf) {
  auto rid = addr.rid();
  auto& region = getRegion(rid);
  region.writeToBuffer(addr.offset(), buf.view());
}

Buffer ZRegionManager::read(const RegionDescriptor& desc,
                           RelAddress addr,
                           size_t size) const {
  auto rid = addr.rid();
  auto& region = getRegion(rid);
  // Do not expect to read beyond what was already written
  if (addr.offset() + size > region.getLastEntryEndOffset()) {
    XLOGF(ERR, "region read error {}, offset {}", rid.index(), addr.offset());
  }
  XDCHECK_LE(addr.offset() + size, region.getLastEntryEndOffset());
  if (!desc.isPhysReadMode()) {
    auto buffer = Buffer(size);
    XDCHECK(region.hasBuffer());
    region.readFromBuffer(addr.offset(), buffer.mutableView());
    return buffer;
  }
  XDCHECK(isValidIORange(addr.offset(), size));

  return device_.read(physicalOffset(addr), size);
}

bool ZRegionManager::trimRegion(RelAddress addr) const {
  auto &zns = (ZonedDevice&)device_;
  return zns.trimBlock(physicalOffset(addr), regionSize_);
}

void ZRegionManager::flush() { device_.flush(); }

void ZRegionManager::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_bc_reclaim", reclaimCount_.get());
  visitor("navy_bc_reclaim_time", reclaimTimeCountUs_.get());
  visitor("navy_bc_region_reclaim_errors", reclaimRegionErrors_.get());
  visitor("navy_bc_evicted", evictedCount_.get());
  visitor("navy_bc_num_regions", numRegions_);
  visitor("navy_bc_num_clean_regions", cleanRegions_.size());
  visitor("navy_bc_external_fragmentation", externalFragmentation_.get());
  visitor("navy_bc_physical_written", physicalWrittenCount_.get());
  visitor("navy_bc_inmem_active", numInMemBufActive_.get());
  visitor("navy_bc_inmem_waiting_flush", numInMemBufWaitingFlush_.get());
  visitor("navy_bc_inmem_flush_retries", numInMemBufFlushRetries_.get());
  visitor("navy_bc_inmem_flush_failures", numInMemBufFlushFailures_.get());
  visitor("navy_bc_inmem_cleanup_retries", numInMemBufCleanupRetries_.get());
  policy_->getCounters(visitor);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook
