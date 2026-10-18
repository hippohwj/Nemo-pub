#ifndef ARBORETUM_SRC_COMMON_RWLOCK_H_
#define ARBORETUM_SRC_COMMON_RWLOCK_H_

#include <shared_mutex>
#include <atomic>
#include <chrono>

namespace arboretum {

// Forward declarations only: Types.h includes this header and GlobalData.h
// includes Types.h, so pulling GlobalData.h in here would be a cycle.
// g_latch_on is set from g_latch_bench once the warm-up ends (tpcc.h), so the
// instrumented path costs two clock reads per acquisition and only while
// measuring.
extern std::atomic<bool> g_latch_on;
extern std::atomic<uint64_t> g_latch_wait_ns_ex, g_latch_wait_ns_sh,
    g_latch_hold_ns_ex, g_latch_ex_cnt, g_latch_sh_cnt;
inline uint64_t LatchNow() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now().time_since_epoch()).count();
}

struct RWLock {
  std::shared_timed_mutex latch;
  std::atomic<uint32_t> status{0}; // 1 bit for ex lock; the others for ref cnt
  // g_latch_bench: acquire timestamp of the current EX holder. Only the EX
  // holder writes it and only while it holds the latch, so no extra sync is
  // needed. SH holders are many, so only their WAIT is measured, not the hold.
  // carved out of the existing padding on purpose: RWLock is embedded in
  // PageBufferManager's per-slot descriptor, and growing it changes buffer
  // layout arithmetic. sizeof(RWLock) must not move.
  uint64_t ex_acq_ts_{0};
  char pad_[56]; // padding to avoid false sharing (64 - sizeof(ex_acq_ts_))
  bool TryLockEX() {
    if (latch.try_lock()) {
      status = 1;
      return true;
    }
    return false;
  };
  void LockEX() {
    if (g_latch_on.load(std::memory_order_relaxed)) {
      auto t0 = LatchNow();
      latch.lock();
      auto t1 = LatchNow();
      g_latch_wait_ns_ex.fetch_add(t1 - t0, std::memory_order_relaxed);
      g_latch_ex_cnt.fetch_add(1, std::memory_order_relaxed);
      ex_acq_ts_ = t1;
    } else {
      latch.lock();
    }
    status = 1;
  }
  void LockSH() {
    if (g_latch_on.load(std::memory_order_relaxed)) {
      auto t0 = LatchNow();
      latch.lock_shared();
      g_latch_wait_ns_sh.fetch_add(LatchNow() - t0, std::memory_order_relaxed);
      g_latch_sh_cnt.fetch_add(1, std::memory_order_relaxed);
    } else {
      latch.lock_shared();
    }
    status.fetch_add(1 << 1);
  }
  void Lock(bool exclusive) {
    if (exclusive)
      LockEX();
    else
      LockSH();
  }
  bool IsEXLocked() const { return status == 1; };
  bool IsLocked() const { return status > 0; };
  void Unlock() {
    if (IsEXLocked()) {
      if (g_latch_on.load(std::memory_order_relaxed) && ex_acq_ts_) {
        g_latch_hold_ns_ex.fetch_add(LatchNow() - ex_acq_ts_,
                                     std::memory_order_relaxed);
        ex_acq_ts_ = 0;
      }
      status = 0;
      latch.unlock();
    } else if (IsLocked()) {
      status.fetch_sub(1 << 1);
      latch.unlock_shared();
    }
  }
};

struct UpgradableRWLock {
  std::shared_timed_mutex latch;
  std::atomic<uint32_t> status{0}; // 1 bit for ex lock; the others for ref cnt
  char pad_[64]; // padding to avoid false sharing
  bool TryLockEX() {
    if (latch.try_lock()) {
      status = 1;
      return true;
    }
    return false;
  };
  void LockEX() {
    latch.lock();
    status = 1;
  }
  void LockSH() {
    latch.lock_shared();
    status.fetch_add(2);
  }
  void Lock(bool exclusive) {
    if (exclusive)
      LockEX();
    else
      LockSH();
  }
  bool IsEXLocked() const { return status == 1; };
  bool IsLocked() const { return status > 0; };
  void Unlock() {
    if (IsEXLocked()) {
      status = 0;
      latch.unlock();
    } else if (IsLocked()) {
      status.fetch_sub(2);
      latch.unlock_shared();
    }
  }

};

}

#endif //ARBORETUM_SRC_COMMON_RWLOCK_H_
