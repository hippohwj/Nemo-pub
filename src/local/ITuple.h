#ifndef ARBORETUM_SRC_LOCAL_ITUPLE_H_
#define ARBORETUM_SRC_LOCAL_ITUPLE_H_

#include "common/Common.h"
#include "ISchema.h"
#include <cstddef>

namespace arboretum {

// metadata manipulation (ref bit/dirty bit/gap bit/tombstone bit/phantom bit)
// Bit masks for tuple metadata
static constexpr uint8_t REF_BIT   = 0b00000001; // Bit 0: Reference Bit
static constexpr uint8_t DIRTY_BIT = 0b00000010; // Bit 1: Dirty Bit
static constexpr uint8_t GAP_BIT = 0b00000100; // Bit 2: GAP Bit
static constexpr uint8_t TOMBSTONE_BIT = 0b00001000; // Bit 3: tombstone Bit
static constexpr uint8_t PHANTOM_BIT = 0b00010000; // Bit 4: Phantom Bit


class ITuple {

 public:
  ITuple() = default;;
  explicit ITuple(OID tbl_id, OID tid) : tbl_id_(tbl_id), tid_(tid) {};
  void SetFromLoaded(char * loaded_data, size_t sz) { SetData(loaded_data, sz); };
  void SetData(char * src, size_t sz);
  void SetData(char * src, size_t sz, BufferType btype);
  // Offset of the record payload inside a page-resident tuple image. This is
  // a WIRE FORMAT (see the layout warning further down), frozen when the page
  // datasets were dumped: sizeof(ITuple) was 72 then, and the payload started
  // at 72 - sizeof(next_key_lock_) - sizeof(next_key_in_storage_) = 67.
  // It used to be spelled with that arithmetic, which silently tracked
  // sizeof(ITuple) and therefore moved the moment a field was added.
  static constexpr size_t kPageTupleDataOffset = 67;

  char * GetData() {
    if (g_buf_type == PGBUF) return (char *) this + kPageTupleDataOffset;
    return (char *) this + sizeof(ITuple);
  };

  char * GetData(BufferType btype) {
    if (btype == PGBUF) return (char *) this + kPageTupleDataOffset;
    return (char *) this + sizeof(ITuple);
  };

  void SetTID(OID tid) { tid_ = tid; };
  OID GetTID() const { return tid_; };
  OID GetTableId() const { return tbl_id_; };
  void GetField(ISchema * schema, OID col, SearchKey &key);
  void SetField(ISchema * schema, OID col, std::string value);
  void SetField(ISchema * schema, OID col, int64_t value);
  void GetPrimaryKey(ISchema * schema, SearchKey &key);
  SearchKey GetPrimaryKey(ISchema * schema) {
    SearchKey key;
    GetPrimaryKey(schema, key);
    return key;
  }


  // metadata manipulation (ref bit/dirty bit/phantom bits)
  bool SetRefBit();
  bool UnsetRefBit();
  bool SetDirty();
  bool UnsetDirty();
  bool SetGapBit();
  bool UnsetGapBit();
  bool SetTombstone();
  bool UnsetTombstone(); 
  bool SetPhantom();
  bool UnsetPhantom();
  bool SetMeta(uint8_t bitmask);
  bool UnsetMeta(uint8_t bitmask);


  bool IsRefBitSet();
  bool IsDirty(); 
  bool IsTombstone();
  bool IsPhantom();
  bool IsGapBitSet(); 
  bool IsNegativeGap();
  bool IsMetaSet(uint8_t bitmask);

  // buffer helper
  std::atomic<uint32_t> buf_state_{0};

  // metadata manipulation (ref bit/dirty bit/gap bit/tombstone bit/phantom bit)
  std::atomic<uint8_t> tuple_meta_{GAP_BIT};

  // -------------------------------------------------------------------
  // ON-DISK LAYOUT. Page datasets (g_buf_type == PGBUF) store whole ITuple
  // images inside BasePage, so every field from here down to tid_ -- and
  // kPageTupleDataOffset above -- is a wire format, not a struct detail.
  // Inserting a field anywhere above tid_ invalidates every page dataset
  // already in remote storage, and does so silently: the reader picks
  // tbl_id_/tid_/columns up at the wrong offsets, GetTableId() returns
  // garbage, and ITxn::PreCommit indexes ARDB::tables_ out of bounds.
  // Put new fields AFTER next_key_in_storage_; the static_asserts below fail
  // the build if this prefix ever moves again.
  // -------------------------------------------------------------------

  // should not be counted in the the size of the tuple, it is only used for a evcition baseline comparison
  ITuple * clock_next_{nullptr};
  std::mutex buf_latch_{};

 private:
  OID tbl_id_{0};
 public:
  OID tid_{0}; // identifier used in the lock table

  // a hack for the next key lock, not counted in the size of the tuple
  // should use a separate lock table instead of embedding the lock in the tuple
  std::atomic<uint32_t> next_key_lock_{0};

  // remove this field, it is already encoded in tuple_meta_
  bool next_key_in_storage_{false};

  // ---- past the end of the on-disk prefix: free to add/reorder ----
  // exact-LRU mode (g_evict_lru, buffer-replacement microbenchmark only):
  // doubly-linked recency list; clock_next_ is reused as the MRU-ward link.
  // Process-local state only, so it lives below the wire format.
  ITuple * lru_prev_{nullptr};
};

// Guard the wire format described above. tid_ is the last field of the
// on-page prefix, so pinning its offset pins everything before it; pinning
// next_key_lock_ additionally pins the payload offset.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winvalid-offsetof"
static_assert(offsetof(ITuple, tid_) == 60,
              "on-page ITuple prefix moved: every PGBUF page dataset in remote "
              "storage would be read at the wrong offsets. Add new fields after "
              "next_key_in_storage_ instead.");
static_assert(offsetof(ITuple, next_key_lock_) == ITuple::kPageTupleDataOffset - 3,
              "on-page payload offset no longer matches the dumped page format.");
#pragma GCC diagnostic pop

}

#endif //ARBORETUM_SRC_LOCAL_ITUPLE_H_
