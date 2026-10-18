#ifndef ARBORETUM_SRC_REMOTE_SIMNETPROTOCOL_H_
#define ARBORETUM_SRC_REMOTE_SIMNETPROTOCOL_H_

// Wire protocol shared by SimNetClient and SimNetServer.
// Frames are length-prefixed little-endian binary (same-arch assumption):
//   request:  [u32 body_len][u64 req_id][u8 opcode][payload...]
//   reply:    [u32 body_len][u64 req_id][u8 status][payload...]
// Strings within a payload are u32-length-prefixed; single-value replies
// (LOAD, STORE_AND_LOAD) carry the raw value bytes with no prefix.

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <cstring>
#include <string>

namespace arboretum {
namespace simnet {

constexpr uint64_t kHelloMagic = 0x53494d4e45543031ULL; // "SIMNET01"
constexpr uint32_t kMaxFrameBody = 1u << 30;            // 1 GB sanity cap
constexpr size_t kFrameFixed = sizeof(uint64_t) + sizeof(uint8_t);

enum Op : uint8_t {
  OP_HELLO = 1,
  OP_CREATE_TABLE,
  OP_DELETE_TABLE,
  OP_LOAD,          // also serves numeric loads (8-byte values)
  OP_STORE,         // also serves numeric stores
  OP_STORE_AND_LOAD,
  OP_BATCH_STORE,
  OP_RANGE,
  OP_LOG_FLUSH,
  OP_CHECKPOINT,
};

enum Status : uint8_t {
  ST_OK = 0,
  ST_NOT_FOUND,
  ST_ERROR,
  ST_BATCH_TOO_LARGE, // EGT >100 rows with g_sim_enforce_batch_limit
  ST_THROTTLED,       // reserved for the deferred RU/429 simulation
};

// -------------------- payload encoding --------------------

inline void PutU32(std::string &buf, uint32_t v) {
  buf.append((const char *) &v, sizeof(v));
}
inline void PutU64(std::string &buf, uint64_t v) {
  buf.append((const char *) &v, sizeof(v));
}
inline void PutStr(std::string &buf, const std::string &s) {
  PutU32(buf, (uint32_t) s.size());
  buf.append(s);
}
inline void PutBytes(std::string &buf, const char *data, size_t sz) {
  PutU32(buf, (uint32_t) sz);
  if (sz != 0) buf.append(data, sz);
}

// bounds-checked sequential decoder; check ok() once after the last field
class Reader {
 public:
  explicit Reader(const std::string &buf) : buf_(buf) {}
  uint32_t GetU32() {
    uint32_t v = 0;
    Copy(&v, sizeof(v));
    return v;
  }
  uint64_t GetU64() {
    uint64_t v = 0;
    Copy(&v, sizeof(v));
    return v;
  }
  bool GetStr(std::string &s) {
    auto sz = GetU32();
    if (!ok_ || buf_.size() - off_ < sz) {
      ok_ = false;
      return false;
    }
    s.assign(buf_, off_, sz);
    off_ += sz;
    return true;
  }
  // remaining raw bytes (single-value payloads)
  void GetRest(std::string &s) {
    s.assign(buf_, off_, buf_.size() - off_);
    off_ = buf_.size();
  }
  bool ok() const { return ok_; }

 private:
  void Copy(void *dst, size_t sz) {
    if (buf_.size() - off_ < sz) {
      ok_ = false;
      return;
    }
    memcpy(dst, buf_.data() + off_, sz);
    off_ += sz;
  }
  const std::string &buf_;
  size_t off_{0};
  bool ok_{true};
};

// -------------------- socket helpers --------------------

inline bool SendAll(int fd, const char *buf, size_t len) {
  while (len > 0) {
    auto n = ::send(fd, buf, len, MSG_NOSIGNAL);
    if (n <= 0) {
      if (n < 0 && errno == EINTR) continue;
      return false;
    }
    buf += n;
    len -= (size_t) n;
  }
  return true;
}

inline bool RecvAll(int fd, char *buf, size_t len) {
  while (len > 0) {
    auto n = ::recv(fd, buf, len, 0);
    if (n <= 0) {
      if (n < 0 && errno == EINTR) continue;
      return false; // 0 = orderly shutdown
    }
    buf += n;
    len -= (size_t) n;
  }
  return true;
}

// assembles header + body into one buffer so each frame is a single send()
inline bool SendFrame(int fd, uint64_t req_id, uint8_t code,
                      const std::string &payload) {
  std::string frame;
  auto body_len = (uint32_t) (kFrameFixed + payload.size());
  frame.reserve(sizeof(body_len) + body_len);
  frame.append((const char *) &body_len, sizeof(body_len));
  frame.append((const char *) &req_id, sizeof(req_id));
  frame.append((const char *) &code, sizeof(code));
  frame.append(payload);
  return SendAll(fd, frame.data(), frame.size());
}

inline bool RecvFrame(int fd, uint64_t &req_id, uint8_t &code,
                      std::string &payload) {
  uint32_t body_len;
  if (!RecvAll(fd, (char *) &body_len, sizeof(body_len))) return false;
  if (body_len < kFrameFixed || body_len > kMaxFrameBody) return false;
  if (!RecvAll(fd, (char *) &req_id, sizeof(req_id))) return false;
  if (!RecvAll(fd, (char *) &code, sizeof(code))) return false;
  payload.resize(body_len - kFrameFixed);
  return payload.empty() || RecvAll(fd, &payload[0], payload.size());
}

// lockstep request/reply over TCP: Nagle + delayed ACK would inject spurious
// 40ms stalls, so NODELAY is mandatory on every simnet socket
inline void SetNoDelay(int fd) {
  int one = 1;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

} // namespace simnet
} // namespace arboretum

#endif // ARBORETUM_SRC_REMOTE_SIMNETPROTOCOL_H_
