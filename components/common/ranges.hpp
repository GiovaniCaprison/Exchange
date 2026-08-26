// The range framing the sequenced stream travels in off the ring (docs/PROTOCOL.md): a framed
// RangeHeader naming the first sequence, the epoch and the count, then count framed messages
// each prefixed by its u16 length. A count of zero is a heartbeat carrying the next sequence, so
// silence is distinguishable from loss; a count of 65535 is end of session. The shape follows
// MoldUDP64 (Nasdaq, public specification), with the epoch in place of the session name.

#pragma once

#include <cstdint>
#include <cstring>

#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/RangeHeader.h"

namespace exchange::common::ranges {

inline constexpr std::uint16_t END_OF_SESSION = 65535;

// One packet's budget, chosen to sit under an MTU with headroom; live publication and the
// rewinder chunk by the same number so a rewound stream looks like the one that was missed.
inline constexpr std::size_t PACKET_BYTES = 1200;

inline constexpr std::size_t headerBytes() {
  return protocol::MessageHeader::encodedLength() + protocol::RangeHeader::sbeBlockLength();
}

// Builds one range in a caller-owned buffer: open reserves the header and names the first
// sequence, add appends a length-prefixed message, close writes the header and yields the size.
class Builder {
 public:
  Builder(char* buffer, const std::size_t capacity) : buffer_(buffer), capacity_(capacity) {}

  void open(const std::uint64_t firstSequence, const std::uint32_t epoch) {
    firstSequence_ = firstSequence;
    epoch_ = epoch;
    count_ = 0;
    cursor_ = headerBytes();
    open_ = true;
  }

  bool isOpen() const { return open_; }
  bool fits(const std::size_t length) const {
    return cursor_ + sizeof(std::uint16_t) + length <= capacity_;
  }

  void add(const char* message, const std::uint16_t length) {
    std::memcpy(buffer_ + cursor_, &length, sizeof length);
    std::memcpy(buffer_ + cursor_ + sizeof length, message, length);
    cursor_ += sizeof length + length;
    count_++;
  }

  std::size_t close() { return seal(count_); }
  std::size_t closeEndOfSession() { return seal(END_OF_SESSION); }

 private:
  std::size_t seal(const std::uint16_t count) {
    protocol::RangeHeader header;
    header.wrapAndApplyHeader(buffer_, 0, capacity_);
    header.firstSequence(firstSequence_).epoch(epoch_).count(count).reserved(0);
    open_ = false;
    return cursor_;
  }

  char* buffer_;
  std::size_t capacity_;
  std::uint64_t firstSequence_ = 0;
  std::uint32_t epoch_ = 0;
  std::uint16_t count_ = 0;
  std::size_t cursor_ = 0;
  bool open_ = false;
};

// Reads one range: the header's facts, and each message handed over as (bytes, length).
class Reader {
 public:
  Reader(char* bytes, const std::size_t length) : bytes_(bytes) {
    protocol::MessageHeader wrap;
    wrap.wrap(bytes, 0, 0, length);
    protocol::RangeHeader header;
    header.wrapForDecode(bytes, wrap.encodedLength(), wrap.blockLength(), wrap.version(), length);
    firstSequence_ = header.firstSequence();
    epoch_ = header.epoch();
    count_ = header.count();
  }

  std::uint64_t firstSequence() const { return firstSequence_; }
  std::uint32_t epoch() const { return epoch_; }
  std::uint16_t count() const { return count_; }
  bool heartbeat() const { return count_ == 0; }
  bool endOfSession() const { return count_ == END_OF_SESSION; }

  // Messages in order; skip names how many to pass over, for a range that overlaps what a
  // consumer already delivered.
  template <typename Handler>
  void forEach(Handler&& handler, const std::uint16_t skip = 0) {
    if (heartbeat() || endOfSession()) {
      return;
    }
    std::size_t at = headerBytes();
    for (std::uint16_t seen = 0; seen < count_; seen++) {
      std::uint16_t length = 0;
      std::memcpy(&length, bytes_ + at, sizeof length);
      if (seen >= skip) {
        handler(bytes_ + at + sizeof length, static_cast<std::size_t>(length));
      }
      at += sizeof length + length;
    }
  }

 private:
  char* bytes_;
  std::uint64_t firstSequence_ = 0;
  std::uint32_t epoch_ = 0;
  std::uint16_t count_ = 0;
};

}  // namespace exchange::common::ranges
