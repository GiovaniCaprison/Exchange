// The in-process standby loopback the offline proofs and harnesses share: the link holds one
// range, the acknowledgment cell holds one answer, and a pump moves both, once per submission.
// Deterministic because it is single threaded, allocation free because both cells are fixed, and
// honest about what it is: the pipeline's machinery measured without a wire, which brackets the
// safe policy's cost from below until the campaign runs it across real cores.

#pragma once

#include <cstdint>
#include <cstring>

#include "exchange_protocol/MessageHeader.h"
#include "ranges.hpp"
#include "standby.hpp"

namespace exchange::sequencer {

// The link's primary side: a ring-producer surface over one fixed cell.
class LoopbackLink {
 public:
  std::size_t claim(const std::size_t length) {
    length_ = length;
    return 0;
  }
  char* buffer() { return space_; }
  void commit() {}
  void publish() { full_ = true; }

  bool full() const { return full_; }
  char* range() { return space_; }
  std::size_t length() const { return length_; }
  void drain() { full_ = false; }

 private:
  char space_[common::ranges::PACKET_BYTES + 8] = {};
  std::size_t length_ = 0;
  bool full_ = false;
};

// The standby's acknowledgment carrier: one cell, read as the watermark it names.
class LoopbackAcks {
 public:
  std::size_t claim(std::size_t) { return 0; }
  char* buffer() { return space_; }
  void commit() {}
  void publish() { full_ = true; }

  bool full() const { return full_; }
  std::uint64_t take() {
    full_ = false;
    std::uint64_t upToSequence = 0;
    std::memcpy(&upToSequence, space_ + ::exchange::protocol::MessageHeader::encodedLength(),
                sizeof upToSequence);
    return upToSequence;
  }

 private:
  char space_[64] = {};
  bool full_ = false;
};

// One pump: whatever the primary shipped reaches the standby, and whatever the standby answered
// reaches the primary.
template <typename Primary, typename Journal>
void pumpLoopback(Primary& primary, LoopbackLink& link, Standby<LoopbackAcks, Journal>& standby,
                  LoopbackAcks& acks) {
  if (link.full()) {
    standby.onRange(link.range(), link.length());
    link.drain();
  }
  if (acks.full()) {
    primary.onReplicationAck(acks.take());
  }
}

}  // namespace exchange::sequencer
