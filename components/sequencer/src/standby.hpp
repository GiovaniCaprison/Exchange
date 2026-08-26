// The standby's half of replication: consume ranges from the link, journal every command, and
// acknowledge the highest contiguous sequence held, one acknowledgment per range so the return
// path is grouped rather than per command (docs/PROTOCOL.md, publication and replication
// ranges). What this class holds is the failover invariant's substance: after any acknowledged
// sequence, its journal is a byte-identical prefix-or-more of the primary's, so a takeover
// publishes the suffix and continues, transferring nothing.

#pragma once

#include <cstdint>
#include <cstring>

#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/ReplicationAck.h"
#include "ranges.hpp"

namespace exchange::sequencer {

inline constexpr std::size_t REPLICATION_ACK_BYTES =
    ::exchange::protocol::MessageHeader::encodedLength() +
    ::exchange::protocol::ReplicationAck::sbeBlockLength();

template <typename AckRing, typename Journal>
class Standby {
 public:
  Standby(Journal& journal, AckRing& acks, const std::uint32_t epoch = 1)
      : journal_(journal), acks_(acks), epoch_(epoch) {}

  // One range from the link. The link is lossless, so a range past the next sequence is a
  // violated precondition, counted and refused; an overlap is the primary resending a covered
  // prefix and is skipped, which makes the journal append-exactly-once by construction.
  void onRange(char* bytes, const std::size_t length) {
    common::ranges::Reader reader(bytes, length);
    if (reader.epoch() < epoch_) {
      violations_++;
      return;
    }
    epoch_ = reader.epoch();
    if (reader.firstSequence() > held_ + 1) {
      violations_++;
      return;
    }
    if (reader.endOfSession()) {
      ended_ = true;
      acknowledgeHeld();
      return;
    }
    if (!reader.heartbeat()) {
      const std::uint16_t skip = static_cast<std::uint16_t>(held_ + 1 - reader.firstSequence());
      if (reader.count() > skip) {
        reader.forEach(
            [&](char* message, const std::size_t size) {
              journal_.append(message, static_cast<std::uint32_t>(size));
            },
            skip);
        held_ = reader.firstSequence() + reader.count() - 1;
      }
    }
    acknowledgeHeld();
  }

  std::uint64_t held() const { return held_; }
  std::uint32_t epoch() const { return epoch_; }
  bool ended() const { return ended_; }
  std::uint64_t violations() const { return violations_; }

 private:
  void acknowledgeHeld() {
    namespace sbe = ::exchange::protocol;
    const std::size_t at = acks_.claim(REPLICATION_ACK_BYTES);
    sbe::ReplicationAck ack;
    ack.wrapAndApplyHeader(acks_.buffer(), at, at + REPLICATION_ACK_BYTES);
    ack.upToSequence(held_).epoch(epoch_).reserved(0);
    acks_.commit();
    acks_.publish();
  }

  Journal& journal_;
  AckRing& acks_;
  std::uint32_t epoch_;
  std::uint64_t held_ = 0;
  std::uint64_t violations_ = 0;
  bool ended_ = false;
};

}  // namespace exchange::sequencer
