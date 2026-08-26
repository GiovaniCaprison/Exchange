// The standby's half of replication: consume submissions from the link, journal every command,
// mirror the dedupe windows from the envelopes, and acknowledge the highest contiguous sequence
// held, one acknowledgment per range so the return path is grouped rather than per command
// (docs/PROTOCOL.md, publication and replication ranges). What this class holds is failover's
// substance: after any acknowledged sequence its journal is a byte-identical prefix-or-more of
// the primary's, its windows answer the retries the primary would have answered, and the link's
// heartbeats tell it the published floor, which bounds the suffix a takeover republishes.

#pragma once

#include <cstdint>
#include <cstring>

#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/ReplicationAck.h"
#include "leadership.hpp"
#include "ranges.hpp"
#include "sequencer.hpp"

namespace exchange::sequencer {

inline constexpr std::size_t REPLICATION_ACK_BYTES =
    ::exchange::protocol::MessageHeader::encodedLength() +
    ::exchange::protocol::ReplicationAck::sbeBlockLength();

template <typename AckRing, typename Journal>
class Standby {
 public:
  Standby(Journal& journal, AckRing& acks, const std::uint32_t gateways = 8,
          const std::uint32_t epoch = 1)
      : journal_(journal), acks_(acks), epoch_(epoch) {
    gateways_.resize(gateways);
    for (GatewayState& gateway : gateways_) {
      gateway.key.assign(WINDOW, 0);
      gateway.sequence.assign(WINDOW, 0);
      gateway.timestamp.assign(WINDOW, 0);
    }
  }

  static constexpr std::uint64_t WINDOW = 1024;

  // One range from the link, its messages whole submissions. The link is lossless, so a range
  // past the next sequence is a violated precondition, counted and refused; an overlap is the
  // primary resending a covered prefix and is skipped, which makes the journal
  // append-exactly-once by construction.
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
      noteFloor(reader.firstSequence());
      acknowledgeHeld();
      return;
    }
    if (reader.heartbeat()) {
      // A marker names the primary's next unpublished sequence: the takeover floor.
      noteFloor(reader.firstSequence());
      acknowledgeHeld();
      return;
    }
    const std::uint16_t skip = static_cast<std::uint16_t>(held_ + 1 - reader.firstSequence());
    if (reader.count() > skip) {
      reader.forEach([&](char* record, const std::size_t size) { onSubmission(record, size); },
                     skip);
      held_ = reader.firstSequence() + reader.count() - 1;
    }
    acknowledgeHeld();
  }

  // Everything a takeover adopts: the watermark, the floor, and the mirrored windows.
  Inherited inherited() const {
    Inherited state;
    state.held = held_;
    state.publishedFloor = publishedFloor_;
    state.gateways = gateways_;
    return state;
  }

  std::uint64_t held() const { return held_; }
  std::uint64_t publishedFloor() const { return publishedFloor_; }
  std::uint32_t epoch() const { return epoch_; }
  bool ended() const { return ended_; }
  std::uint64_t violations() const { return violations_; }

 private:
  void onSubmission(char* record, const std::size_t size) {
    namespace sbe = ::exchange::protocol;
    sbe::MessageHeader wrap;
    wrap.wrap(record, 0, 0, size);
    sbe::GatewaySubmission submission;
    submission.wrapForDecode(record, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                             size);
    char* command = record + SUBMISSION_BYTES;
    const std::size_t commandLength = size - SUBMISSION_BYTES;
    journal_.append(command, static_cast<std::uint32_t>(commandLength));
    const std::uint32_t gatewayId = submission.gatewayId();
    if (gatewayId < gateways_.size()) {
      GatewayState& gateway = gateways_[gatewayId];
      const std::uint64_t gatewaySequence = submission.gatewaySequence();
      const std::uint64_t slot = gatewaySequence & (WINDOW - 1);
      std::uint64_t sequence = 0;
      std::uint64_t timestamp = 0;
      std::memcpy(&sequence, command + sbe::MessageHeader::encodedLength(), sizeof sequence);
      std::memcpy(&timestamp, command + sbe::MessageHeader::encodedLength() + sizeof sequence,
                  sizeof timestamp);
      gateway.key[slot] = gatewaySequence;
      gateway.sequence[slot] = sequence;
      gateway.timestamp[slot] = timestamp;
      gateway.nextExpected = gatewaySequence + 1;
    }
  }

  void noteFloor(const std::uint64_t floor) {
    publishedFloor_ = floor > publishedFloor_ ? floor : publishedFloor_;
  }

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
  std::uint64_t publishedFloor_ = 1;
  std::uint64_t violations_ = 0;
  bool ended_ = false;
  std::vector<GatewayState> gateways_;
};

}  // namespace exchange::sequencer
