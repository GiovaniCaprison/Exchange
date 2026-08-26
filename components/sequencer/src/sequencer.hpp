// The sequencer's core: where nondeterminism becomes a recorded fact. One thread consumes
// gateway submissions, and for each fresh one commits the two choices nothing downstream may
// make again, its place in the global order and the venue's one timestamp, writing both into the
// command's context in place so the journaled and published stream is made of ordinary commands
// (docs/PROTOCOL.md, the submission plane). Exactly-once is retry plus deduplication: a gateway
// that never saw its acknowledgment resubmits under the same gatewaySequence, and the window of
// past acknowledgments answers the retry with the place the command already holds.
//
// The order of operations per fresh command is the durability contract: stamp, journal, then
// acknowledge and publish, so an acknowledged command survives this process's death under the
// local policy. The replication link tightens the same order to the safe policy later in the
// component's arc (docs/components/sequencer.md).

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "exchange_protocol/CommandSequenced.h"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/MessageHeader.h"
#include "ranges.hpp"

namespace exchange::sequencer {

namespace sbe = ::exchange::protocol;

inline constexpr std::size_t SUBMISSION_BYTES =
    sbe::MessageHeader::encodedLength() + sbe::GatewaySubmission::sbeBlockLength();
inline constexpr std::size_t ACK_BYTES =
    sbe::MessageHeader::encodedLength() + sbe::CommandSequenced::sbeBlockLength();

template <typename OutRing, typename AckRing, typename PacketSink, typename Journal, typename Clock>
class Sequencer {
 public:
  // Past acknowledgments a gateway can still be re-answered from. A retry older than the window
  // is counted and dropped: a gateway that far behind its own acknowledgments is resynchronising,
  // which is failover's problem rather than dedupe's.
  static constexpr std::uint64_t WINDOW = 1024;

  Sequencer(OutRing& out, std::vector<AckRing>& acks, PacketSink& packets, Journal& journal,
            Clock& clock, const std::uint32_t epoch = 1)
      : out_(out),
        acks_(acks),
        packets_(packets),
        journal_(journal),
        clock_(clock),
        epoch_(epoch),
        builder_(packet_, sizeof packet_) {
    gateways_.resize(acks.size());
    for (Gateway& gateway : gateways_) {
      gateway.key.assign(WINDOW, 0);
      gateway.sequence.assign(WINDOW, 0);
      gateway.timestamp.assign(WINDOW, 0);
    }
  }

  // One submission-plane record: a framed GatewaySubmission and the framed command it prefixes.
  void onSubmission(char* bytes, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(bytes, 0, 0, length);
    if (wrap.templateId() != sbe::GatewaySubmission::sbeTemplateId() ||
        length <= SUBMISSION_BYTES) {
      violations_++;
      return;
    }
    sbe::GatewaySubmission submission;
    submission.wrapForDecode(bytes, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                             length);
    const std::uint64_t gatewaySequence = submission.gatewaySequence();
    const std::uint32_t gatewayId = submission.gatewayId();
    if (gatewayId >= gateways_.size()) {
      violations_++;
      return;
    }
    Gateway& gateway = gateways_[gatewayId];

    if (gatewaySequence == gateway.nextExpected) {
      char* command = bytes + SUBMISSION_BYTES;
      const std::size_t commandLength = length - SUBMISSION_BYTES;
      sequence_++;
      const std::uint64_t timestamp = clock_.now();
      // The two committed choices, written where every consumer will read them: a command's
      // context opens its body with sequence then timestamp.
      constexpr std::size_t context = sbe::MessageHeader::encodedLength();
      std::memcpy(command + context, &sequence_, sizeof sequence_);
      std::memcpy(command + context + sizeof sequence_, &timestamp, sizeof timestamp);
      journal_.append(command, static_cast<std::uint32_t>(commandLength));
      publish(command, commandLength);
      const std::uint64_t slot = gatewaySequence & (WINDOW - 1);
      gateway.key[slot] = gatewaySequence;
      gateway.sequence[slot] = sequence_;
      gateway.timestamp[slot] = timestamp;
      gateway.nextExpected++;
      acknowledge(gatewayId, gatewaySequence, sequence_, timestamp);
      return;
    }
    if (gatewaySequence < gateway.nextExpected) {
      // A resubmission: the earlier answer stands and is repeated, because sequencing it again
      // would hand one command two places in the order.
      const std::uint64_t slot = gatewaySequence & (WINDOW - 1);
      if (gateway.key[slot] == gatewaySequence) {
        duplicates_++;
        acknowledge(gatewayId, gatewaySequence, gateway.sequence[slot], gateway.timestamp[slot]);
      } else {
        dropped_++;
      }
      return;
    }
    // A gap on a lossless carrier is a violated precondition; refusing it keeps dedupe sound.
    dropped_++;
  }

  // The pending range leaves as one packet; on the wire a range is a batch or nothing.
  void flush() {
    if (!builder_.isOpen()) {
      return;
    }
    packets_.send(packet_, builder_.close());
  }

  // An empty range naming the next sequence, so a silent wire is distinguishable from a lossy
  // one.
  void heartbeat() {
    flush();
    builder_.open(sequence_ + 1, epoch_);
    packets_.send(packet_, builder_.close());
  }

  void endSession() {
    flush();
    builder_.open(sequence_ + 1, epoch_);
    packets_.send(packet_, builder_.closeEndOfSession());
  }

  std::uint64_t sequence() const { return sequence_; }
  std::uint64_t duplicates() const { return duplicates_; }
  std::uint64_t dropped() const { return dropped_; }
  std::uint64_t violations() const { return violations_; }
  std::uint32_t epoch() const { return epoch_; }

 private:
  struct Gateway {
    std::uint64_t nextExpected = 1;
    std::vector<std::uint64_t> key;
    std::vector<std::uint64_t> sequence;
    std::vector<std::uint64_t> timestamp;
  };

  void publish(const char* command, const std::size_t length) {
    const std::size_t at = out_.claim(length);
    std::memcpy(out_.buffer() + at, command, length);
    out_.commit();
    out_.publish();
    if (builder_.isOpen() && !builder_.fits(length)) {
      flush();
    }
    if (!builder_.isOpen()) {
      builder_.open(sequence_, epoch_);
    }
    builder_.add(command, static_cast<std::uint16_t>(length));
  }

  void acknowledge(const std::uint32_t gatewayId, const std::uint64_t gatewaySequence,
                   const std::uint64_t sequence, const std::uint64_t timestamp) {
    AckRing& ring = acks_[gatewayId];
    const std::size_t at = ring.claim(ACK_BYTES);
    sbe::CommandSequenced ack;
    ack.wrapAndApplyHeader(ring.buffer(), at, at + ACK_BYTES);
    ack.gatewaySequence(gatewaySequence).sequence(sequence).timestamp(timestamp);
    ring.commit();
    ring.publish();
  }

  OutRing& out_;
  std::vector<AckRing>& acks_;
  PacketSink& packets_;
  Journal& journal_;
  Clock& clock_;
  std::uint32_t epoch_;
  std::uint64_t sequence_ = 0;
  std::uint64_t duplicates_ = 0;
  std::uint64_t dropped_ = 0;
  std::uint64_t violations_ = 0;
  std::vector<Gateway> gateways_;
  char packet_[common::ranges::PACKET_BYTES] = {};
  common::ranges::Builder builder_;
};

}  // namespace exchange::sequencer
