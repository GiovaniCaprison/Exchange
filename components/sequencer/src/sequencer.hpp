// The sequencer's core: where nondeterminism becomes a recorded fact. One thread consumes
// gateway submissions, and for each fresh one commits the two choices nothing downstream may
// make again, its place in the global order and the venue's one timestamp, writing both into the
// command's context in place so the journaled and published stream is made of ordinary commands
// (docs/PROTOCOL.md, the submission plane). Exactly-once is retry plus deduplication: a gateway
// that never saw its acknowledgment resubmits under the same gatewaySequence, and the window of
// past acknowledgments answers the retry with the place the command already holds.
//
// Durability is the order of operations per fresh command, and the policy names who must hold a
// command before the world may know it. Under the local policy the journal write gates
// publication and acknowledgment; under the safe policy the standby's replication acknowledgment
// does, so published is a subset of replicated by construction and the standby always holds
// everything any consumer has seen. The pipeline never stalls per command: sequencing and
// shipping continue while acknowledgments stream back, and everything the watermark covers is
// published in order, which is what pipelined replication with grouped acknowledgments means.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
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

enum class Durability { LOCAL, SAFE };

// A replication link that goes nowhere, for deployments and proofs that run without a standby.
struct NullLink {
  std::size_t claim(std::size_t) { return 0; }
  char* buffer() { return space_; }
  void commit() {}
  void publish() {}
  char space_[common::ranges::PACKET_BYTES + 8] = {};
};

template <typename OutRing, typename AckRing, typename PacketSink, typename Journal, typename Clock,
          typename Link>
class Sequencer {
 public:
  // Past acknowledgments a gateway can still be re-answered from. A retry older than the window
  // is counted and dropped: a gateway that far behind its own acknowledgments is resynchronising,
  // which is failover's problem rather than dedupe's.
  static constexpr std::uint64_t WINDOW = 1024;

  // Commands sequenced and shipped but not yet covered by the standby's acknowledgment. The
  // capacity is an engineering bound sized far past any healthy link's flight; hitting it means
  // the standby died mid-session, which the lease machinery owns rather than this queue.
  static constexpr std::uint64_t PIPELINE = 1 << 15;

  Sequencer(OutRing& out, std::vector<AckRing>& acks, PacketSink& packets, Journal& journal,
            Clock& clock, Link& link, const Durability policy = Durability::LOCAL,
            const std::uint32_t epoch = 1)
      : out_(out),
        acks_(acks),
        packets_(packets),
        journal_(journal),
        clock_(clock),
        link_(link),
        policy_(policy),
        epoch_(epoch),
        builder_(packet_, sizeof packet_) {
    gateways_.resize(acks.size());
    for (Gateway& gateway : gateways_) {
      gateway.key.assign(WINDOW, 0);
      gateway.sequence.assign(WINDOW, 0);
      gateway.timestamp.assign(WINDOW, 0);
    }
    pending_.resize(PIPELINE);
    arena_.resize(std::size_t{1} << 23);
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
      ship(sequence_, command, commandLength);
      const std::uint64_t slot = gatewaySequence & (WINDOW - 1);
      gateway.key[slot] = gatewaySequence;
      gateway.sequence[slot] = sequence_;
      gateway.timestamp[slot] = timestamp;
      gateway.nextExpected++;
      if (policy_ == Durability::LOCAL) {
        acked_ = sequence_;
        publishAt(sequence_, command, commandLength);
        acknowledge(gatewayId, gatewaySequence, sequence_, timestamp);
      } else {
        enqueue(sequence_, gatewayId, gatewaySequence, timestamp, command, commandLength);
      }
      return;
    }
    if (gatewaySequence < gateway.nextExpected) {
      // A resubmission: the earlier answer stands and is repeated, because sequencing it again
      // would hand one command two places in the order. An answer is owed only once it is
      // durable: a retry of a command still in the pipeline stays unanswered, and the gateway's
      // next retry finds it covered, which is retry plus dedupe carrying the wait too.
      const std::uint64_t slot = gatewaySequence & (WINDOW - 1);
      if (gateway.key[slot] == gatewaySequence && gateway.sequence[slot] <= acked_) {
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

  // The standby holds everything up to here: publish and acknowledge, in order, all of it.
  void onReplicationAck(const std::uint64_t upToSequence) {
    if (upToSequence > acked_) {
      acked_ = upToSequence;
    }
    while (pendingCount_ > 0) {
      const Pending& entry = pending_[pendingHead_ & (PIPELINE - 1)];
      if (entry.sequence > acked_) {
        break;
      }
      publishAt(entry.sequence, arena_.data() + entry.offset, entry.length);
      acknowledge(entry.gatewayId, entry.gatewaySequence, entry.sequence, entry.timestamp);
      arenaUsed_ -= entry.length;
      pendingHead_++;
      pendingCount_--;
    }
  }

  // The pending range leaves as one packet; on the wire a range is a batch or nothing.
  void flush() {
    if (!builder_.isOpen()) {
      return;
    }
    packets_.send(packet_, builder_.close());
  }

  // An empty range naming the next unpublished sequence, so a silent wire is distinguishable
  // from a lossy one; the standby hears the same pulse on its own carrier.
  void heartbeat() {
    flush();
    builder_.open(published_ + 1, epoch_);
    packets_.send(packet_, builder_.close());
    shipMarker(published_ + 1, false);
  }

  void endSession() {
    if (pendingCount_ != 0) {
      // Closing the session over unpublished commands would put the end inside the stream.
      throw std::runtime_error("end of session with the pipeline still holding commands");
    }
    flush();
    builder_.open(published_ + 1, epoch_);
    packets_.send(packet_, builder_.closeEndOfSession());
    shipMarker(published_ + 1, true);
  }

  std::uint64_t sequence() const { return sequence_; }
  std::uint64_t published() const { return published_; }
  std::uint64_t acked() const { return acked_; }
  std::uint64_t pending() const { return pendingCount_; }
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

  struct Pending {
    std::uint64_t sequence = 0;
    std::uint64_t gatewaySequence = 0;
    std::uint64_t timestamp = 0;
    std::uint32_t gatewayId = 0;
    std::uint32_t offset = 0;
    std::uint32_t length = 0;
  };

  // One command to the standby as a one-message range, shipped the moment it is sequenced, so
  // the link pipelines instead of waiting on batches.
  void ship(const std::uint64_t sequence, const char* command, const std::size_t length) {
    char range[common::ranges::PACKET_BYTES];
    common::ranges::Builder builder(range, sizeof range);
    builder.open(sequence, epoch_);
    builder.add(command, static_cast<std::uint16_t>(length));
    const std::size_t built = builder.close();
    const std::size_t at = link_.claim(built);
    std::memcpy(link_.buffer() + at, range, built);
    link_.commit();
    link_.publish();
  }

  void shipMarker(const std::uint64_t firstSequence, const bool end) {
    char range[common::ranges::PACKET_BYTES];
    common::ranges::Builder builder(range, sizeof range);
    builder.open(firstSequence, epoch_);
    const std::size_t built = end ? builder.closeEndOfSession() : builder.close();
    const std::size_t at = link_.claim(built);
    std::memcpy(link_.buffer() + at, range, built);
    link_.commit();
    link_.publish();
  }

  void enqueue(const std::uint64_t sequence, const std::uint32_t gatewayId,
               const std::uint64_t gatewaySequence, const std::uint64_t timestamp,
               const char* command, const std::size_t length) {
    if (pendingCount_ == PIPELINE ||
        arenaUsed_ + length + common::ranges::PACKET_BYTES > arena_.size()) {
      throw std::runtime_error("the replication pipeline overflowed; the standby is gone");
    }
    if (arenaIn_ + length > arena_.size()) {
      arenaIn_ = 0;
    }
    std::memcpy(arena_.data() + arenaIn_, command, length);
    Pending& entry = pending_[(pendingHead_ + pendingCount_) & (PIPELINE - 1)];
    entry.sequence = sequence;
    entry.gatewaySequence = gatewaySequence;
    entry.timestamp = timestamp;
    entry.gatewayId = gatewayId;
    entry.offset = static_cast<std::uint32_t>(arenaIn_);
    entry.length = static_cast<std::uint32_t>(length);
    arenaIn_ += length;
    arenaUsed_ += length;
    pendingCount_++;
  }

  void publishAt(const std::uint64_t sequence, const char* command, const std::size_t length) {
    const std::size_t at = out_.claim(length);
    std::memcpy(out_.buffer() + at, command, length);
    out_.commit();
    out_.publish();
    if (builder_.isOpen() && !builder_.fits(length)) {
      flush();
    }
    if (!builder_.isOpen()) {
      builder_.open(sequence, epoch_);
    }
    builder_.add(command, static_cast<std::uint16_t>(length));
    published_ = sequence;
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
  Link& link_;
  Durability policy_;
  std::uint32_t epoch_;
  std::uint64_t sequence_ = 0;
  std::uint64_t published_ = 0;
  std::uint64_t acked_ = 0;
  std::uint64_t duplicates_ = 0;
  std::uint64_t dropped_ = 0;
  std::uint64_t violations_ = 0;
  std::vector<Gateway> gateways_;
  std::vector<Pending> pending_;
  std::vector<char> arena_;
  std::uint64_t pendingHead_ = 0;
  std::uint64_t pendingCount_ = 0;
  std::size_t arenaIn_ = 0;
  std::size_t arenaUsed_ = 0;
  char packet_[common::ranges::PACKET_BYTES] = {};
  common::ranges::Builder builder_;
};

}  // namespace exchange::sequencer
