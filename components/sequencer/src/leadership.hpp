// Leadership: at most one sequencer extends the log, and two nodes cannot decide that alone, so
// a witness grants epoch-numbered leases (docs/PROTOCOL.md, leadership). The witness's whole law
// fits in one method: a holder renews its own epoch; a higher epoch is granted only when the
// standing lease has expired, so a live primary cannot be usurped and an expired one has, by the
// clock both sides share the shape of, already stopped publishing. Raft's terms are the general
// form this fixed-role deployment simplifies (Ongaro and Ousterhout 2014).
//
// The state a takeover inherits travels here too: the standby mirrors the primary's dedupe
// windows from the replication link's envelopes, and the new leader adopts them, which is what
// keeps a gateway's post-failover resubmission harmless in the one dangerous case, a command
// replicated but never acknowledged.

#pragma once

#include <cstdint>
#include <vector>

namespace exchange::sequencer {

// One gateway's dedupe state, as the primary keeps it and the standby mirrors it.
struct GatewayState {
  std::uint64_t nextExpected = 1;
  std::vector<std::uint64_t> key;
  std::vector<std::uint64_t> sequence;
  std::vector<std::uint64_t> timestamp;
};

// Everything a takeover adopts from the standby it was.
struct Inherited {
  std::uint64_t held = 0;
  std::uint64_t publishedFloor = 1;
  std::vector<GatewayState> gateways;
};

template <typename Clock>
class Witness {
 public:
  struct Answer {
    bool granted = false;
    std::uint32_t epoch = 0;
    std::uint32_t holder = 0;
    std::uint64_t ttl = 0;
  };

  Witness(Clock& clock, const std::uint64_t ttl) : clock_(clock), ttl_(ttl) {}

  Answer onRequest(const std::uint32_t nodeId, const std::uint32_t epoch,
                   const std::uint64_t lastSequence) {
    const std::uint64_t now = clock_.now();
    lastSeen_ = lastSequence > lastSeen_ ? lastSequence : lastSeen_;
    if (epoch == epoch_ && nodeId == holder_) {
      expiry_ = now + ttl_;
      return {true, epoch_, holder_, ttl_};
    }
    if (epoch > epoch_ && now >= expiry_) {
      epoch_ = epoch;
      holder_ = nodeId;
      expiry_ = now + ttl_;
      return {true, epoch_, holder_, ttl_};
    }
    return {false, epoch_, holder_, expiry_ > now ? expiry_ - now : 0};
  }

  std::uint32_t holder() const { return holder_; }
  std::uint32_t epoch() const { return epoch_; }

 private:
  Clock& clock_;
  std::uint64_t ttl_;
  std::uint32_t holder_ = 0;
  std::uint32_t epoch_ = 0;
  std::uint64_t expiry_ = 0;
  std::uint64_t lastSeen_ = 0;
};

// The node's side of the lease: whether it is held right now, when renewal is due, and what the
// witness's answer did to it. A sequencer whose lease is out must stop sequencing, which is the
// process loop's gate rather than this class's; the class only tells the truth about time.
template <typename Clock>
class Lease {
 public:
  Lease(Clock& clock, const std::uint32_t nodeId) : clock_(clock), nodeId_(nodeId) {}

  bool held() const { return clock_.now() < expiry_; }
  bool renewDue() const { return clock_.now() + ttl_ / 3 >= expiry_; }
  std::uint32_t epoch() const { return epoch_; }
  std::uint32_t nodeId() const { return nodeId_; }

  template <typename Answer>
  void onAnswer(const Answer& answer) {
    if (answer.granted && answer.holder == nodeId_) {
      epoch_ = answer.epoch;
      ttl_ = answer.ttl;
      expiry_ = clock_.now() + answer.ttl;
    }
  }

 private:
  Clock& clock_;
  std::uint32_t nodeId_;
  std::uint32_t epoch_ = 0;
  std::uint64_t ttl_ = 0;
  std::uint64_t expiry_ = 0;
};

}  // namespace exchange::sequencer
