// The gateway: where clients meet the venue (docs/components/gateway.md). Connections carry
// bytes; the machine between them and the rings is here, and it is deliberately socket free so
// every suite can drive it with scripted streams. A session opens with a login the gateway
// checks against its credential table, speaks the command vocabulary upstream with the session's
// participantId written in place, so identity is the session's fact and a client cannot speak as
// anyone else, and reads back the venue's own events filtered to what it owns, numbered
// implicitly so a reconnection is a login naming the sequence reached (docs/PROTOCOL.md, the
// session).
//
// The gateway holds no venue state (P-1). What it does hold is bookkeeping under failure: every
// forwarded submission stays in a fixed pending ring until the sequencer's acknowledgment covers
// it, and because acknowledgments are monotone per gateway, the unacknowledged set is always a
// contiguous suffix, so resubmission after a failover is a replay of that suffix under the
// original numbering, made harmless by the dedupe the new leader inherited. Time enters through
// a clock the tests can script, for heartbeats and timeouts only; the venue's one timestamp
// remains the sequencer's (P-3).

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/CommandSequenced.h"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/LoginAccepted.h"
#include "exchange_protocol/LoginRejected.h"
#include "exchange_protocol/LoginRequest.h"
#include "exchange_protocol/LogoutRequest.h"
#include "exchange_protocol/MassCancel.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRejected.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/OrderTriggered.h"
#include "exchange_protocol/ReplaceOrder.h"
#include "exchange_protocol/SessionEnded.h"
#include "exchange_protocol/SessionHeartbeat.h"
#include "sequencer.hpp"
#include "wire.hpp"

namespace exchange::gateway {

namespace sbe = ::exchange::protocol;

struct Credential {
  std::uint32_t participantId = 0;
  std::uint64_t secret = 0;
};

template <typename SubmitRing, typename Clock>
class Gateway {
 public:
  static constexpr std::size_t CONNECTIONS = 64;
  static constexpr std::uint64_t PENDING = 1 << 14;
  static constexpr std::size_t ORDERS = 1 << 17;

  Gateway(SubmitRing& submissions, Clock& clock, const std::uint32_t gatewayId,
          std::vector<Credential> credentials,
          const std::uint64_t heartbeatNanos = 1'000'000'000ULL,
          const std::uint64_t resubmitAfterNanos = 500'000'000ULL)
      : submissions_(submissions),
        clock_(clock),
        gatewayId_(gatewayId),
        credentials_(std::move(credentials)),
        heartbeat_(heartbeatNanos),
        resubmitAfter_(resubmitAfterNanos) {
    connections_.resize(CONNECTIONS);
    for (Connection& connection : connections_) {
      connection.out.reserve(OUT_BYTES);
    }
    streams_.resize(credentials_.size());
    for (Stream& stream : streams_) {
      stream.log.reserve(LOG_BYTES);
      stream.offsets.reserve(LOG_ENTRIES);
      stream.lengths.reserve(LOG_ENTRIES);
    }
    pending_.resize(PENDING);
    pendingArena_.resize(std::size_t{1} << 22);
    orderKeys_.assign(ORDERS, 0);
    orderOwners_.assign(ORDERS, 0);
    lastAck_ = clock_.now();
    newOrderParticipantAt_ = participantOffsetOf<sbe::NewOrder>();
    cancelParticipantAt_ = participantOffsetOf<sbe::CancelOrder>();
    replaceParticipantAt_ = participantOffsetOf<sbe::ReplaceOrder>();
    massCancelParticipantAt_ = participantOffsetOf<sbe::MassCancel>();
  }

  // The connection surface, driven by whatever owns the sockets ------------------------------

  int opened() {
    for (std::size_t slot = 0; slot < connections_.size(); slot++) {
      if (connections_[slot].state == State::FREE) {
        Connection& connection = connections_[slot];
        connection.state = State::AWAITING_LOGIN;
        connection.participant = NOBODY;
        connection.reassembler.reset();
        connection.out.clear();
        connection.drained = 0;
        connection.lastInbound = clock_.now();
        connection.lastOutbound = clock_.now();
        return static_cast<int>(slot);
      }
    }
    return -1;
  }

  void received(const int slot, const char* bytes, const std::size_t length) {
    Connection& connection = connections_[static_cast<std::size_t>(slot)];
    if (connection.state != State::AWAITING_LOGIN && connection.state != State::ESTABLISHED) {
      return;
    }
    connection.lastInbound = clock_.now();
    const bool sound = connection.reassembler.feed(
        bytes, length,
        [&](char* message, const std::size_t size) { onMessage(slot, message, size); });
    if (!sound) {
      // A poisoned stream gets no pleasantries: nothing it says after this can be trusted, so
      // the connection just ends.
      connection.state = State::ENDED;
      unbind(connection);
      poisoned_++;
    }
  }

  void closed(const int slot) {
    Connection& connection = connections_[static_cast<std::size_t>(slot)];
    unbind(connection);
    connection.state = State::FREE;
    connection.out.clear();
    connection.drained = 0;
  }

  // Outbound bytes awaiting the wire, and how many of them the wire took.
  std::pair<const char*, std::size_t> outbound(const int slot) const {
    const Connection& connection = connections_[static_cast<std::size_t>(slot)];
    return {connection.out.data() + connection.drained, connection.out.size() - connection.drained};
  }

  void drained(const int slot, const std::size_t bytes) {
    Connection& connection = connections_[static_cast<std::size_t>(slot)];
    connection.drained += bytes;
    if (connection.drained == connection.out.size()) {
      connection.out.clear();
      connection.drained = 0;
    }
  }

  bool wantsClose(const int slot) const {
    const Connection& connection = connections_[static_cast<std::size_t>(slot)];
    return connection.state == State::ENDED && connection.out.size() == connection.drained;
  }

  // Heartbeats out, timeouts in, and the resubmission clock, all on the injected time.
  void onTick() {
    const std::uint64_t now = clock_.now();
    for (std::size_t slot = 0; slot < connections_.size(); slot++) {
      Connection& connection = connections_[slot];
      if (connection.state == State::ESTABLISHED && now - connection.lastOutbound >= heartbeat_) {
        char message[64];
        sbe::SessionHeartbeat pulse;
        pulse.wrapAndApplyHeader(message, 0, sizeof message);
        pulse.reserved(0);
        enqueue(connection, message, encodedSize<sbe::SessionHeartbeat>());
      }
      if ((connection.state == State::ESTABLISHED || connection.state == State::AWAITING_LOGIN) &&
          now - connection.lastInbound >= DEAD * heartbeat_) {
        end(connection, connection.state == State::ESTABLISHED);
        timeouts_++;
      }
    }
    if (nextGatewaySequence_ - 1 > ackedUpTo_ && now - lastAck_ >= resubmitAfter_) {
      resubmitUnacked();
      lastAck_ = now;
    }
  }

  // Everything acknowledged and unacknowledged goes again; retry plus dedupe makes it harmless,
  // which is why the trigger can afford to be a timeout.
  void resubmitUnacked() {
    for (std::uint64_t sequence = ackedUpTo_ + 1; sequence < nextGatewaySequence_; sequence++) {
      const Pending& entry = pending_[(sequence - 1) & (PENDING - 1)];
      const std::size_t at = submissions_.claim(entry.length);
      std::memcpy(submissions_.buffer() + at, pendingArena_.data() + entry.offset, entry.length);
      submissions_.commit();
      submissions_.publish();
      resubmitted_++;
    }
  }

  // The venue surface -------------------------------------------------------------------------

  void onAck(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    sbe::CommandSequenced ack;
    ack.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(), length);
    if (ack.gatewaySequence() > ackedUpTo_) {
      ackedUpTo_ = ack.gatewaySequence();
    }
    lastAck_ = clock_.now();
  }

  void onEvent(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::OrderAccepted::sbeTemplateId(): {
        sbe::OrderAccepted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        remember(event.orderId(), event.participantId());
        deliver(event.participantId(), message, length);
        break;
      }
      case sbe::OrderRejected::sbeTemplateId(): {
        sbe::OrderRejected event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        deliver(event.participantId(), message, length);
        break;
      }
      case sbe::OrderExecuted::sbeTemplateId(): {
        sbe::OrderExecuted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const std::uint32_t aggressor = ownerOf(event.aggressorOrderId());
        const std::uint32_t resting = ownerOf(event.restingOrderId());
        if (aggressor != NOBODY) {
          deliver(aggressor, message, length);
        }
        if (resting != NOBODY && resting != aggressor) {
          deliver(resting, message, length);
        }
        break;
      }
      case sbe::OrderRested::sbeTemplateId():
      case sbe::OrderReduced::sbeTemplateId():
      case sbe::OrderRemoved::sbeTemplateId():
      case sbe::OrderTriggered::sbeTemplateId(): {
        // These four open with (context, orderId), so the owner is one map read away.
        std::uint64_t orderId = 0;
        std::memcpy(
            &orderId,
            message + sbe::MessageHeader::encodedLength() + sbe::EventContext::encodedLength(),
            sizeof orderId);
        const std::uint32_t owner = ownerOf(orderId);
        if (owner != NOBODY) {
          deliver(owner, message, length);
        }
        break;
      }
      default:
        // Session state and auction indications are the public feed's to carry; an order entry
        // session hears about its own orders and nothing else, which is OUCH's shape too.
        break;
    }
  }

  std::uint64_t submitted() const { return nextGatewaySequence_ - 1; }
  std::uint64_t acked() const { return ackedUpTo_; }
  std::uint64_t resubmitted() const { return resubmitted_; }
  std::uint64_t poisoned() const { return poisoned_; }
  std::uint64_t timeouts() const { return timeouts_; }
  std::uint64_t rejections() const { return rejections_; }
  std::uint64_t unroutable() const { return unroutable_; }

 private:
  enum class State : std::uint8_t { FREE, AWAITING_LOGIN, ESTABLISHED, ENDED };

  static constexpr std::uint32_t NOBODY = 0xFFFFFFFF;
  static constexpr std::uint64_t DEAD = 5;
  static constexpr std::size_t OUT_BYTES = 1 << 20;
  static constexpr std::size_t LOG_BYTES = 1 << 22;
  static constexpr std::size_t LOG_ENTRIES = 1 << 16;

  struct Connection {
    State state = State::FREE;
    std::uint32_t participant = NOBODY;
    Reassembler reassembler;
    std::vector<char> out;
    std::size_t drained = 0;
    std::uint64_t lastInbound = 0;
    std::uint64_t lastOutbound = 0;
  };

  // One participant's sequenced stream: framed events, kept whole so replay is a memory read.
  struct Stream {
    std::vector<char> log;
    std::vector<std::size_t> offsets;
    std::vector<std::size_t> lengths;
    int boundTo = -1;
  };

  struct Pending {
    std::uint32_t offset = 0;
    std::uint32_t length = 0;
  };

  template <typename Message>
  static constexpr std::size_t encodedSize() {
    return sbe::MessageHeader::encodedLength() + Message::sbeBlockLength();
  }

  void onMessage(const int slot, char* message, const std::size_t length) {
    Connection& connection = connections_[static_cast<std::size_t>(slot)];
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    if (wrap.schemaId() != sbe::LoginRequest::sbeSchemaId()) {
      // A well-framed message from some other world entirely; the framing was luck.
      connection.state = State::ENDED;
      unbind(connection);
      poisoned_++;
      return;
    }
    if (connection.state == State::AWAITING_LOGIN) {
      if (wrap.templateId() != sbe::LoginRequest::sbeTemplateId()) {
        connection.state = State::ENDED;
        poisoned_++;
        return;
      }
      login(slot, message, length);
      return;
    }
    switch (wrap.templateId()) {
      case sbe::NewOrder::sbeTemplateId():
        submit(connection, message, length, newOrderParticipantAt_);
        break;
      case sbe::CancelOrder::sbeTemplateId():
        submit(connection, message, length, cancelParticipantAt_);
        break;
      case sbe::ReplaceOrder::sbeTemplateId():
        submit(connection, message, length, replaceParticipantAt_);
        break;
      case sbe::MassCancel::sbeTemplateId():
        submit(connection, message, length, massCancelParticipantAt_);
        break;
      case sbe::SessionHeartbeat::sbeTemplateId():
        break;
      case sbe::LogoutRequest::sbeTemplateId():
        end(connection, true);
        break;
      default:
        // Session control, instrument definitions, or anything else a client has no business
        // sending: the vocabulary is the permission, and speaking outside it ends the session.
        connection.state = State::ENDED;
        unbind(connection);
        poisoned_++;
        break;
    }
  }

  void login(const int slot, char* message, const std::size_t length) {
    Connection& connection = connections_[static_cast<std::size_t>(slot)];
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    sbe::LoginRequest request;
    request.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          length);

    const Credential* found = nullptr;
    std::size_t streamAt = 0;
    for (std::size_t at = 0; at < credentials_.size(); at++) {
      if (credentials_[at].participantId == request.participantId()) {
        found = &credentials_[at];
        streamAt = at;
        break;
      }
    }
    if (found == nullptr) {
      reject(connection, sbe::LoginRefusal::UNKNOWN_PARTICIPANT);
      return;
    }
    if (found->secret != request.credential()) {
      reject(connection, sbe::LoginRefusal::BAD_CREDENTIAL);
      return;
    }
    Stream& stream = streams_[streamAt];
    if (stream.boundTo >= 0) {
      reject(connection, sbe::LoginRefusal::ALREADY_LOGGED_IN);
      return;
    }
    const std::uint64_t from = request.expectedSequence() == 0 ? 1 : request.expectedSequence();
    if (from > stream.offsets.size() + 1) {
      reject(connection, sbe::LoginRefusal::SEQUENCE_AHEAD);
      return;
    }

    connection.state = State::ESTABLISHED;
    connection.participant = request.participantId();
    stream.boundTo = slot;
    char answer[64];
    sbe::LoginAccepted accepted;
    accepted.wrapAndApplyHeader(answer, 0, sizeof answer);
    accepted.nextSequence(from).participantId(request.participantId()).reserved(0);
    enqueue(connection, answer, encodedSize<sbe::LoginAccepted>());
    // The replay: everything the participant's stream holds from the sequence asked for, byte
    // exactly as first sent; the log keeps frames whole, so this is raw bytes out.
    for (std::size_t at = from - 1; at < stream.offsets.size(); at++) {
      raw(connection, stream.log.data() + stream.offsets[at], stream.lengths[at]);
    }
  }

  void reject(Connection& connection, const sbe::LoginRefusal::Value reason) {
    char answer[64];
    sbe::LoginRejected rejected;
    rejected.wrapAndApplyHeader(answer, 0, sizeof answer);
    rejected.reason(reason);
    enqueue(connection, answer, encodedSize<sbe::LoginRejected>());
    connection.state = State::ENDED;
    rejections_++;
  }

  void end(Connection& connection, const bool politely) {
    if (politely) {
      char last[64];
      sbe::SessionEnded ended;
      ended.wrapAndApplyHeader(last, 0, sizeof last);
      ended.reserved(0);
      enqueue(connection, last, encodedSize<sbe::SessionEnded>());
    }
    connection.state = State::ENDED;
    unbind(connection);
  }

  void unbind(Connection& connection) {
    if (connection.participant != NOBODY) {
      for (Stream& stream : streams_) {
        if (stream.boundTo >= 0 &&
            &connections_[static_cast<std::size_t>(stream.boundTo)] == &connection) {
          stream.boundTo = -1;
        }
      }
      connection.participant = NOBODY;
    }
  }

  // Where each command keeps its participantId, taken from the codecs so a schema change moves
  // it here by failing loudly rather than by drifting.
  template <typename Command>
  static std::size_t participantOffsetOf();

  void submit(Connection& connection, char* command, const std::size_t length,
              const std::size_t participantOffset) {
    // Identity is the session's fact: whatever the client wrote here is overwritten.
    std::memcpy(command + participantOffset, &connection.participant,
                sizeof connection.participant);
    const std::uint64_t gatewaySequence = nextGatewaySequence_++;
    if (gatewaySequence - ackedUpTo_ > PENDING) {
      throw std::runtime_error("the gateway's pending window overflowed; the venue is gone");
    }
    const std::size_t record = sequencer::SUBMISSION_BYTES + length;
    const std::size_t at = submissions_.claim(record);
    sbe::GatewaySubmission envelope;
    envelope.wrapAndApplyHeader(submissions_.buffer(), at, at + record);
    envelope.gatewaySequence(gatewaySequence).gatewayId(gatewayId_).reserved(0);
    std::memcpy(submissions_.buffer() + at + sequencer::SUBMISSION_BYTES, command, length);
    submissions_.commit();
    submissions_.publish();
    // The same bytes wait in the pending ring for the acknowledgment that retires them.
    if (arenaIn_ + record > pendingArena_.size()) {
      arenaIn_ = 0;
    }
    std::memcpy(pendingArena_.data() + arenaIn_, submissions_.buffer() + at, record);
    Pending& entry = pending_[(gatewaySequence - 1) & (PENDING - 1)];
    entry.offset = static_cast<std::uint32_t>(arenaIn_);
    entry.length = static_cast<std::uint32_t>(record);
    arenaIn_ += record;
  }

  void remember(const std::uint64_t orderId, const std::uint32_t participant) {
    if (++orderCount_ > (ORDERS * 3) / 4) {
      throw std::runtime_error("the gateway's order table overflowed; size it for the session");
    }
    std::size_t slot = orderId & (ORDERS - 1);
    while (orderKeys_[slot] != 0) {
      slot = (slot + 1) & (ORDERS - 1);
    }
    orderKeys_[slot] = orderId;
    orderOwners_[slot] = participant;
  }

  std::uint32_t ownerOf(const std::uint64_t orderId) {
    std::size_t slot = orderId & (ORDERS - 1);
    while (orderKeys_[slot] != 0) {
      if (orderKeys_[slot] == orderId) {
        return orderOwners_[slot];
      }
      slot = (slot + 1) & (ORDERS - 1);
    }
    unroutable_++;
    return NOBODY;
  }

  void deliver(const std::uint32_t participant, const char* message, const std::size_t length) {
    for (std::size_t at = 0; at < credentials_.size(); at++) {
      if (credentials_[at].participantId != participant) {
        continue;
      }
      Stream& stream = streams_[at];
      if (stream.log.size() + length + 2 > LOG_BYTES || stream.offsets.size() == LOG_ENTRIES) {
        throw std::runtime_error("a session stream overflowed; size the log for the session");
      }
      const std::size_t start = stream.log.size();
      const std::uint16_t prefix = static_cast<std::uint16_t>(length);
      stream.log.insert(stream.log.end(), reinterpret_cast<const char*>(&prefix),
                        reinterpret_cast<const char*>(&prefix) + sizeof prefix);
      stream.log.insert(stream.log.end(), message, message + length);
      stream.offsets.push_back(start);
      stream.lengths.push_back(length + sizeof prefix);
      if (stream.boundTo >= 0) {
        Connection& connection = connections_[static_cast<std::size_t>(stream.boundTo)];
        raw(connection, stream.log.data() + start, length + sizeof prefix);
      }
      return;
    }
    unroutable_++;
  }

  // A frame into a connection's outbound bytes; the log keeps frames whole, so replay reuses raw.
  void enqueue(Connection& connection, const char* message, const std::size_t length) {
    const std::uint16_t prefix = static_cast<std::uint16_t>(length);
    raw(connection, reinterpret_cast<const char*>(&prefix), sizeof prefix);
    raw(connection, message, length);
  }

  void raw(Connection& connection, const char* bytes, const std::size_t length) {
    if (connection.state == State::ENDED || connection.state == State::FREE) {
      return;
    }
    if (connection.out.size() + length > OUT_BYTES) {
      // A client that cannot drain its own reports is ended rather than buffered forever.
      connection.state = State::ENDED;
      unbind(connection);
      return;
    }
    connection.out.insert(connection.out.end(), bytes, bytes + length);
    connection.lastOutbound = clock_.now();
  }

  SubmitRing& submissions_;
  Clock& clock_;
  std::uint32_t gatewayId_;
  std::vector<Credential> credentials_;
  std::uint64_t heartbeat_;
  std::uint64_t resubmitAfter_;
  std::vector<Connection> connections_;
  std::vector<Stream> streams_;
  std::vector<Pending> pending_;
  std::vector<char> pendingArena_;
  std::size_t arenaIn_ = 0;
  std::uint64_t nextGatewaySequence_ = 1;
  std::uint64_t ackedUpTo_ = 0;
  std::uint64_t lastAck_ = 0;
  std::vector<std::uint64_t> orderKeys_;
  std::vector<std::uint32_t> orderOwners_;
  std::size_t orderCount_ = 0;
  std::size_t newOrderParticipantAt_ = 0;
  std::size_t cancelParticipantAt_ = 0;
  std::size_t replaceParticipantAt_ = 0;
  std::size_t massCancelParticipantAt_ = 0;
  std::uint64_t resubmitted_ = 0;
  std::uint64_t poisoned_ = 0;
  std::uint64_t rejections_ = 0;
  std::uint64_t timeouts_ = 0;
  std::uint64_t unroutable_ = 0;
};

// The participantId field offsets, one specialisation per command the vocabulary permits.
template <typename SubmitRing, typename Clock>
template <typename Command>
std::size_t Gateway<SubmitRing, Clock>::participantOffsetOf() {
  char probe[128] = {};
  Command command;
  command.wrapAndApplyHeader(probe, 0, sizeof probe);
  command.participantId(0xA5A5A5A5U);
  for (std::size_t at = 0; at + sizeof(std::uint32_t) <= sizeof probe; at++) {
    std::uint32_t read = 0;
    std::memcpy(&read, probe + at, sizeof read);
    if (read == 0xA5A5A5A5U) {
      return at;
    }
  }
  throw std::logic_error("no participantId in a permitted command");
}

}  // namespace exchange::gateway
