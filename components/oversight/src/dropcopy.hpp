// Drop copy (docs/components/oversight.md): the read-only session server beside the gateway. A
// firm responsible for a participant logs in over the session plane naming that participant as
// its scope, hears the participant's events framed and implicitly numbered exactly as the
// trading session does, and reconnects by naming the sequence it reached, replayed byte exactly
// from the retained per-scope stream. The machine is socket free like the gateway, driven by
// whatever owns the sockets, and holds the gateway's session law: credentials checked, typed
// refusals, heartbeats both ways, silence kills. The one difference is the plane's whole point:
// a drop copy session may speak nothing but session plane, and a command poisons it, because the
// channel exists so the client cannot touch it.

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "exchange_protocol/LoginAccepted.h"
#include "exchange_protocol/LoginRejected.h"
#include "exchange_protocol/LoginRequest.h"
#include "exchange_protocol/LogoutRequest.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRejected.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/OrderTriggered.h"
#include "exchange_protocol/SessionEnded.h"
#include "exchange_protocol/SessionHeartbeat.h"
#include "ownership.hpp"
#include "wire.hpp"

namespace exchange::oversight {

namespace sbe = ::exchange::protocol;

// Who may watch whom: the watcher's credential opens a session scoped to one participant.
struct Watcher {
  std::uint32_t scopeParticipant = 0;
  std::uint64_t credential = 0;
};

template <typename Clock>
class DropCopy {
 public:
  static constexpr std::size_t CONNECTIONS = 16;

  DropCopy(Clock& clock, std::vector<Watcher> watchers,
           const std::uint64_t heartbeatNanos = 1'000'000'000ULL)
      : clock_(clock), watchers_(std::move(watchers)), heartbeat_(heartbeatNanos) {
    connections_.resize(CONNECTIONS);
    for (Connection& connection : connections_) {
      connection.out.reserve(OUT_BYTES);
    }
    streams_.resize(watchers_.size());
    for (Stream& stream : streams_) {
      stream.log.reserve(LOG_BYTES);
      stream.offsets.reserve(LOG_ENTRIES);
      stream.lengths.reserve(LOG_ENTRIES);
    }
  }

  // The connection surface, driven by whatever owns the sockets ------------------------------

  int opened() {
    for (std::size_t slot = 0; slot < connections_.size(); slot++) {
      if (connections_[slot].state == State::FREE) {
        Connection& connection = connections_[slot];
        connection.state = State::AWAITING_LOGIN;
        connection.watcher = NOBODY;
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
        [&](char* message, const std::size_t size) { onMessage(connection, message, size); });
    if (!sound) {
      poison(connection);
    }
  }

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
    return connection.state == State::ENDED && connection.drained == connection.out.size();
  }

  void closed(const int slot) {
    Connection& connection = connections_[static_cast<std::size_t>(slot)];
    for (Stream& stream : streams_) {
      if (stream.boundTo == slot) {
        stream.boundTo = -1;
      }
    }
    connection.state = State::FREE;
  }

  void onTick() {
    const std::uint64_t now = clock_.now();
    for (std::size_t slot = 0; slot < connections_.size(); slot++) {
      Connection& connection = connections_[slot];
      if (connection.state != State::ESTABLISHED) {
        continue;
      }
      if (now - connection.lastInbound > DEAD * heartbeat_) {
        end(connection);
        continue;
      }
      if (now - connection.lastOutbound >= heartbeat_) {
        sbe::SessionHeartbeat pulse;
        char space[64] = {};
        pulse.wrapAndApplyHeader(space, 0, sizeof space);
        pulse.reserved(0);
        enqueue(connection, space, encodedSize<sbe::SessionHeartbeat>());
      }
    }
  }

  // The venue surface: the same event stream everyone hears, routed to whoever watches --------

  void onEvent(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::OrderAccepted::sbeTemplateId(): {
        sbe::OrderAccepted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        owners_.accepted(event.orderId(), event.participantId(), event.context().instrumentId());
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
        // A rested order that filled whole is done; the delivery came first.
        owners_.onExecuted(event.restingOrderId(), event.quantity());
        break;
      }
      case sbe::OrderRested::sbeTemplateId(): {
        sbe::OrderRested event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = owners_.find(event.orderId());
        if (at >= 0) {
          deliver(owners_.at(at).participant, message, length);
          owners_.onRested(event.orderId(),
                           static_cast<std::uint8_t>(event.side() == sbe::Side::BUY ? 0 : 1),
                           event.price(), event.quantity());
        }
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        sbe::OrderReduced event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = owners_.find(event.orderId());
        if (at >= 0) {
          deliver(owners_.at(at).participant, message, length);
          owners_.onReduced(event.orderId(), event.quantity());
        }
        break;
      }
      case sbe::OrderRemoved::sbeTemplateId(): {
        sbe::OrderRemoved event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = owners_.find(event.orderId());
        if (at >= 0) {
          deliver(owners_.at(at).participant, message, length);
          owners_.onRemoved(event.orderId(), event.reason() == sbe::RemoveReason::REPLACED);
        }
        break;
      }
      case sbe::OrderTriggered::sbeTemplateId(): {
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
        break;
    }
  }

  std::uint64_t poisoned() const { return poisoned_; }
  std::uint64_t rejections() const { return rejections_; }
  std::uint64_t exhausted() const { return exhausted_; }

 private:
  enum class State : std::uint8_t { FREE, AWAITING_LOGIN, ESTABLISHED, ENDED };

  static constexpr std::uint32_t NOBODY = 0xFFFFFFFF;
  static constexpr std::uint64_t DEAD = 5;
  static constexpr std::size_t OUT_BYTES = 1 << 20;
  static constexpr std::size_t LOG_BYTES = 1 << 22;
  static constexpr std::size_t LOG_ENTRIES = 1 << 16;

  struct Connection {
    State state = State::FREE;
    std::uint32_t watcher = NOBODY;
    gateway::Reassembler reassembler;
    std::vector<char> out;
    std::size_t drained = 0;
    std::uint64_t lastInbound = 0;
    std::uint64_t lastOutbound = 0;
  };

  // One scope's stream: framed events, kept whole so replay is a memory read.
  struct Stream {
    std::vector<char> log;
    std::vector<std::size_t> offsets;
    std::vector<std::size_t> lengths;
    int boundTo = -1;
    bool exhausted = false;
  };

  template <typename Message>
  static constexpr std::size_t encodedSize() {
    return sbe::MessageHeader::encodedLength() + Message::sbeBlockLength();
  }

  void onMessage(Connection& connection, char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    if (wrap.schemaId() != sbe::MessageHeader::sbeSchemaId()) {
      poison(connection);
      return;
    }
    switch (wrap.templateId()) {
      case sbe::LoginRequest::sbeTemplateId(): {
        sbe::LoginRequest request;
        request.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                              length);
        login(connection, request);
        break;
      }
      case sbe::SessionHeartbeat::sbeTemplateId():
        break;
      case sbe::LogoutRequest::sbeTemplateId():
        end(connection);
        break;
      default:
        // The plane's whole point: a drop copy session may say nothing but session plane.
        poisoned_++;
        poison(connection);
        break;
    }
  }

  void login(Connection& connection, sbe::LoginRequest& request) {
    if (connection.state != State::AWAITING_LOGIN) {
      poison(connection);
      return;
    }
    long streamAt = -1;
    for (std::size_t at = 0; at < watchers_.size(); at++) {
      if (watchers_[at].scopeParticipant == request.participantId()) {
        streamAt = static_cast<long>(at);
        break;
      }
    }
    if (streamAt < 0) {
      refuse(connection, sbe::LoginRefusal::UNKNOWN_PARTICIPANT);
      return;
    }
    if (watchers_[static_cast<std::size_t>(streamAt)].credential != request.credential()) {
      refuse(connection, sbe::LoginRefusal::BAD_CREDENTIAL);
      return;
    }
    if (streams_[static_cast<std::size_t>(streamAt)].exhausted) {
      refuse(connection, sbe::LoginRefusal::EXHAUSTED);
      return;
    }
    Stream& stream = streams_[static_cast<std::size_t>(streamAt)];
    if (stream.boundTo >= 0) {
      refuse(connection, sbe::LoginRefusal::ALREADY_LOGGED_IN);
      return;
    }
    const std::uint64_t from = request.expectedSequence() == 0 ? 1 : request.expectedSequence();
    if (from > stream.offsets.size() + 1) {
      refuse(connection, sbe::LoginRefusal::SEQUENCE_AHEAD);
      return;
    }
    connection.state = State::ESTABLISHED;
    connection.watcher = static_cast<std::uint32_t>(streamAt);
    stream.boundTo = boundSlot(connection);
    char space[64] = {};
    sbe::LoginAccepted accepted;
    accepted.wrapAndApplyHeader(space, 0, sizeof space);
    accepted.nextSequence(from)
        .participantId(watchers_[static_cast<std::size_t>(streamAt)].scopeParticipant)
        .reserved(0);
    enqueue(connection, space, encodedSize<sbe::LoginAccepted>());
    // The replay: everything the scope's stream holds from the sequence asked for, byte exact.
    for (std::size_t at = from - 1; at < stream.offsets.size(); at++) {
      raw(connection, stream.log.data() + stream.offsets[at], stream.lengths[at]);
    }
  }

  void refuse(Connection& connection, const sbe::LoginRefusal::Value reason) {
    char space[64] = {};
    sbe::LoginRejected rejected;
    rejected.wrapAndApplyHeader(space, 0, sizeof space);
    rejected.reason(reason);
    enqueue(connection, space, encodedSize<sbe::LoginRejected>());
    rejections_++;
    connection.state = State::ENDED;
  }

  void end(Connection& connection) {
    if (connection.state == State::ESTABLISHED || connection.state == State::AWAITING_LOGIN) {
      char space[64] = {};
      sbe::SessionEnded ended;
      ended.wrapAndApplyHeader(space, 0, sizeof space);
      ended.reserved(0);
      enqueue(connection, space, encodedSize<sbe::SessionEnded>());
    }
    connection.state = State::ENDED;
  }

  void poison(Connection& connection) {
    connection.out.clear();
    connection.drained = 0;
    connection.state = State::ENDED;
  }

  int boundSlot(const Connection& connection) const {
    return static_cast<int>(&connection - connections_.data());
  }

  void enqueue(Connection& connection, const char* message, const std::size_t length) {
    const std::uint16_t prefix = static_cast<std::uint16_t>(length);
    const std::size_t start = connection.out.size();
    connection.out.resize(start + sizeof prefix + length);
    std::memcpy(connection.out.data() + start, &prefix, sizeof prefix);
    std::memcpy(connection.out.data() + start + sizeof prefix, message, length);
    connection.lastOutbound = clock_.now();
  }

  void raw(Connection& connection, const char* bytes, const std::size_t length) {
    const std::size_t start = connection.out.size();
    connection.out.resize(start + length);
    std::memcpy(connection.out.data() + start, bytes, length);
    connection.lastOutbound = clock_.now();
  }

  void deliver(const std::uint32_t participant, const char* message, const std::size_t length) {
    for (std::size_t at = 0; at < watchers_.size(); at++) {
      if (watchers_[at].scopeParticipant != participant) {
        continue;
      }
      Stream& stream = streams_[at];
      if (stream.exhausted) {
        continue;
      }
      if (stream.log.size() + length + sizeof(std::uint16_t) > LOG_BYTES ||
          stream.offsets.size() == LOG_ENTRIES) {
        // Sized for the session, and a scope that outgrows it ends, politely and alone: a live
        // tail without retention would break the byte-exact promise silently, so the watcher is
        // told and the door answers EXHAUSTED until tomorrow.
        stream.exhausted = true;
        exhausted_++;
        if (stream.boundTo >= 0) {
          end(connections_[static_cast<std::size_t>(stream.boundTo)]);
        }
        continue;
      }
      const std::uint16_t prefix = static_cast<std::uint16_t>(length);
      const std::size_t start = stream.log.size();
      stream.log.resize(start + sizeof prefix + length);
      std::memcpy(stream.log.data() + start, &prefix, sizeof prefix);
      std::memcpy(stream.log.data() + start + sizeof prefix, message, length);
      stream.offsets.push_back(start);
      stream.lengths.push_back(sizeof prefix + length);
      if (stream.boundTo >= 0) {
        raw(connections_[static_cast<std::size_t>(stream.boundTo)], stream.log.data() + start,
            sizeof prefix + length);
      }
    }
  }

  // Ownership is the shared discipline in components/common/ownership.hpp.
  std::uint32_t ownerOf(const std::uint64_t orderId) const {
    const long at = owners_.find(orderId);
    return at < 0 ? NOBODY : owners_.at(at).participant;
  }

  Clock& clock_;
  std::vector<Watcher> watchers_;
  std::uint64_t heartbeat_;
  std::vector<Connection> connections_;
  std::vector<Stream> streams_;
  common::Ownership owners_;
  std::uint64_t poisoned_ = 0;
  std::uint64_t rejections_ = 0;
  std::uint64_t exhausted_ = 0;
};

}  // namespace exchange::oversight
