// The gateway suites' shared machinery: a client that speaks the session plane in bytes, event
// forgers for the venue side, and a decoder that reads a session's outbound stream back into
// (templateId, message) pairs through the same reassembler the gateway trusts its own life to.

#pragma once

#include <cstdint>
#include <cstring>
#include <utility>
#include <vector>

#include "exchange_protocol/LoginRequest.h"
#include "exchange_protocol/LogoutRequest.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/SessionHeartbeat.h"
#include "harness.hpp"
#include "wire.hpp"

namespace exchange::gateway::test {

namespace sbe = ::exchange::protocol;
using exchange::matcher::test::CommandWriter;

inline void framed(std::vector<char>& out, const char* message, const std::uint16_t length) {
  out.insert(out.end(), reinterpret_cast<const char*>(&length),
             reinterpret_cast<const char*>(&length) + sizeof length);
  out.insert(out.end(), message, message + length);
}

template <typename Message, typename Fill>
std::vector<char> sessionMessage(Fill&& fill) {
  std::vector<char> bytes;
  char space[128] = {};
  Message message;
  message.wrapAndApplyHeader(space, 0, sizeof space);
  fill(message);
  framed(
      bytes, space,
      static_cast<std::uint16_t>(sbe::MessageHeader::encodedLength() + Message::sbeBlockLength()));
  return bytes;
}

inline std::vector<char> loginBytes(const std::uint32_t participant, const std::uint64_t secret,
                                    const std::uint64_t expected) {
  return sessionMessage<sbe::LoginRequest>([&](sbe::LoginRequest& login) {
    login.expectedSequence(expected).credential(secret).participantId(participant).reserved(0);
  });
}

inline std::vector<char> heartbeatBytes() {
  return sessionMessage<sbe::SessionHeartbeat>(
      [](sbe::SessionHeartbeat& pulse) { pulse.reserved(0); });
}

inline std::vector<char> logoutBytes() {
  return sessionMessage<sbe::LogoutRequest>([](sbe::LogoutRequest& logout) { logout.reserved(0); });
}

// A command as a client sends it: the harness's encoding with the stamps zeroed, and whatever
// participantId the client claims left in place, since the gateway must not believe it.
inline std::vector<char> commandBytes(const CommandWriter::Framed& command) {
  std::vector<char> bytes;
  std::vector<char> copy = command.bytes;
  std::memset(copy.data() + sbe::MessageHeader::encodedLength(), 0, 2 * sizeof(std::uint64_t));
  framed(bytes, copy.data(), static_cast<std::uint16_t>(copy.size()));
  return bytes;
}

// The venue's side, forged: enough of each event for routing to chew on.
inline std::vector<char> acceptedEvent(const std::uint64_t orderId, const std::uint32_t participant,
                                       const std::uint64_t clientOrderId) {
  char space[128] = {};
  sbe::OrderAccepted event;
  event.wrapAndApplyHeader(space, 0, sizeof space);
  event.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(1).reserved(0);
  event.orderId(orderId).clientOrderId(clientOrderId).participantId(participant);
  return std::vector<char>(
      space, space + sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength());
}

inline std::vector<char> executedEvent(const std::uint64_t aggressor, const std::uint64_t resting) {
  char space[128] = {};
  sbe::OrderExecuted event;
  event.wrapAndApplyHeader(space, 0, sizeof space);
  event.context().sequence(2).inputSequence(2).timestamp(2).instrumentId(1).reserved(0);
  event.executionId(1).aggressorOrderId(aggressor).restingOrderId(resting).price(100).quantity(1);
  return std::vector<char>(
      space, space + sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength());
}

// Reads a session's outbound bytes back into (templateId, message bytes) pairs.
inline std::vector<std::pair<std::uint16_t, std::vector<char>>> unframed(const char* bytes,
                                                                         std::size_t length) {
  std::vector<std::pair<std::uint16_t, std::vector<char>>> messages;
  Reassembler reassembler;
  const bool sound = reassembler.feed(bytes, length, [&](char* message, const std::size_t size) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, size);
    messages.emplace_back(wrap.templateId(), std::vector<char>(message, message + size));
  });
  if (!sound) {
    messages.clear();
  }
  return messages;
}

}  // namespace exchange::gateway::test
