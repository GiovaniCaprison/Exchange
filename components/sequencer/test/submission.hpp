// The sequencer suites' shared machinery: submissions built from the matcher harness's flow with
// the stamps zeroed, capturing sinks for every carrier, and a decoder for acknowledgments. The
// matcher harness is borrowed on purpose: its CommandWriter stamps sequence and timestamp exactly
// as the sequencer must, so byte equality against a hand-stamped flow is the core proof.

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "clock.hpp"
#include "exchange_protocol/CommandSequenced.h"
#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/MessageHeader.h"
#include "harness.hpp"
#include "journal.hpp"
#include "sequencer.hpp"

namespace exchange::sequencer::test {

namespace sbe = ::exchange::protocol;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;

// One submission-plane record: the envelope, then the command with the stamps the sequencer owns
// zeroed out, exactly as a gateway would forward it.
inline std::vector<char> submissionRecord(const std::uint32_t gatewayId,
                                          const std::uint64_t gatewaySequence,
                                          const std::vector<char>& command) {
  std::vector<char> record(SUBMISSION_BYTES + command.size());
  sbe::GatewaySubmission submission;
  submission.wrapAndApplyHeader(record.data(), 0, record.size());
  submission.gatewaySequence(gatewaySequence).gatewayId(gatewayId).reserved(0);
  std::memcpy(record.data() + SUBMISSION_BYTES, command.data(), command.size());
  std::memset(record.data() + SUBMISSION_BYTES + sbe::MessageHeader::encodedLength(), 0,
              2 * sizeof(std::uint64_t));
  return record;
}

// Deals a stamped flow across gateways round robin, each gateway numbering its own submissions,
// which is the racy arrival the sequencer turns back into the flow's exact order.
inline std::vector<std::vector<char>> dealtSubmissions(
    const std::vector<CommandWriter::Framed>& flow, const std::uint32_t gateways) {
  std::vector<std::uint64_t> counters(gateways, 0);
  std::vector<std::vector<char>> records;
  records.reserve(flow.size());
  for (std::size_t at = 0; at < flow.size(); at++) {
    const std::uint32_t gateway = static_cast<std::uint32_t>(at % gateways);
    records.push_back(submissionRecord(gateway, ++counters[gateway], flow[at].bytes));
  }
  return records;
}

struct Ack {
  std::uint64_t gatewaySequence = 0;
  std::uint64_t sequence = 0;
  std::uint64_t timestamp = 0;
};

inline std::vector<Ack> readAcks(std::vector<char> bytes) {
  std::vector<Ack> acks;
  std::size_t at = 0;
  while (at + ACK_BYTES <= bytes.size()) {
    sbe::MessageHeader wrap;
    wrap.wrap(bytes.data(), at, 0, bytes.size());
    sbe::CommandSequenced ack;
    ack.wrapForDecode(bytes.data(), at + wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                      bytes.size());
    acks.push_back({ack.gatewaySequence(), ack.sequence(), ack.timestamp()});
    at += ACK_BYTES;
  }
  return acks;
}

struct CapturingPacketSink {
  std::vector<std::vector<char>> packets;
  void send(const char* bytes, const std::size_t length) {
    packets.emplace_back(bytes, bytes + length);
  }
};

struct DiscardingPacketSink {
  void send(const char*, std::size_t) {}
};

// The rewind channel between a packet source and the rewinder: requests are recorded and served
// by the test's driver loop, as a transport would carry them, so the source never reenters
// itself.
struct RewindChannel {
  struct Request {
    std::uint64_t firstSequence;
    std::uint32_t count;
  };
  std::vector<Request> requests;
  void request(const std::uint64_t firstSequence, const std::uint32_t count) {
    requests.push_back({firstSequence, count});
  }
};

// A sequencer wired to capturing carriers on the scripted clock, the one shape every suite
// drives.
struct Wired {
  CapturingRing out;
  std::vector<CapturingRing> acks;
  CapturingPacketSink packets;
  common::journal::Writer journal;
  ScriptedClock clock;
  Sequencer<CapturingRing, CapturingRing, CapturingPacketSink, common::journal::Writer,
            ScriptedClock>
      sequencer;

  Wired(const std::string& journalPath, const std::uint32_t gateways)
      : acks(gateways), journal(journalPath), sequencer(out, acks, packets, journal, clock) {}
};

}  // namespace exchange::sequencer::test
