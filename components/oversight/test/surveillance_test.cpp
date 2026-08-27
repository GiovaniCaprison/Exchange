// Surveillance held to its definitions with the real matcher deciding what actually happened: a
// wash trade is caught exactly, the Coscia shape is caught when the cancelled pressure dwarfs
// the executions and read as layering when it stood at several prices, honest flow raises
// nothing, the window is a real boundary, and a replayed day raises byte-identical alerts,
// because every detection is a pure function of the stream.

#include "surveillance.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <vector>

#include "exchange_protocol/SurveillanceAlert.h"
#include "flow.hpp"
#include "harness.hpp"
#include "partition.hpp"

using namespace exchange::oversight;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
namespace sbe = exchange::protocol;

namespace {

struct AlertSink {
  std::vector<std::vector<char>> alerts;
  void operator()(char* bytes, const std::size_t length) {
    alerts.emplace_back(bytes, bytes + length);
  }
};

sbe::SurveillanceAlert decode(std::vector<char>& bytes) {
  sbe::MessageHeader wrap;
  wrap.wrap(bytes.data(), 0, 0, bytes.size());
  sbe::SurveillanceAlert alert;
  alert.wrapForDecode(bytes.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                      bytes.size());
  return alert;
}

// The courtroom: the real matcher decides what happened, and surveillance watches the events.
struct Court {
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  AlertSink sink;
  Surveillance<AlertSink> watch;
  CommandWriter writer;
  std::size_t consumed = 0;

  explicit Court(const Config config) : watch(sink, config) {
    play(writer.instrument(1, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
    play(writer.session(1, sbe::SessionState::CONTINUOUS));
  }

  void play(const CommandWriter::Framed& command) {
    std::vector<char> bytes = command.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
    const std::vector<char>& seen = events.captured();
    while (consumed < seen.size()) {
      sbe::MessageHeader wrap;
      std::vector<char> copy(seen.begin() + static_cast<long>(consumed), seen.end());
      wrap.wrap(copy.data(), 0, 0, copy.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      watch.onEvent(copy.data(), length);
      consumed += length;
    }
  }

  void rest(const std::uint32_t participant, const std::uint64_t clientOrderId,
            const sbe::Side::Value side, const std::int64_t price, const std::int64_t quantity) {
    play(writer.newOrder(1, clientOrderId, participant, side, sbe::Pricing::LIMIT,
                         sbe::TimeInForce::GOOD_TILL_CANCEL, false, price, quantity, 0, 0, 0, 0));
  }

  void take(const std::uint32_t participant, const std::uint64_t clientOrderId,
            const sbe::Side::Value side, const std::int64_t price, const std::int64_t quantity) {
    play(writer.newOrder(1, clientOrderId, participant, side, sbe::Pricing::LIMIT,
                         sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, price, quantity, 0, 0, 0,
                         0));
  }

  void cancel(const std::uint32_t participant, const std::uint64_t clientOrderId) {
    play(writer.cancel(1, clientOrderId, participant));
  }
};

Config tight() {
  Config config;
  // Timestamps in the harness advance one nanosecond per command, so the window is a command
  // budget the scenarios control exactly.
  config.windowNanos = 10;
  config.spoofMultiple = 4;
  config.minimumCancelled = 1;
  config.layeringLevels = 2;
  return config;
}

}  // namespace

TEST_CASE("both sides of one print, one participant: the wash trade is caught exactly") {
  Court court(tight());
  court.rest(7, 1, sbe::Side::BUY, 100'000, 10);
  court.take(7, 2, sbe::Side::SELL, 100'000, 10);
  REQUIRE(court.watch.washTrades() == 1);
  REQUIRE(court.sink.alerts.size() == 1);
  const sbe::SurveillanceAlert alert = decode(court.sink.alerts[0]);
  CHECK(alert.kind() == sbe::AlertKind::WASH_TRADE);
  CHECK(alert.participantId() == 7);
  CHECK(alert.instrumentId() == 1);
  CHECK(alert.executedQuantity() == 10);

  // Two honest strangers printing the same way raise nothing.
  court.rest(7, 3, sbe::Side::BUY, 100'000, 10);
  court.take(8, 4, sbe::Side::SELL, 100'000, 10);
  CHECK(court.sink.alerts.size() == 1);
}

TEST_CASE("the Coscia shape: small executions, large away-side pressure promptly cancelled") {
  Court court(tight());
  // The counterparty offers; participant 7 rests large spoof sells at one level, executes a
  // small buy, and cancels the pressure inside the window.
  court.rest(8, 1, sbe::Side::SELL, 100'005, 5);
  court.rest(7, 100, sbe::Side::SELL, 100'050, 60);
  court.take(7, 101, sbe::Side::BUY, 100'005, 5);
  CHECK(court.sink.alerts.empty());
  court.cancel(7, 100);
  REQUIRE(court.sink.alerts.size() == 1);
  const sbe::SurveillanceAlert alert = decode(court.sink.alerts[0]);
  CHECK(alert.kind() == sbe::AlertKind::SPOOFING);
  CHECK(alert.participantId() == 7);
  CHECK(alert.executedQuantity() == 5);
  CHECK(alert.cancelledQuantity() == 60);
  CHECK(alert.priceLevels() == 1);
}

TEST_CASE("the same pressure across price levels reads as layering") {
  Court court(tight());
  court.rest(8, 1, sbe::Side::SELL, 100'005, 5);
  court.rest(7, 100, sbe::Side::SELL, 100'050, 15);
  court.rest(7, 101, sbe::Side::SELL, 100'055, 15);
  court.rest(7, 102, sbe::Side::SELL, 100'060, 15);
  court.take(7, 103, sbe::Side::BUY, 100'005, 5);
  court.cancel(7, 100);
  CHECK(court.sink.alerts.empty());
  court.cancel(7, 101);
  REQUIRE(court.sink.alerts.size() == 1);
  const sbe::SurveillanceAlert alert = decode(court.sink.alerts[0]);
  CHECK(alert.kind() == sbe::AlertKind::LAYERING);
  CHECK(alert.cancelledQuantity() == 30);
  CHECK(alert.priceLevels() == 2);

  // The storm is one case: the third cancel inside the quiet window adds no alert.
  court.cancel(7, 102);
  CHECK(court.sink.alerts.size() == 1);
}

TEST_CASE("honest flow raises nothing, and the window is a real boundary") {
  Court court(tight());
  // A dealer's ordinary life: quote, get filled, cancel the remainder of the SAME side, requote.
  court.rest(8, 1, sbe::Side::SELL, 100'005, 5);
  court.rest(7, 100, sbe::Side::BUY, 100'000, 10);
  court.take(8, 2, sbe::Side::SELL, 100'000, 4);
  court.cancel(7, 100);
  CHECK(court.sink.alerts.empty());

  // The spoof shape, but the cancellation comes long after the window closed: eleven commands
  // of silence at a nanosecond each put the fill out of reach.
  court.rest(7, 200, sbe::Side::SELL, 100'050, 60);
  court.take(7, 201, sbe::Side::BUY, 100'005, 5);
  for (std::uint64_t filler = 300; filler < 312; filler++) {
    court.rest(9, filler, sbe::Side::BUY, 99'000, 1);
  }
  court.cancel(7, 200);
  CHECK(court.sink.alerts.empty());

  // Quiet commands push that cancellation out of every later window too.
  for (std::uint64_t filler = 320; filler < 332; filler++) {
    court.rest(9, filler, sbe::Side::BUY, 99'000, 1);
  }

  // Cancelled quantity below the multiple stays honest too.
  court.rest(8, 3, sbe::Side::SELL, 100'005, 5);
  court.rest(7, 400, sbe::Side::SELL, 100'050, 15);
  court.take(7, 401, sbe::Side::BUY, 100'005, 5);
  court.cancel(7, 400);
  CHECK(court.sink.alerts.empty());
}

TEST_CASE("a replayed day raises byte-identical alerts") {
  const auto once = [] {
    Court court(tight());
    court.rest(8, 1, sbe::Side::SELL, 100'005, 5);
    court.rest(7, 100, sbe::Side::SELL, 100'050, 30);
    court.rest(7, 101, sbe::Side::SELL, 100'055, 30);
    court.take(7, 102, sbe::Side::BUY, 100'005, 5);
    court.cancel(7, 100);
    court.cancel(7, 101);
    court.rest(9, 200, sbe::Side::BUY, 100'000, 10);
    court.take(9, 201, sbe::Side::SELL, 100'000, 10);
    return court.sink.alerts;
  };
  const std::vector<std::vector<char>> first = once();
  const std::vector<std::vector<char>> second = once();
  REQUIRE(first.size() == 2);
  CHECK(first == second);
}
