// The session's last word, held to the outsider's arithmetic: the summary the close publishes
// equals the numbers a naive consumer computes from the public prints themselves, per
// instrument, with a print counted once however many book orders it touched; a session with no
// prints says nothing; and a new session's tape owes yesterday nothing.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <map>
#include <set>
#include <vector>

#include "exchange_protocol/PublicSessionSummary.h"
#include "feedtest.hpp"
#include "ranges.hpp"

using namespace exchange::marketdata;
using namespace exchange::marketdata::test;
using exchange::matcher::test::generatedFlow;
namespace common = exchange::common;
namespace sbe = exchange::protocol;

namespace {

struct NaiveTape {
  std::int64_t open = 0;
  std::int64_t high = 0;
  std::int64_t low = 0;
  std::int64_t close = 0;
  std::int64_t volume = 0;
  std::uint64_t prints = 0;
};

// The outsider's arithmetic over the public packets: one print per executionId, whichever book
// orders it touched, and the summaries as they arrive.
struct Outsider {
  std::map<std::uint32_t, NaiveTape> tapes;
  std::map<std::uint32_t, std::set<std::uint64_t>> seen;
  std::map<std::uint32_t, NaiveTape> published;

  void onPublic(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    if (wrap.templateId() == sbe::PublicOrderExecuted::sbeTemplateId()) {
      sbe::PublicOrderExecuted print;
      print.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          length);
      const std::uint32_t instrument = print.context().instrumentId();
      if (!seen[instrument].insert(print.executionId()).second) {
        return;
      }
      NaiveTape& tape = tapes[instrument];
      if (tape.prints == 0) {
        tape.open = print.price();
        tape.high = print.price();
        tape.low = print.price();
      }
      tape.high = print.price() > tape.high ? print.price() : tape.high;
      tape.low = print.price() < tape.low ? print.price() : tape.low;
      tape.close = print.price();
      tape.volume += print.quantity();
      tape.prints++;
    } else if (wrap.templateId() == sbe::PublicSessionSummary::sbeTemplateId()) {
      sbe::PublicSessionSummary summary;
      summary.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
      published[summary.context().instrumentId()] = {summary.openPrice(), summary.highPrice(),
                                                     summary.lowPrice(),  summary.closePrice(),
                                                     summary.volume(),    summary.prints()};
    }
  }
};

void feedAll(Outsider& outsider, std::vector<std::vector<char>>& packets, std::size_t& from) {
  for (; from < packets.size(); from++) {
    common::ranges::Reader reader(packets[from].data(), packets[from].size());
    reader.forEach(
        [&](char* message, const std::size_t size) { outsider.onPublic(message, size); });
  }
}

}  // namespace

TEST_CASE("the close publishes the outsider's own arithmetic, instrument by instrument") {
  const auto flow = generatedFlow(57, 5000);
  Venue venue;
  venue.play(flow, 0, flow.size());

  // The venue closes every instrument it traded; each close is one command through the matcher.
  CommandWriter closer;
  std::vector<CommandWriter::Framed> closes;
  for (const std::uint32_t instrument : venue.builder.instruments()) {
    closes.push_back(closer.session(instrument, sbe::SessionState::CLOSED));
  }
  REQUIRE(!closes.empty());

  Outsider outsider;
  std::size_t consumed = 0;
  feedAll(outsider, venue.a.packets, consumed);
  for (CommandWriter::Framed& close : closes) {
    std::vector<char> bytes = close.bytes;
    venue.partition.onCommand(bytes.data(), 0, bytes.size());
    std::vector<char> fresh(venue.events.captured().begin() + static_cast<long>(venue.consumed),
                            venue.events.captured().end());
    venue.consumed = venue.events.captured().size();
    eachMessage(fresh, [&](char* message, const std::size_t length) {
      venue.builder.onEvent(message, length);
    });
  }
  venue.publisher.flush();
  feedAll(outsider, venue.a.packets, consumed);

  std::size_t summarised = 0;
  for (const std::uint32_t instrument : venue.builder.instruments()) {
    const NaiveTape& mine = outsider.tapes[instrument];
    if (mine.prints == 0) {
      // A session with no prints says nothing.
      CHECK(!outsider.published.contains(instrument));
      continue;
    }
    REQUIRE(outsider.published.contains(instrument));
    const NaiveTape& official = outsider.published[instrument];
    CHECK(official.open == mine.open);
    CHECK(official.high == mine.high);
    CHECK(official.low == mine.low);
    CHECK(official.close == mine.close);
    CHECK(official.volume == mine.volume);
    CHECK(official.prints == mine.prints);
    summarised++;
  }
  CHECK(summarised > 0);
}

TEST_CASE("a new session's tape owes yesterday nothing") {
  Venue venue;
  CommandWriter writer;
  const auto play = [&](const CommandWriter::Framed& command) {
    std::vector<char> bytes = command.bytes;
    venue.partition.onCommand(bytes.data(), 0, bytes.size());
    std::vector<char> fresh(venue.events.captured().begin() + static_cast<long>(venue.consumed),
                            venue.events.captured().end());
    venue.consumed = venue.events.captured().size();
    eachMessage(fresh, [&](char* message, const std::size_t length) {
      venue.builder.onEvent(message, length);
    });
    venue.publisher.flush();
  };
  play(writer.instrument(1, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
  play(writer.session(1, sbe::SessionState::CONTINUOUS));
  play(writer.newOrder(1, 1, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 10, 0, 0, 0, 0));
  play(writer.newOrder(1, 2, 8, sbe::Side::SELL, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, 100'000, 10, 0, 0, 0, 0));
  play(writer.session(1, sbe::SessionState::CLOSED));
  CHECK(venue.builder.tapeOf(1).prints == 1);
  CHECK(venue.builder.tapeOf(1).close == 100'000);

  // Tomorrow: pre-open wipes the tape, and the new day's one print is the whole story.
  play(writer.session(1, sbe::SessionState::PRE_OPEN));
  CHECK(venue.builder.tapeOf(1).prints == 0);
  play(writer.session(1, sbe::SessionState::CONTINUOUS));
  play(writer.newOrder(1, 3, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'010, 5, 0, 0, 0, 0));
  play(writer.newOrder(1, 4, 8, sbe::Side::SELL, sbe::Pricing::LIMIT,
                       sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, 100'010, 5, 0, 0, 0, 0));
  play(writer.session(1, sbe::SessionState::CLOSED));
  const auto tape = venue.builder.tapeOf(1);
  CHECK(tape.prints == 1);
  CHECK(tape.open == 100'010);
  CHECK(tape.volume == 5);
}
