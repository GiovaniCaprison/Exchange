// The flagship: a whole trading day, opened, traded, halted by its own wildness, reopened
// through an auction, and closed, with every actor real, the scheduler, the sequencer, the
// matcher, and every clock scripted. Driven twice, the day journals identical bytes, halts
// included, which is determinism covering the component with authority too.

#include "day.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

using namespace exchange::operations;
using namespace exchange::operations::test;
namespace sbe = exchange::protocol;

namespace {

std::filesystem::path scratch(const std::string& name) {
  return std::filesystem::temp_directory_path() / ("exchange-operations-" + name);
}

std::vector<char> fileBytes(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  return std::vector<char>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

// One scripted day, everything deterministic, returning what the scheduler counted.
std::uint64_t oneDay(const std::string& journalPath) {
  Config config;
  config.instruments = {1};
  config.gatewayId = 1;
  config.bandBasisPoints = 500;
  config.haltNanos = 5'000;
  config.auctionNanos = 2'000;
  const std::uint64_t start = 1'000'000'000'000ULL;
  config.calendar = {{start + 100, sbe::SessionState::PRE_OPEN},
                     {start + 200, sbe::SessionState::OPENING_AUCTION},
                     {start + 300, sbe::SessionState::CONTINUOUS},
                     {start + 100'000, sbe::SessionState::CLOSING_AUCTION},
                     {start + 101'000, sbe::SessionState::CLOSED}};
  Day day(journalPath, config);
  CommandWriter writer;

  // The instrument exists before the day begins; reference data rides the client carrier here.
  day.client(writer.instrument(1, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));

  // Pre-open, then the opening call gathers interest.
  day.advance(150);
  day.advance(100);
  day.client(writer.newOrder(1, 1, 1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 40, 0, 0, 0, 0));
  day.client(writer.newOrder(1, 2, 2, sbe::Side::SELL, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 30, 0, 0, 0, 0));

  // The open: the uncross prints, and continuous trading breathes.
  day.advance(100);
  CHECK(day.scheduler.confirmedOf(1) == sbe::SessionState::CONTINUOUS);
  for (std::uint64_t at = 0; at < 12; at++) {
    day.client(writer.newOrder(1, 100 + at, 1 + at % 3, sbe::Side::BUY, sbe::Pricing::LIMIT,
                               sbe::TimeInForce::GOOD_TILL_CANCEL, false,
                               100'000 + 5 * static_cast<std::int64_t>(at % 4), 5, 0, 0, 0, 0));
    day.client(writer.newOrder(1, 200 + at, 1 + at % 3, sbe::Side::SELL, sbe::Pricing::LIMIT,
                               sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false,
                               100'000 + 5 * static_cast<std::int64_t>(at % 4), 5, 0, 0, 0, 0));
  }
  CHECK(day.scheduler.halts() == 0);

  // The wildness: a deep sell into thin bids prints far from the mean, and the venue pauses
  // itself. The resting bid far below the band is what the panic executes against.
  day.client(writer.newOrder(1, 300, 1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 90'000, 50, 0, 0, 0, 0));
  day.client(writer.newOrder(1, 301, 2, sbe::Side::SELL, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, 90'000, 20, 0, 0, 0, 0));
  CHECK(day.scheduler.halts() == 1);
  // The halt was decided while the events drained; one more beat carries the command through
  // the sequencer and brings its confirmation back.
  day.pump();
  CHECK(day.scheduler.confirmedOf(1) == sbe::SessionState::HALTED);

  // The pause, the reopening call, fresh interest, and continuous trading again.
  day.advance(5'100);
  CHECK(day.scheduler.confirmedOf(1) == sbe::SessionState::OPENING_AUCTION);
  day.client(writer.newOrder(1, 400, 1, sbe::Side::BUY, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 95'000, 10, 0, 0, 0, 0));
  day.client(writer.newOrder(1, 401, 2, sbe::Side::SELL, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 95'000, 10, 0, 0, 0, 0));
  day.advance(2'100);
  CHECK(day.scheduler.confirmedOf(1) == sbe::SessionState::CONTINUOUS);

  // The closing call and the close.
  day.advance(100'000);
  day.advance(2'000);
  CHECK(day.scheduler.confirmedOf(1) == sbe::SessionState::CLOSED);
  return day.scheduler.halts();
}

}  // namespace

TEST_CASE("the same day, twice, journals identical bytes, halts included") {
  const std::filesystem::path first = scratch("day-one.exj");
  const std::filesystem::path second = scratch("day-two.exj");
  CHECK(oneDay(first.string()) == 1);
  CHECK(oneDay(second.string()) == 1);
  const std::vector<char> one = fileBytes(first.string());
  CHECK(!one.empty());
  CHECK(one == fileBytes(second.string()));
  std::filesystem::remove(first);
  std::filesystem::remove(second);
}
