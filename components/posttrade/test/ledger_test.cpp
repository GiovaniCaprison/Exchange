// The ledger's law is arithmetic, held over the real matcher: a scripted trade names its buyer
// and its seller correctly whichever side rested; a wash trade nets to nothing while its volumes
// count; and over thousands of generated commands, positions sum to zero per instrument, cash
// sums to zero across the venue, and bought minus sold is the position, account by account. The
// day's files are byte-identical across a replay, and re-reading them recovers the facts.

#include "ledger.hpp"

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "flow.hpp"
#include "harness.hpp"
#include "partition.hpp"

using namespace exchange::posttrade;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::matcher::test::generatedFlow;
namespace sbe = exchange::protocol;

namespace {

// One venue in a box: the flow through a partition, the events through the ledger.
struct Books {
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  Ledger ledger;
  std::size_t consumed = 0;

  void play(const CommandWriter::Framed& command) {
    std::vector<char> bytes = command.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
    drain();
  }

  void drain() {
    const std::vector<char>& seen = events.captured();
    while (consumed < seen.size()) {
      sbe::MessageHeader wrap;
      std::vector<char> copy(seen.begin() + static_cast<long>(consumed), seen.end());
      wrap.wrap(copy.data(), 0, 0, copy.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      ledger.onEvent(copy.data(), length);
      consumed += length;
    }
  }
};

}  // namespace

TEST_CASE("a trade names its buyer and its seller, whichever side rested") {
  Books books;
  CommandWriter writer;
  books.play(writer.instrument(1, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
  books.play(writer.session(1, sbe::SessionState::CONTINUOUS));

  // A bid rests and an offer takes it: the resting owner bought.
  books.play(writer.newOrder(1, 1, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 10, 0, 0, 0, 0));
  books.play(writer.newOrder(1, 2, 8, sbe::Side::SELL, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, 100'000, 10, 0, 0, 0,
                             0));
  REQUIRE(books.ledger.trades().size() == 1);
  CHECK(books.ledger.trades()[0].buyer == 7);
  CHECK(books.ledger.trades()[0].seller == 8);
  CHECK(books.ledger.positionOf(7, 1) == 10);
  CHECK(books.ledger.positionOf(8, 1) == -10);
  CHECK(books.ledger.cashOf(7) == -1'000'000);
  CHECK(books.ledger.cashOf(8) == 1'000'000);

  // An offer rests and a bid takes it: the resting owner sold.
  books.play(writer.newOrder(1, 3, 9, sbe::Side::SELL, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'005, 5, 0, 0, 0, 0));
  books.play(writer.newOrder(1, 4, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, 100'005, 5, 0, 0, 0, 0));
  REQUIRE(books.ledger.trades().size() == 2);
  CHECK(books.ledger.trades()[1].buyer == 7);
  CHECK(books.ledger.trades()[1].seller == 9);
  CHECK(books.ledger.positionOf(7, 1) == 15);

  // A wash trade nets to nothing while its volumes count.
  books.play(writer.newOrder(1, 5, 9, sbe::Side::BUY, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::GOOD_TILL_CANCEL, false, 100'000, 4, 0, 0, 0, 0));
  books.play(writer.newOrder(1, 6, 9, sbe::Side::SELL, sbe::Pricing::LIMIT,
                             sbe::TimeInForce::IMMEDIATE_OR_CANCEL, false, 100'000, 4, 0, 0, 0, 0));
  CHECK(books.ledger.positionOf(9, 1) == -5);
  for (const Account& account : books.ledger.accounts()) {
    if (account.participant == 9 && account.instrumentId == 1) {
      CHECK(account.bought == 4);
      CHECK(account.sold == 9);
    }
  }
}

TEST_CASE("conservation holds over generated flow: the venue always nets to zero") {
  const auto flow = generatedFlow(2027, 8000);
  Books books;
  for (const CommandWriter::Framed& command : flow) {
    books.play(command);
  }
  REQUIRE(books.ledger.trades().size() > 100);

  std::map<std::uint32_t, std::int64_t> perInstrument;
  std::int64_t cash = 0;
  for (const Account& account : books.ledger.accounts()) {
    perInstrument[account.instrumentId] += account.position;
    cash += account.cash;
    CHECK(account.bought - account.sold == account.position);
    CHECK(account.bought >= 0);
    CHECK(account.sold >= 0);
  }
  for (const auto& [instrument, net] : perInstrument) {
    CHECK(net == 0);
  }
  CHECK(cash == 0);

  // The tape is in sequence order, which is the day's order.
  for (std::size_t at = 1; at < books.ledger.trades().size(); at++) {
    CHECK(books.ledger.trades()[at - 1].sequence <= books.ledger.trades()[at].sequence);
  }
}

TEST_CASE("a replayed day writes byte-identical files, and the files carry the facts") {
  const auto flow = generatedFlow(303, 5000);
  const auto oneDay = [&flow] {
    Books books;
    for (const CommandWriter::Framed& command : flow) {
      books.play(command);
    }
    std::ostringstream trades;
    std::ostringstream positions;
    books.ledger.writeTrades(trades);
    books.ledger.writePositions(positions);
    return std::pair{trades.str(), positions.str()};
  };
  const auto [tradesOne, positionsOne] = oneDay();
  const auto [tradesTwo, positionsTwo] = oneDay();
  CHECK(tradesOne == tradesTwo);
  CHECK(positionsOne == positionsTwo);
  CHECK(tradesOne.starts_with("executionId,sequence,timestamp,instrumentId,price,quantity"));
  CHECK(positionsOne.starts_with("participantId,instrumentId,position,bought,sold,cash"));

  // Re-reading the positions file recovers conservation: the file itself audits.
  std::istringstream reread(positionsOne);
  std::string line;
  std::getline(reread, line);
  std::int64_t position = 0;
  std::int64_t cash = 0;
  std::size_t rows = 0;
  while (std::getline(reread, line)) {
    std::istringstream row(line);
    std::string field;
    std::vector<std::string> fields;
    while (std::getline(row, field, ',')) {
      fields.push_back(field);
    }
    REQUIRE(fields.size() == 6);
    position += std::stoll(fields[2]);
    cash += std::stoll(fields[5]);
    rows++;
  }
  CHECK(rows > 0);
  CHECK(cash == 0);
}
