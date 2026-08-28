// The proof that matters most: in the whole-room day, the ledger's position for every
// participant equals the position that participant's own order entry client computed from its
// own fills. The venue's books and the participants' books agree because both are functions of
// the same stream, which is the architecture keeping its central promise in the place money
// would notice.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>

#include "bots.hpp"
#include "ledger.hpp"
#include "room.hpp"

using namespace exchange::ecosystem;
using namespace exchange::ecosystem::test;
using exchange::posttrade::Ledger;

TEST_CASE("the venue's books and the participants' books agree, seat by seat") {
  const std::filesystem::path journal =
      std::filesystem::temp_directory_path() / "exchange-posttrade-agreement.exj";
  Room room(journal.string());
  Seat& dealer = room.seat(7, 42);
  Seat& crowdA = room.seat(8, 43);
  Seat& crowdB = room.seat(9, 44);
  Seat& chaser = room.seat(10, 45);
  room.open(1);

  Maker<Feed, Entry> maker(dealer.feed, dealer.entry, {});
  NoiseTaker<Feed, Entry> noiseA(crowdA.feed, crowdA.entry, {}, 11);
  NoiseTaker<Feed, Entry> noiseB(crowdB.feed, crowdB.entry, {}, 22);
  MomentumTaker<Feed, Entry> momentum(chaser.feed, chaser.entry, {});

  Ledger ledger;
  std::size_t booked = 0;
  for (int at = 0; at < 600; at++) {
    room.tick(1'000'000, [&] {
      maker.onTick();
      noiseA.onTick();
      noiseB.onTick();
      momentum.onTick();
    });
    room.drainEvents(
        booked, [&](char* message, const std::size_t length) { ledger.onEvent(message, length); });
  }

  REQUIRE(ledger.trades().size() > 20);
  CHECK(ledger.positionOf(7, 1) == dealer.entry.positionOf(1));
  CHECK(ledger.positionOf(8, 1) == crowdA.entry.positionOf(1));
  CHECK(ledger.positionOf(9, 1) == crowdB.entry.positionOf(1));
  CHECK(ledger.positionOf(10, 1) == chaser.entry.positionOf(1));
  CHECK(ledger.positionOf(7, 1) + ledger.positionOf(8, 1) + ledger.positionOf(9, 1) +
            ledger.positionOf(10, 1) ==
        0);
  std::filesystem::remove(journal);
}
