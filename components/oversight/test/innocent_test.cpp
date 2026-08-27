// The negative control: the ecosystem's own seeded day, the dealer quoting and re-quoting, the
// crowd hitting it and the chaser chasing, watched end to end by surveillance under its default
// thresholds, raises not one alert. A detector is only as good as its silence on the honest, and
// the honest day here is the same deterministic day the ecosystem's flagship journals, so this
// answer is byte-stable too.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "bots.hpp"
#include "room.hpp"
#include "surveillance.hpp"

using namespace exchange::ecosystem;
using namespace exchange::ecosystem::test;
namespace oversight = exchange::oversight;

namespace {

struct CountingSink {
  std::vector<std::vector<char>> alerts;
  void operator()(char* bytes, const std::size_t length) {
    alerts.emplace_back(bytes, bytes + length);
  }
};

}  // namespace

TEST_CASE("the honest day raises nothing") {
  const std::filesystem::path journal =
      std::filesystem::temp_directory_path() / "exchange-oversight-innocent.exj";
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

  CountingSink sink;
  oversight::Surveillance<CountingSink> watch(sink, {});
  std::size_t watched = 0;

  std::uint64_t executions = 0;
  for (int at = 0; at < 600; at++) {
    room.tick(1'000'000, [&] {
      maker.onTick();
      noiseA.onTick();
      noiseB.onTick();
      momentum.onTick();
    });
    room.drainEvents(
        watched, [&](char* message, const std::size_t length) { watch.onEvent(message, length); });
    executions = dealer.entry.executions();
  }

  // The day was a market, and the watcher had plenty to chew on; it swallowed all of it.
  CHECK(executions > 20);
  CHECK(watch.alerts() == 0);
  CHECK(sink.alerts.empty());
  std::filesystem::remove(journal);
}
