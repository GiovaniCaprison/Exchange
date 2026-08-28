// Sharding held to the architecture's promise: two partitions each serving half the instruments,
// fed the same sequenced stream, together decide exactly what one partition serving everything
// decides. Prices, quantities and books agree instrument by instrument; ids never collide
// because each shard writes its number in the top byte of every one it mints; a venue-wide mass
// cancel sweeps every shard's books; and a shard replayed twice is byte-identical, which is the
// determinism claim surviving the deployment that splits it.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <map>
#include <utility>
#include <vector>

#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderExecuted.h"
#include "harness.hpp"
#include "partition.hpp"

using namespace exchange::matcher;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
namespace sbe = exchange::protocol;

namespace {

// A two-instrument day from one writer, deterministic, with crossings, cancels and a sweep.
std::vector<CommandWriter::Framed> twoBookDay(const std::uint64_t seed,
                                              const std::uint64_t commands) {
  CommandWriter writer;
  std::vector<CommandWriter::Framed> flow;
  for (std::uint32_t instrument = 1; instrument <= 2; instrument++) {
    flow.push_back(writer.instrument(instrument, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
    flow.push_back(writer.session(instrument, sbe::SessionState::CONTINUOUS));
  }
  std::uint64_t state = seed * 2685821657736338717ULL + 1;
  const auto next = [&state]() {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * 2685821657736338717ULL;
  };
  std::vector<std::uint64_t> live[3];
  std::uint64_t client = 0;
  for (std::uint64_t at = 0; at < commands; at++) {
    const std::uint32_t instrument = 1 + static_cast<std::uint32_t>(next() % 2);
    const std::uint64_t roll = next() % 100;
    if (roll < 30 && !live[instrument].empty()) {
      const std::size_t pick = next() % live[instrument].size();
      const std::uint64_t name = live[instrument][pick];
      live[instrument][pick] = live[instrument].back();
      live[instrument].pop_back();
      flow.push_back(writer.cancel(instrument, name, 1 + static_cast<std::uint32_t>(name % 4)));
      continue;
    }
    client++;
    const bool aggressive = roll >= 85;
    const std::int64_t price = 100'000 + 5 * static_cast<std::int64_t>(next() % 30) -
                               5 * static_cast<std::int64_t>(next() % 30);
    flow.push_back(writer.newOrder(
        instrument, client, 1 + static_cast<std::uint32_t>(client % 4),
        next() % 2 == 0 ? sbe::Side::BUY : sbe::Side::SELL, sbe::Pricing::LIMIT,
        aggressive ? sbe::TimeInForce::IMMEDIATE_OR_CANCEL : sbe::TimeInForce::GOOD_TILL_CANCEL,
        false, price, 1 + static_cast<std::int64_t>(next() % 40), 0, 0, 0, 0));
    if (!aggressive) {
      live[instrument].push_back(client);
    }
  }
  flow.push_back(writer.massCancel(0, 0, 2));
  return flow;
}

struct Run {
  CapturingRing events;
  Partition<CapturingRing> partition{events};

  void play(const std::vector<CommandWriter::Framed>& flow) {
    for (const CommandWriter::Framed& command : flow) {
      std::vector<char> bytes = command.bytes;
      partition.onCommand(bytes.data(), 0, bytes.size());
    }
  }
};

// The facts a consumer would settle against: every print's (price, quantity) per instrument, in
// that instrument's own order, and every minted id's top byte.
struct Prints {
  std::map<std::uint32_t, std::vector<std::pair<std::int64_t, std::int64_t>>> perInstrument;
  std::map<std::uint32_t, std::size_t> idNamespaces;

  void digest(const CapturingRing& events) {
    std::vector<char> copy(events.captured());
    std::size_t at = 0;
    while (at < copy.size()) {
      sbe::MessageHeader wrap;
      wrap.wrap(copy.data(), at, 0, copy.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      if (wrap.templateId() == sbe::OrderExecuted::sbeTemplateId()) {
        sbe::OrderExecuted print;
        print.wrapForDecode(copy.data(), at + sbe::MessageHeader::encodedLength(),
                            wrap.blockLength(), wrap.version(), copy.size());
        perInstrument[print.context().instrumentId()].emplace_back(print.price(), print.quantity());
        idNamespaces[static_cast<std::uint32_t>(print.executionId() >> 56)]++;
      }
      at += length;
    }
  }
};

std::vector<std::pair<std::int64_t, std::int64_t>> ladderShape(const Partition<CapturingRing>& run,
                                                               const std::uint32_t instrument,
                                                               const std::int32_t side) {
  std::vector<std::pair<std::int64_t, std::int64_t>> shape;
  const auto& engine = run.engine(instrument);
  for (const std::int32_t slot : engine.book().restingSlots()) {
    if (run.slab().cold(slot).side == side) {
      shape.emplace_back(run.slab().cold(slot).clientOrderId, run.slab().hot(slot).displayed);
    }
  }
  std::sort(shape.begin(), shape.end());
  return shape;
}

}  // namespace

TEST_CASE("two shards together decide exactly what one partition decides") {
  const std::vector<CommandWriter::Framed> flow = twoBookDay(97, 6000);

  Run whole;
  whole.play(flow);

  Run one;
  one.partition.serve({1});
  one.partition.shard(1);
  one.play(flow);

  Run two;
  two.partition.serve({2});
  two.partition.shard(2);
  two.play(flow);

  Prints wholePrints;
  wholePrints.digest(whole.events);
  Prints shardPrints;
  shardPrints.digest(one.events);
  shardPrints.digest(two.events);

  // Every print agrees, instrument by instrument, in each instrument's own order.
  REQUIRE(wholePrints.perInstrument.size() == 2);
  CHECK(wholePrints.perInstrument == shardPrints.perInstrument);
  CHECK(wholePrints.perInstrument[1].size() > 50);

  // Each shard minted its ids in its own namespace, so a merged stream never collides.
  REQUIRE(shardPrints.idNamespaces.size() == 2);
  CHECK(shardPrints.idNamespaces.contains(1));
  CHECK(shardPrints.idNamespaces.contains(2));

  // The books left standing agree too, by the client names and displays that survive
  // renumbering, and the venue-wide sweep emptied participant two's books on both shards.
  for (std::uint32_t instrument = 1; instrument <= 2; instrument++) {
    const Run& shard = instrument == 1 ? one : two;
    for (std::int32_t side = 0; side <= 1; side++) {
      CHECK(ladderShape(whole.partition, instrument, side) ==
            ladderShape(shard.partition, instrument, side));
    }
  }

  // The same shard, replayed, is byte-identical: the determinism claim survives the split.
  Run again;
  again.partition.serve({1});
  again.partition.shard(1);
  again.play(flow);
  CHECK(one.events.captured() == again.events.captured());
}
