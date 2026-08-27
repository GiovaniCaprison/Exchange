// The ledger held to the venue itself: a generated session runs through the matcher while the
// gate shadows every admission and hears every event, and at checkpoints the ledger equals what
// the engine actually holds for each participant, resting remaining plus waiting stops, at the
// admitted prices. Then everything is cancelled and the ledger drains to exactly zero, which is
// the conservation the protocol promises to the invariants.

#include <catch2/catch_test_macros.hpp>
#include <cstdint>
#include <cstring>
#include <vector>

#include "clock.hpp"
#include "flow.hpp"
#include "harness.hpp"
#include "ladder.hpp"
#include "partition.hpp"
#include "risk.hpp"

using namespace exchange::risk;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;
using exchange::matcher::test::generatedFlow;
using exchange::sequencer::VirtualClock;
namespace sbe = exchange::protocol;

namespace {

// What the engine truly holds for a participant: remaining quantity at each order's own price,
// across the ladders and the trigger book alike.
std::int64_t engineHolds(const exchange::matcher::Partition<CapturingRing>& partition,
                         const std::uint32_t instrument, const std::uint32_t participant) {
  const auto& engine = partition.engine(instrument);
  const auto& book = engine.book();
  const auto& slab = engine.slab();
  std::int64_t held = 0;
  for (std::int32_t side = 0; side <= 1; side++) {
    const auto& ladder = book.ladderOf(side);
    for (std::int32_t rank = ladder.best(); rank != exchange::matcher::Ladder::EMPTY;
         rank = ladder.occupiedFrom(rank + 1)) {
      for (std::int32_t slot = ladder.headAt(rank); slot != 0;
           slot = static_cast<std::int32_t>(slab.hot(slot).next)) {
        if (slab.cold(slot).participantId == participant) {
          held += slab.hot(slot).remaining * book.priceOfRank(side, rank);
        }
      }
    }
  }
  std::vector<std::int32_t> waiting;
  engine.triggers().of(participant, waiting);
  for (const std::int32_t slot : waiting) {
    held += slab.hot(slot).remaining * book.priceOfTick(slab.hot(slot).tick);
  }
  return held;
}

Intent intentOf(const CommandWriter::Framed& framed) {
  std::vector<char> copy = framed.bytes;
  sbe::MessageHeader wrap;
  wrap.wrap(copy.data(), 0, 0, copy.size());
  Intent intent;
  intent.templateId = wrap.templateId();
  if (wrap.templateId() == sbe::NewOrder::sbeTemplateId()) {
    sbe::NewOrder command;
    command.wrapForDecode(copy.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          copy.size());
    intent.clientOrderId = command.clientOrderId();
    intent.instrumentId = command.context().instrumentId();
    intent.price = command.price();
    intent.quantity = command.quantity();
    intent.buying = command.side() == sbe::Side::BUY;
    intent.market = command.pricing() == sbe::Pricing::MARKET;
  } else if (wrap.templateId() == sbe::ReplaceOrder::sbeTemplateId()) {
    sbe::ReplaceOrder command;
    command.wrapForDecode(copy.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          copy.size());
    intent.clientOrderId = command.clientOrderId();
    intent.instrumentId = command.context().instrumentId();
    intent.price = command.price();
    intent.quantity = command.quantity();
  } else if (wrap.templateId() == sbe::CancelOrder::sbeTemplateId()) {
    sbe::CancelOrder command;
    command.wrapForDecode(copy.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          copy.size());
    intent.clientOrderId = command.clientOrderId();
    intent.instrumentId = command.context().instrumentId();
  } else if (wrap.templateId() == sbe::MassCancel::sbeTemplateId()) {
    sbe::MassCancel command;
    command.wrapForDecode(copy.data(), wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          copy.size());
    intent.clientOrderId = command.clientOrderId();
    intent.instrumentId = command.context().instrumentId();
  }
  return intent;
}

}  // namespace

TEST_CASE("the ledger tracks the engine and drains to zero when everything closes") {
  const std::vector<CommandWriter::Framed> flow = generatedFlow(113, 6000);
  VirtualClock clock;
  Limits generous;
  generous.burst = 1'000'000;
  std::vector<std::pair<std::uint32_t, Limits>> table;
  for (std::uint32_t participant = 1; participant <= 5; participant++) {
    table.emplace_back(participant, generous);
  }
  Risk<VirtualClock> risk(clock, table);

  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition(events);
  std::size_t consumed = 0;

  const auto pump = [&](const CommandWriter::Framed& framed, const bool admitFirst) {
    if (admitFirst) {
      sbe::MessageHeader wrap;
      std::vector<char> copy = framed.bytes;
      wrap.wrap(copy.data(), 0, 0, copy.size());
      const std::uint16_t id = wrap.templateId();
      if (id == sbe::NewOrder::sbeTemplateId() || id == sbe::CancelOrder::sbeTemplateId() ||
          id == sbe::ReplaceOrder::sbeTemplateId() || id == sbe::MassCancel::sbeTemplateId()) {
        Intent intent = intentOf(framed);
        const std::uint32_t participant = 1 + static_cast<std::uint32_t>(intent.clientOrderId % 5);
        REQUIRE(risk.admit(participant, intent).admitted);
      }
    }
    std::vector<char> bytes = framed.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
    std::vector<char> fresh(events.captured().begin() + static_cast<long>(consumed),
                            events.captured().end());
    consumed = events.captured().size();
    std::size_t at = 0;
    while (at < fresh.size()) {
      sbe::MessageHeader wrap;
      wrap.wrap(fresh.data(), at, 0, fresh.size());
      const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
      risk.onEvent(fresh.data() + at, length);
      at += length;
    }
  };

  for (std::size_t at = 0; at < flow.size(); at++) {
    pump(flow[at], true);
    if (at % 750 == 749) {
      for (const auto& [participant, limits] : table) {
        INFO("after command " << at << " for participant " << participant);
        CHECK(risk.exposure(participant) ==
              engineHolds(partition, exchange::matcher::test::FLOW_INSTRUMENT, participant));
      }
    }
  }

  // Conservation: everything closes, and the ledger answers zero for everyone.
  CommandWriter writer;
  for (std::uint32_t participant = 1; participant <= 5; participant++) {
    const CommandWriter::Framed sweep =
        writer.massCancel(exchange::matcher::test::FLOW_INSTRUMENT, 0, participant);
    pump(sweep, false);
  }
  for (const auto& [participant, limits] : table) {
    INFO("after the sweep for participant " << participant);
    CHECK(risk.exposure(participant) == 0);
  }
}
