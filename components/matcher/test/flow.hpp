// The deterministic generated flow every proof shares: a seed in, framed sequenced commands out,
// with the sequencer's stamping done by the writer so a journal, a partition and the binary all
// see the same bytes. Fidelity to any real market is not the point here; reaching states is (the
// measurement harness owns its own flow questions).

#pragma once

#include <algorithm>
#include <cstdint>
#include <vector>

#include "harness.hpp"

namespace exchange::matcher::test {

inline constexpr std::uint32_t FLOW_INSTRUMENT = 1;

inline std::vector<CommandWriter::Framed> generatedFlow(const std::uint64_t seed,
                                                        const std::uint64_t commands) {
  CommandWriter writer;
  std::vector<CommandWriter::Framed> flow;
  flow.push_back(
      writer.instrument(FLOW_INSTRUMENT, 5, 1, 5, 1'000'000, 100'000'000, 100'000, false));
  flow.push_back(writer.session(FLOW_INSTRUMENT, sbe::SessionState::CONTINUOUS));
  std::uint64_t state = seed * 2685821657736338717ULL + 1;
  const auto next = [&state]() {
    state ^= state >> 12;
    state ^= state << 25;
    state ^= state >> 27;
    return state * 2685821657736338717ULL;
  };
  std::vector<std::uint64_t> live;
  std::uint64_t client = 0;
  for (std::uint64_t at = 0; at < commands; at++) {
    const std::uint64_t roll = next() % 100;
    if (at % 1500 == 0) {
      flow.push_back(writer.session(FLOW_INSTRUMENT, at % 3000 == 0
                                                         ? sbe::SessionState::OPENING_AUCTION
                                                         : sbe::SessionState::CONTINUOUS));
      continue;
    }
    if (roll < 40 && !live.empty()) {
      const std::size_t pick = next() % live.size();
      const std::uint64_t name = live[pick];
      live[pick] = live.back();
      live.pop_back();
      flow.push_back(
          writer.cancel(FLOW_INSTRUMENT, name, 1 + static_cast<std::uint32_t>(name % 5)));
      continue;
    }
    if (roll < 50 && !live.empty()) {
      const std::uint64_t name = live[next() % live.size()];
      flow.push_back(writer.replace(FLOW_INSTRUMENT, name, 1 + static_cast<std::uint32_t>(name % 5),
                                    1 + static_cast<std::int64_t>(next() % 60),
                                    100000 + 5 * static_cast<std::int64_t>(next() % 40) -
                                        5 * static_cast<std::int64_t>(next() % 40)));
      continue;
    }
    client++;
    const bool aggressive = roll >= 88;
    const bool buy = next() % 2 == 0;
    const std::int64_t offset = 5 * static_cast<std::int64_t>(next() % (aggressive ? 3 : 40));
    const std::int64_t price =
        buy ? 100000 - (aggressive ? -offset : offset) : 100000 + (aggressive ? -offset : offset);
    const bool ioc = aggressive || next() % 20 == 0;
    const std::int64_t quantity = 1 + static_cast<std::int64_t>(next() % 40);
    const std::int64_t display =
        next() % 12 == 0 ? 1 + static_cast<std::int64_t>(next() % quantity) : 0;
    const std::int64_t trigger = next() % 40 == 0
                                     ? 100000 + 5 * static_cast<std::int64_t>(next() % 30) -
                                           5 * static_cast<std::int64_t>(next() % 30)
                                     : 0;
    if (!ioc) {
      live.push_back(client);
    }
    flow.push_back(writer.newOrder(
        FLOW_INSTRUMENT, client, 1 + static_cast<std::uint32_t>(client % 5),
        buy ? sbe::Side::BUY : sbe::Side::SELL, sbe::Pricing::LIMIT,
        ioc ? sbe::TimeInForce::IMMEDIATE_OR_CANCEL : sbe::TimeInForce::GOOD_TILL_CANCEL, false,
        std::max<std::int64_t>(price, 5), quantity, 0, display,
        trigger == 0 ? 0 : std::max<std::int64_t>(trigger, 5),
        next() % 10 == 0 ? 1 + next() % 5 : 0));
  }
  return flow;
}

}  // namespace exchange::matcher::test
