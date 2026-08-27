// The market data suites' shared machinery: one matcher run whose events feed the builder, a
// deliberately naive public-side book that consumes the feed the way any outsider would, and
// walkers for both so equality claims compare like with like. The naive book is std::map on
// purpose: the reference should be obviously correct, and the builder is held to it and to the
// matcher's own ladders at once.

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <map>
#include <tuple>
#include <utility>
#include <vector>

#include "builder.hpp"
#include "exchange_protocol/MessageHeader.h"
#include "flow.hpp"
#include "harness.hpp"
#include "partition.hpp"
#include "publisher.hpp"

namespace exchange::marketdata::test {

namespace sbe = ::exchange::protocol;
using exchange::matcher::test::CapturingRing;
using exchange::matcher::test::CommandWriter;

struct CapturingSink {
  std::vector<std::vector<char>> packets;
  void send(const char* bytes, const std::size_t length) {
    packets.emplace_back(bytes, bytes + length);
  }
};

using WiredPublisher = Publisher<CapturingSink, CapturingSink>;
using WiredBuilder = Builder<WiredPublisher>;

// Walks messages laid end to end, the way the matcher's capturing ring stores events.
template <typename Handler>
void eachMessage(std::vector<char>& bytes, Handler&& handler) {
  std::size_t at = 0;
  while (at < bytes.size()) {
    sbe::MessageHeader wrap;
    wrap.wrap(bytes.data(), at, 0, bytes.size());
    const std::size_t length = sbe::MessageHeader::encodedLength() + wrap.blockLength();
    handler(bytes.data() + at, length);
    at += length;
  }
}

// One venue in a box: the flow through a partition, the events through the builder.
struct Venue {
  CapturingRing events;
  exchange::matcher::Partition<CapturingRing> partition{events};
  CapturingSink a;
  CapturingSink b;
  WiredPublisher publisher{a, b};
  WiredBuilder builder{publisher};
  std::size_t consumed = 0;

  void play(const std::vector<CommandWriter::Framed>& flow, const std::size_t from,
            const std::size_t until) {
    for (std::size_t at = from; at < until; at++) {
      std::vector<char> bytes = flow[at].bytes;
      partition.onCommand(bytes.data(), 0, bytes.size());
      std::vector<char> fresh(events.captured().begin() + static_cast<long>(consumed),
                              events.captured().end());
      consumed = events.captured().size();
      eachMessage(fresh, [&](char* message, const std::size_t length) {
        builder.onEvent(message, length);
      });
    }
    publisher.flush();
  }
};

// The outsider's book: public messages in, the visible book out, obviously.
struct NaiveBook {
  struct Order {
    std::int64_t price = 0;
    std::int64_t quantity = 0;
    std::uint32_t instrument = 0;
    std::uint8_t side = 0;
    std::uint64_t arrival = 0;
  };
  std::map<std::uint64_t, Order> orders;
  std::uint64_t arrivals = 0;

  void onPublic(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    if (wrap.templateId() == sbe::PublicOrderAdded::sbeTemplateId()) {
      sbe::PublicOrderAdded added;
      added.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          length);
      Order& order = orders[added.orderId()];
      order.price = added.price();
      order.quantity = added.quantity();
      order.instrument = added.context().instrumentId();
      order.side = added.side() == sbe::Side::BUY ? 0 : 1;
      order.arrival = ++arrivals;
    } else if (wrap.templateId() == sbe::PublicOrderExecuted::sbeTemplateId()) {
      sbe::PublicOrderExecuted executed;
      executed.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                             length);
      auto found = orders.find(executed.orderId());
      if (found != orders.end()) {
        found->second.quantity -= executed.quantity();
        if (found->second.quantity <= 0) {
          orders.erase(found);
        }
      }
    } else if (wrap.templateId() == sbe::PublicOrderReduced::sbeTemplateId()) {
      sbe::PublicOrderReduced reduced;
      reduced.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
      auto found = orders.find(reduced.orderId());
      if (found != orders.end()) {
        found->second.quantity = reduced.quantity();
      }
    } else if (wrap.templateId() == sbe::PublicOrderRemoved::sbeTemplateId()) {
      sbe::PublicOrderRemoved removed;
      removed.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
      orders.erase(removed.orderId());
    }
  }

  // Canonical form: (instrument, side, priority price order, arrival) with id, price, quantity.
  std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> canonical() const {
    std::vector<const std::pair<const std::uint64_t, Order>*> sorted;
    sorted.reserve(orders.size());
    for (const auto& entry : orders) {
      sorted.push_back(&entry);
    }
    std::sort(sorted.begin(), sorted.end(), [](const auto* a, const auto* b) {
      if (a->second.instrument != b->second.instrument) {
        return a->second.instrument < b->second.instrument;
      }
      if (a->second.side != b->second.side) {
        return a->second.side < b->second.side;
      }
      if (a->second.price != b->second.price) {
        return a->second.side == 0 ? a->second.price > b->second.price
                                   : a->second.price < b->second.price;
      }
      return a->second.arrival < b->second.arrival;
    });
    std::vector<std::tuple<std::uint64_t, std::int64_t, std::int64_t>> out;
    out.reserve(sorted.size());
    for (const auto* entry : sorted) {
      out.emplace_back(entry->first, entry->second.price, entry->second.quantity);
    }
    return out;
  }
};

}  // namespace exchange::marketdata::test
