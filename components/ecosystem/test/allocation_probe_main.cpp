// The allocation proof for the participant's seat (P-6): the feed handler digests a session's
// worth of public flow, orders arriving, trading out and emptying the touch into rescans, while
// the order entry client submits, hears its acceptances, rests, fills and dies, with the global
// allocator counting after warm-up. Both clients size everything at construction, so one request
// is a failure.
//
//   ecosystem-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "clock.hpp"
#include "feedhandler.hpp"
#include "orderentry.hpp"
#include "ranges.hpp"

namespace {

bool locked = false;
std::uint64_t denied = 0;

struct SilentRewind {
  void request(const std::uint64_t, const std::uint32_t) {}
};

}  // namespace

void* operator new(const std::size_t size) {
  if (locked) {
    denied++;
  }
  void* const memory = std::malloc(size);
  if (memory == nullptr) {
    throw std::bad_alloc();
  }
  return memory;
}

void* operator new[](const std::size_t size) { return operator new(size); }

void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }
void operator delete[](void* memory) noexcept { std::free(memory); }
void operator delete[](void* memory, std::size_t) noexcept { std::free(memory); }

int main(const int count, char** values) {
  namespace sbe = exchange::protocol;
  using namespace exchange::ecosystem;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  SilentRewind rewind;
  FeedHandler<SilentRewind> feed(rewind);
  exchange::sequencer::VirtualClock clock;
  OrderEntry<exchange::sequencer::VirtualClock> client(clock, 7, 42);
  client.connected();
  client.drainedBy(client.outbound().second);
  {
    char space[64] = {};
    sbe::LoginAccepted accepted;
    const std::uint16_t length =
        sbe::MessageHeader::encodedLength() + sbe::LoginAccepted::sbeBlockLength();
    std::memcpy(space, &length, sizeof length);
    accepted.wrapAndApplyHeader(space + 2, 0, sizeof space - 2);
    accepted.nextSequence(1).participantId(7).reserved(0);
    client.received(space, 2 + length);
  }

  // Wide enough for the sweep turn, which carries the add, the execution and seventeen removals.
  char packet[2048] = {};
  char message[128] = {};
  char frame[128] = {};
  std::uint64_t sequence = 1;
  std::uint64_t sessionSeen = 0;
  const std::uint64_t events = 200'000;
  const std::uint64_t warm = 512;

  // The public side keeps a breathing book: each turn's order mostly trades away, a bounded
  // ring of leftovers rests at the touch, and every so often the leftovers are swept, which
  // empties the touch and runs the rescan under the lock too.
  std::uint64_t leftovers[16] = {};
  std::size_t oldest = 0;
  std::size_t living = 0;

  // One turn of both machines: a public order arrives and trades, and the client's own order is
  // submitted, accepted, rested, and filled.
  const auto turn = [&](const std::uint64_t index) {
    exchange::common::ranges::Builder builder(packet, sizeof packet);
    builder.open(sequence, 1);
    const auto put = [&](const char* bytes, const std::size_t size) {
      builder.add(bytes, static_cast<std::uint16_t>(size));
      sequence++;
    };
    const auto remove = [&](const std::uint64_t orderId) {
      sbe::PublicOrderRemoved removed;
      removed.wrapAndApplyHeader(message, 0, sizeof message);
      removed.context().timestamp(index).instrumentId(1).reserved(0);
      removed.orderId(orderId);
      put(message, sbe::MessageHeader::encodedLength() + sbe::PublicOrderRemoved::sbeBlockLength());
    };
    {
      sbe::PublicOrderAdded added;
      added.wrapAndApplyHeader(message, 0, sizeof message);
      added.context().timestamp(index).instrumentId(1).reserved(0);
      added.orderId(1'000'000 + index).price(10'000).quantity(5);
      added.side(sbe::Side::BUY);
      put(message, sbe::MessageHeader::encodedLength() + sbe::PublicOrderAdded::sbeBlockLength());
    }
    {
      sbe::PublicOrderExecuted executed;
      executed.wrapAndApplyHeader(message, 0, sizeof message);
      executed.context().timestamp(index).instrumentId(1).reserved(0);
      executed.orderId(1'000'000 + index).executionId(index).price(10'000).quantity(4);
      put(message,
          sbe::MessageHeader::encodedLength() + sbe::PublicOrderExecuted::sbeBlockLength());
    }
    if (living == 16) {
      remove(leftovers[oldest]);
      oldest = (oldest + 1) & 15;
      living--;
    }
    leftovers[(oldest + living) & 15] = 1'000'000 + index;
    living++;
    if (index % 256 == 255) {
      // The sweep: the touch empties and the rescan runs.
      while (living > 0) {
        remove(leftovers[oldest]);
        oldest = (oldest + 1) & 15;
        living--;
      }
    }
    const std::size_t built = builder.close();
    feed.onPacket(packet, built);

    const std::uint64_t clientOrderId = client.newOrder(
        1, sbe::Side::BUY, sbe::Pricing::LIMIT, sbe::TimeInForce::GOOD_TILL_CANCEL, 10'000, 5);
    client.drainedBy(client.outbound().second);
    const auto deliver = [&](const auto& fill, auto encoder, const std::size_t size) {
      const std::uint16_t length = static_cast<std::uint16_t>(size);
      std::memcpy(frame, &length, sizeof length);
      encoder.wrapAndApplyHeader(frame + 2, 0, sizeof frame - 2);
      fill(encoder);
      client.received(frame, 2 + size);
    };
    deliver(
        [&](sbe::OrderAccepted& event) {
          event.context()
              .sequence(++sessionSeen)
              .inputSequence(1)
              .timestamp(index)
              .instrumentId(1)
              .reserved(0);
          event.orderId(index + 1).clientOrderId(clientOrderId).participantId(7);
        },
        sbe::OrderAccepted{},
        sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength());
    deliver(
        [&](sbe::OrderRested& event) {
          event.context()
              .sequence(++sessionSeen)
              .inputSequence(1)
              .timestamp(index)
              .instrumentId(1)
              .reserved(0);
          event.orderId(index + 1).price(10'000).quantity(5);
          event.side(sbe::Side::BUY);
        },
        sbe::OrderRested{},
        sbe::MessageHeader::encodedLength() + sbe::OrderRested::sbeBlockLength());
    deliver(
        [&](sbe::OrderExecuted& event) {
          event.context()
              .sequence(++sessionSeen)
              .inputSequence(1)
              .timestamp(index)
              .instrumentId(1)
              .reserved(0);
          event.executionId(index).aggressorOrderId(900'000'000).restingOrderId(index + 1);
          event.price(10'000).quantity(5);
        },
        sbe::OrderExecuted{},
        sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength());
  };

  std::uint64_t at = 0;
  for (; at < warm; at++) {
    turn(at);
  }
  locked = true;
  for (; at < events; at++) {
    turn(at);
    if (misbehave) {
      // A direct call to the allocation function, which the compiler cannot elide the way it
      // may elide a new-expression whose result goes nowhere.
      ::operator delete(::operator new(64));
    }
  }
  locked = false;

  if (feed.lastTradeOf(1).count != events || client.executions() != events) {
    std::fprintf(stderr, "the probe did not exercise both machines\n");
    return 1;
  }
  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
