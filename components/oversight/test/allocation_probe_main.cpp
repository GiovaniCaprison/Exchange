// The allocation proof for oversight (P-6): surveillance digests a session's worth of accepts,
// rests, fills and cancels, sweeps included, and the drop copy machine routes the same stream to
// a bound watcher draining as it goes, with the global allocator counting after warm-up. The
// tables, the histories and the reserved logs are all sized at construction, so one request is a
// failure.
//
//   oversight-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "clock.hpp"
#include "dropcopy.hpp"
#include "surveillance.hpp"

namespace {

bool locked = false;
std::uint64_t denied = 0;

struct DiscardingSink {
  std::uint64_t alerts = 0;
  void operator()(char*, const std::size_t) { alerts++; }
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
  using namespace exchange::oversight;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  DiscardingSink sink;
  Config config;
  config.windowNanos = 64;
  config.minimumCancelled = 1;
  Surveillance<DiscardingSink> watch(sink, config);
  exchange::sequencer::VirtualClock clock;
  DropCopy<exchange::sequencer::VirtualClock> copies(clock, {{7, 42}});
  {
    char space[64] = {};
    sbe::LoginRequest login;
    const std::uint16_t length =
        sbe::MessageHeader::encodedLength() + sbe::LoginRequest::sbeBlockLength();
    std::memcpy(space, &length, sizeof length);
    login.wrapAndApplyHeader(space + 2, 0, sizeof space - 2);
    login.expectedSequence(0).credential(42).participantId(7).reserved(0);
    const int slot = copies.opened();
    copies.received(slot, space, 2 + length);
    copies.drained(slot, copies.outbound(slot).second);
  }

  char message[128] = {};
  const std::uint64_t events = 200'000;
  const std::uint64_t warm = 512;

  // One turn: an order is accepted and rested, a second one crosses it for part, the remainder
  // is cancelled. Fills, cancels, evaluations and periodic sweeps all run under the lock, and
  // every event also rides through the drop copy machine to its bound watcher.
  const auto turn = [&](const std::uint64_t index) {
    const std::uint64_t restingId = index * 2 + 1;
    const std::uint64_t takerId = index * 2 + 2;
    const auto both = [&](const std::size_t length) {
      watch.onEvent(message, length);
      copies.onEvent(message, length);
    };
    {
      sbe::OrderAccepted event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context()
          .sequence(index * 5 + 1)
          .inputSequence(index)
          .timestamp(index * 4)
          .instrumentId(1)
          .reserved(0);
      event.orderId(restingId).clientOrderId(restingId).participantId(7);
      both(sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength());
    }
    {
      sbe::OrderRested event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context()
          .sequence(index * 5 + 2)
          .inputSequence(index)
          .timestamp(index * 4 + 1)
          .instrumentId(1)
          .reserved(0);
      event.orderId(restingId).price(10'000).quantity(5);
      event.side(sbe::Side::BUY);
      both(sbe::MessageHeader::encodedLength() + sbe::OrderRested::sbeBlockLength());
    }
    {
      sbe::OrderAccepted event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context()
          .sequence(index * 5 + 3)
          .inputSequence(index)
          .timestamp(index * 4 + 2)
          .instrumentId(1)
          .reserved(0);
      event.orderId(takerId).clientOrderId(takerId).participantId(8);
      both(sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength());
    }
    {
      sbe::OrderExecuted event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context()
          .sequence(index * 5 + 4)
          .inputSequence(index)
          .timestamp(index * 4 + 3)
          .instrumentId(1)
          .reserved(0);
      event.executionId(index).aggressorOrderId(takerId).restingOrderId(restingId);
      event.price(10'000).quantity(4);
      both(sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength());
    }
    {
      sbe::OrderRemoved event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context()
          .sequence(index * 5 + 5)
          .inputSequence(index)
          .timestamp(index * 4 + 4)
          .instrumentId(1)
          .reserved(0);
      event.orderId(restingId).quantity(1);
      event.reason(sbe::RemoveReason::CANCELLED);
      both(sbe::MessageHeader::encodedLength() + sbe::OrderRemoved::sbeBlockLength());
    }
    copies.drained(0, copies.outbound(0).second);
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

  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
