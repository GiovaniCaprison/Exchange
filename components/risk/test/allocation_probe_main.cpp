// The allocation proof for the gate (P-6): admissions and reconciling events pumped with the
// global allocator counting after warm-up; the tables, the ledger and the token buckets are all
// fixed at construction, so one request is a failure. Refusal is counting rather than crashing
// so the probe can say how many times it was asked.
//
//   risk-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

#include "clock.hpp"
#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderRemoved.h"
#include "risk.hpp"

namespace {

bool locked = false;
std::uint64_t denied = 0;

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
  using exchange::risk::Intent;
  using exchange::risk::Limits;
  using exchange::risk::Risk;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  exchange::sequencer::VirtualClock clock;
  Limits generous;
  generous.burst = 1'000'000;
  Risk<exchange::sequencer::VirtualClock> risk(clock, {{7, generous}});

  // The reconciliation events, forged once before the lock.
  char accepted[128] = {};
  char removed[128] = {};
  {
    sbe::OrderAccepted event;
    event.wrapAndApplyHeader(accepted, 0, sizeof accepted);
    event.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(1).reserved(0);
    event.participantId(7);
    sbe::OrderRemoved gone;
    gone.wrapAndApplyHeader(removed, 0, sizeof removed);
    gone.context().sequence(2).inputSequence(2).timestamp(2).instrumentId(1).reserved(0);
    gone.quantity(1).reason(sbe::RemoveReason::CANCELLED);
  }
  const std::size_t acceptedSize =
      sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength();
  const std::size_t removedSize =
      sbe::MessageHeader::encodedLength() + sbe::OrderRemoved::sbeBlockLength();

  const std::uint64_t commands = 200'000;
  const std::uint64_t warm = 512;
  std::uint64_t at = 0;
  const auto pump = [&](const std::uint64_t index) {
    clock.advance(1'000'000);
    Intent order{sbe::NewOrder::sbeTemplateId(), 900 + index, 1, 1000, 5, true, false};
    if (!risk.admit(7, order).admitted) {
      std::abort();
    }
    // The venue accepts and later removes it, and the ledger breathes in and out.
    const std::uint64_t orderId = index + 1;
    std::memcpy(accepted + sbe::MessageHeader::encodedLength() + 32, &orderId, sizeof orderId);
    std::uint64_t clientOrderId = 900 + index;
    std::memcpy(accepted + sbe::MessageHeader::encodedLength() + 40, &clientOrderId,
                sizeof clientOrderId);
    risk.onEvent(accepted, acceptedSize);
    std::memcpy(removed + sbe::MessageHeader::encodedLength() + 32, &orderId, sizeof orderId);
    risk.onEvent(removed, removedSize);
  };
  for (; at < warm; at++) {
    pump(at);
  }
  locked = true;
  for (; at < commands; at++) {
    pump(at);
    if (misbehave) {
      // A direct call to the allocation function, which the compiler cannot elide the way it
      // may elide a new-expression whose result goes nowhere.
      ::operator delete(::operator new(64));
    }
  }
  locked = false;

  if (risk.exposure(7) != 0) {
    std::fprintf(stderr, "the ledger failed to breathe out\n");
    return 1;
  }
  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
