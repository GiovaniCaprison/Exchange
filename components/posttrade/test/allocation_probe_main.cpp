// The allocation proof for post-trade (P-6): a session's worth of accepts, rests, fills and
// cancels through the ledger, positions moving and the tape growing inside its reservation, with
// the global allocator counting after warm-up. The ownership table, the tape and the accounts
// are all sized at construction, so one request is a failure.
//
//   posttrade-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>

#include "ledger.hpp"

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
  using namespace exchange::posttrade;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  Ledger ledger;
  char message[128] = {};
  const std::uint64_t events = 200'000;
  const std::uint64_t warm = 512;

  // One turn: a bid rests, an offer takes part of it, the remainder is cancelled. Two
  // participants alternate sides so the accounts breathe on both.
  const auto turn = [&](const std::uint64_t index) {
    const std::uint64_t restingId = index * 2 + 1;
    const std::uint64_t takerId = index * 2 + 2;
    const std::uint32_t resting = index % 2 == 0 ? 7U : 8U;
    const std::uint32_t taker = index % 2 == 0 ? 8U : 7U;
    {
      sbe::OrderAccepted event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context().sequence(index * 5 + 1).inputSequence(index).timestamp(index).instrumentId(1);
      event.orderId(restingId).clientOrderId(restingId).participantId(resting);
      ledger.onEvent(message,
                     sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength());
    }
    {
      sbe::OrderRested event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context().sequence(index * 5 + 2).inputSequence(index).timestamp(index).instrumentId(1);
      event.orderId(restingId).price(10'000).quantity(5);
      event.side(index % 2 == 0 ? sbe::Side::BUY : sbe::Side::SELL);
      ledger.onEvent(message,
                     sbe::MessageHeader::encodedLength() + sbe::OrderRested::sbeBlockLength());
    }
    {
      sbe::OrderAccepted event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context().sequence(index * 5 + 3).inputSequence(index).timestamp(index).instrumentId(1);
      event.orderId(takerId).clientOrderId(takerId).participantId(taker);
      ledger.onEvent(message,
                     sbe::MessageHeader::encodedLength() + sbe::OrderAccepted::sbeBlockLength());
    }
    {
      sbe::OrderExecuted event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context().sequence(index * 5 + 4).inputSequence(index).timestamp(index).instrumentId(1);
      event.executionId(index + 1).aggressorOrderId(takerId).restingOrderId(restingId);
      event.price(10'000).quantity(4);
      ledger.onEvent(message,
                     sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength());
    }
    {
      sbe::OrderRemoved event;
      event.wrapAndApplyHeader(message, 0, sizeof message);
      event.context().sequence(index * 5 + 5).inputSequence(index).timestamp(index).instrumentId(1);
      event.orderId(restingId).quantity(1);
      event.reason(sbe::RemoveReason::CANCELLED);
      ledger.onEvent(message,
                     sbe::MessageHeader::encodedLength() + sbe::OrderRemoved::sbeBlockLength());
    }
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

  if (ledger.trades().size() != events) {
    std::fprintf(stderr, "the probe did not trade\n");
    return 1;
  }
  if (ledger.positionOf(7, 1) + ledger.positionOf(8, 1) != 0) {
    std::fprintf(stderr, "conservation failed inside the probe\n");
    return 1;
  }
  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
