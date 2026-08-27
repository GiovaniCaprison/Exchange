// The allocation proof for the market's clock (P-6): a session's worth of prints monitored, the
// bands maintained, halts fired and reopened, ticks ticking, with the global allocator counting
// after warm-up. The rings, the pending window and the band buffers are all fixed at
// construction, so one request is a failure.
//
//   operations-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "clock.hpp"
#include "scheduler.hpp"

namespace {

bool locked = false;
std::uint64_t denied = 0;

class DiscardingRing {
 public:
  std::size_t claim(const std::size_t length) {
    const std::size_t aligned = (length + 7) & ~std::size_t{7};
    if (cursor_ + aligned > sizeof space_) {
      cursor_ = 0;
    }
    const std::size_t at = cursor_;
    cursor_ += aligned;
    return at;
  }
  char* buffer() { return space_; }
  void commit() {}
  void publish() {}

 private:
  char space_[1 << 16] = {};
  std::size_t cursor_ = 0;
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
  using namespace exchange::operations;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  exchange::sequencer::VirtualClock wall;
  DiscardingRing out;
  Config config;
  config.instruments = {1};
  config.bandBasisPoints = 500;
  config.windowNanos = 1'000;
  config.haltNanos = 100;
  config.auctionNanos = 100;
  Scheduler<DiscardingRing, exchange::sequencer::VirtualClock> scheduler(out, wall, config);

  char trading[128] = {};
  {
    sbe::SessionStateChanged event;
    event.wrapAndApplyHeader(trading, 0, sizeof trading);
    event.context().sequence(1).inputSequence(1).timestamp(1).instrumentId(1).reserved(0);
    event.state(sbe::SessionState::CONTINUOUS);
  }
  const std::size_t tradingSize =
      sbe::MessageHeader::encodedLength() + sbe::SessionStateChanged::sbeBlockLength();
  scheduler.onEvent(trading, tradingSize);

  char print[128] = {};
  {
    sbe::OrderExecuted event;
    event.wrapAndApplyHeader(print, 0, sizeof print);
    event.context().sequence(1).inputSequence(1).timestamp(0).instrumentId(1).reserved(0);
    event.executionId(1).aggressorOrderId(900).restingOrderId(901).price(10'000).quantity(1);
  }
  const std::size_t printSize =
      sbe::MessageHeader::encodedLength() + sbe::OrderExecuted::sbeBlockLength();

  const std::uint64_t events = 200'000;
  const std::uint64_t warm = 512;
  std::uint64_t at = 0;
  const auto pump = [&](const std::uint64_t index) {
    wall.advance(10);
    const std::uint64_t when = index + 1;
    // Every four thousandth print panics, so halts and reopens run under the lock too.
    const std::int64_t price = index % 4000 == 3999 ? 20'000 : 10'000;
    std::memcpy(print + sbe::MessageHeader::encodedLength() + 16, &when, sizeof when);
    std::memcpy(print + sbe::MessageHeader::encodedLength() + 32 + 8 + 16, &price, sizeof price);
    scheduler.onEvent(print, printSize);
    scheduler.onTick();
    if (index % 4000 == 0 && index > 0) {
      // The venue confirms the reopening states the scheduler asked for.
      scheduler.onEvent(trading, tradingSize);
    }
  };
  for (; at < warm; at++) {
    pump(at);
  }
  locked = true;
  for (; at < events; at++) {
    pump(at);
    if (misbehave) {
      // A direct call to the allocation function, which the compiler cannot elide the way it
      // may elide a new-expression whose result goes nowhere.
      ::operator delete(::operator new(64));
    }
  }
  locked = false;

  if (scheduler.halts() == 0) {
    std::fprintf(stderr, "the probe never exercised a halt\n");
    return 1;
  }
  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
