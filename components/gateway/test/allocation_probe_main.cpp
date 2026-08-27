// The allocation proof for the gateway (P-6): a logged-in session pumping orders, taking
// acknowledgments, and hearing its reports back, with the global allocator counting every
// request after warm-up. Everything the steady state touches, the reassembler, the pending ring,
// the order table, the stream log and the outbound buffer, is fixed or reserved at construction,
// so one request is a failure. Refusal is counting rather than crashing so the probe can say how
// many times it was asked.
//
//   gateway-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "client.hpp"
#include "clock.hpp"
#include "gateway.hpp"

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
  namespace gate = exchange::gateway;
  namespace test = exchange::gateway::test;
  namespace sbe = exchange::protocol;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  const std::uint64_t commands = 20'000;
  exchange::matcher::test::CommandWriter writer;
  std::vector<std::vector<char>> orders;
  std::vector<std::vector<char>> events;
  orders.reserve(commands);
  events.reserve(commands);
  for (std::uint64_t at = 0; at < commands; at++) {
    orders.push_back(test::commandBytes(
        writer.newOrder(1, 900 + at, 7, sbe::Side::BUY, sbe::Pricing::LIMIT,
                        sbe::TimeInForce::GOOD_TILL_CANCEL, false, 1000, 1, 0, 0, 0, 0)));
    events.push_back(test::acceptedEvent(at + 1, 7, 900 + at));
  }
  char ack[64] = {};
  {
    sbe::CommandSequenced sequenced;
    sequenced.wrapAndApplyHeader(ack, 0, sizeof ack);
    sequenced.gatewaySequence(0).sequence(0).timestamp(0);
  }

  DiscardingRing submissions;
  exchange::sequencer::ScriptedClock clock;
  gate::Gateway<DiscardingRing, exchange::sequencer::ScriptedClock> gateway(submissions, clock, 1,
                                                                            {{7, 42}});
  const int slot = gateway.opened();
  std::vector<char> login = test::loginBytes(7, 42, 0);
  gateway.received(slot, login.data(), login.size());

  // Warm-up runs a few hundred commands unlocked; the reserved vectors reach steady state.
  const std::uint64_t warm = 256;
  std::uint64_t at = 0;
  const auto pump = [&](const std::uint64_t index) {
    gateway.received(slot, orders[index].data(), orders[index].size());
    const std::uint64_t gatewaySequence = index + 1;
    std::memcpy(ack + sbe::MessageHeader::encodedLength(), &gatewaySequence,
                sizeof gatewaySequence);
    gateway.onAck(ack, sizeof ack);
    gateway.onEvent(events[index].data(), events[index].size());
    const auto [bytes, length] = gateway.outbound(slot);
    gateway.drained(slot, length);
    gateway.onTick();
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

  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
