// The allocation proof for the feed (P-6): events prebuilt from a real matcher run pumped
// through the builder and publisher with the allocator counting after warm-up, conflation ticks
// included, so the steady state that publishes the market pays the allocator nothing. Refusal is
// counting rather than crashing so the probe can say how many times it was asked.
//
//   marketdata-allocation-probe [--misbehave]

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

#include "feedtest.hpp"

namespace {

bool locked = false;
std::uint64_t denied = 0;

struct DiscardingSink {
  void send(const char*, std::size_t) {}
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
  namespace market = exchange::marketdata;
  namespace test = exchange::marketdata::test;

  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }

  // The events come from a real run, gathered before the allocator locks.
  const std::vector<test::CommandWriter::Framed> flow =
      exchange::matcher::test::generatedFlow(83, 20'000);
  test::CapturingRing events;
  exchange::matcher::Partition<test::CapturingRing> partition(events);
  for (const test::CommandWriter::Framed& framed : flow) {
    std::vector<char> bytes = framed.bytes;
    partition.onCommand(bytes.data(), 0, bytes.size());
  }
  std::vector<std::pair<std::size_t, std::size_t>> messages;
  {
    std::vector<char> all = events.captured();
    std::size_t at = 0;
    while (at < all.size()) {
      exchange::protocol::MessageHeader wrap;
      wrap.wrap(all.data(), at, 0, all.size());
      const std::size_t length =
          exchange::protocol::MessageHeader::encodedLength() + wrap.blockLength();
      messages.emplace_back(at, length);
      at += length;
    }
  }
  std::vector<char> bytes = events.captured();

  DiscardingSink a;
  DiscardingSink b;
  market::Publisher<DiscardingSink, DiscardingSink> publisher(a, b);
  market::Builder<market::Publisher<DiscardingSink, DiscardingSink>> builder(publisher);

  const std::size_t warm = 512;
  std::size_t at = 0;
  for (; at < warm && at < messages.size(); at++) {
    builder.onEvent(bytes.data() + messages[at].first, messages[at].second);
  }
  builder.onConflation();
  publisher.flush();

  locked = true;
  for (; at < messages.size(); at++) {
    builder.onEvent(bytes.data() + messages[at].first, messages[at].second);
    if (at % 512 == 0) {
      builder.onConflation();
      publisher.flush();
    }
    if (misbehave) {
      // A direct call to the allocation function, which the compiler cannot elide the way it
      // may elide a new-expression whose result goes nowhere.
      ::operator delete(::operator new(64));
    }
  }
  publisher.flush();
  locked = false;

  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
