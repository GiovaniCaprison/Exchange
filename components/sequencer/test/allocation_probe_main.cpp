// The allocation proof for the sequencer (P-6): the global allocator counts every request after
// warm-up, and one request is a failure. Warm-up is the flow's construction, the carriers, and
// the first few sequenced commands, which is where the journal's stdio buffering and the packet
// path settle; every submission after that runs against the counting allocator. Refusal is
// counting rather than crashing so the probe can say how many times it was asked.
//
//   sequencer-allocation-probe --journal J [--misbehave]
//
// --misbehave allocates once per submission inside the locked region, which is how the probe's
// own test proves the counting works: a probe that cannot fail proves nothing.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <new>
#include <string>
#include <vector>

#include "clock.hpp"
#include "flow.hpp"
#include "journal.hpp"
#include "sequencer.hpp"
#include "submission.hpp"

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

struct DiscardingPacketSink {
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
  namespace common = exchange::common;
  namespace sequencing = exchange::sequencer;
  namespace test = exchange::sequencer::test;

  std::string journalPath;
  bool misbehave = false;
  for (int at = 1; at < count; at++) {
    if (std::string("--journal") == values[at] && at + 1 < count) {
      journalPath = values[at + 1];
    }
    if (std::string("--misbehave") == values[at]) {
      misbehave = true;
    }
  }
  if (journalPath.empty()) {
    std::fprintf(stderr, "usage: sequencer-allocation-probe --journal J [--misbehave]\n");
    return 2;
  }

  const std::vector<exchange::matcher::test::CommandWriter::Framed> flow =
      exchange::matcher::test::generatedFlow(42, 20'000);
  std::vector<std::vector<char>> records = test::dealtSubmissions(flow, 3);

  DiscardingRing out;
  std::vector<DiscardingRing> acks(3);
  DiscardingPacketSink packets;
  common::journal::Writer journal(journalPath);
  sequencing::ScriptedClock clock;
  sequencing::Sequencer<DiscardingRing, DiscardingRing, DiscardingPacketSink,
                        common::journal::Writer, sequencing::ScriptedClock>
      sequencer(out, acks, packets, journal, clock);

  // Warm-up sequences a few hundred commands unlocked, spanning at least one packet flush.
  const std::size_t warm = 256;
  std::size_t at = 0;
  for (; at < warm && at < records.size(); at++) {
    sequencer.onSubmission(records[at].data(), records[at].size());
  }

  locked = true;
  for (; at < records.size(); at++) {
    sequencer.onSubmission(records[at].data(), records[at].size());
    if (misbehave) {
      // A direct call to the allocation function, which the compiler cannot elide the way it
      // may elide a new-expression whose result goes nowhere.
      ::operator delete(::operator new(64));
    }
  }
  sequencer.flush();
  locked = false;

  if (denied != 0) {
    std::fprintf(stderr, "the steady state asked the allocator %llu times\n",
                 static_cast<unsigned long long>(denied));
    return 1;
  }
  return 0;
}
