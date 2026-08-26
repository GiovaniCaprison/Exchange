// The allocation proof (P-6): the global allocator counts every request after initialisation, and
// one request is a failure. Initialisation is the journal read, the partition, and the instrument
// definitions at the head of the stream, since a definition is what sizes a ladder; every command
// after the last leading definition runs against the counting allocator. Refusal is counting
// rather than crashing so the probe can say how many times it was asked. Events go into a fixed
// buffer and are dropped, because the proof is about the engine's memory and capturing output
// would put the harness's allocation on the engine's bill.
//
//   allocation-probe --journal J [--misbehave]
//
// --misbehave allocates once per command inside the locked region, which is how the probe's own
// test proves the counting works: a probe that cannot fail proves nothing.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <string>
#include <vector>

#include "exchange_protocol/InstrumentDefinition.h"
#include "exchange_protocol/MessageHeader.h"
#include "journal.hpp"
#include "partition.hpp"

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
  namespace matching = exchange::matcher;
  namespace common = exchange::common;
  namespace sbe = exchange::protocol;
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
    std::fprintf(stderr, "usage: allocation-probe --journal J [--misbehave]\n");
    return 2;
  }

  common::journal::Read log = common::journal::read(journalPath);
  DiscardingRing ring;
  matching::Partition<DiscardingRing> partition(ring);

  // The leading definitions size the ladders and run while the allocator still says yes.
  std::size_t at = 0;
  while (at < log.count()) {
    sbe::MessageHeader header;
    header.wrap(log.messages.data() + log.offsets[at], 0, 0, log.lengths[at]);
    if (header.templateId() != sbe::InstrumentDefinition::sbeTemplateId()) {
      break;
    }
    partition.onCommand(log.messages.data() + log.offsets[at], 0, log.lengths[at]);
    at++;
  }

  locked = true;
  for (; at < log.count(); at++) {
    partition.onCommand(log.messages.data() + log.offsets[at], 0, log.lengths[at]);
    if (misbehave) {
      // A direct call to the allocation function, which the compiler cannot elide the way it may
      // elide a new-expression whose result goes nowhere.
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
