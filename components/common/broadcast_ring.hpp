// A single-producer broadcast ring over a mapped file: one writer, any number of independent
// readers, and no back pressure, because the event stream fans out to the gateway, the market
// data publisher and the operations scheduler at once, and a venue's matcher must never wait on
// its slowest listener. The shape follows Aeron's broadcast buffer (Real Logic, open source
// messaging): the producer claims, encodes in place, and publishes; each reader keeps its own
// position, copies a record out, and then proves the copy was not overwritten mid-read by
// checking the producer's claim marker. A reader that falls a whole buffer behind is lapped: it
// counts the lap, rejoins at the head, and recovers what it missed from the journal, which is
// the recovery posture everywhere else too.
//
// Records are the ring's usual shape: a u32 payload length and a u32 kind (message or padding),
// eight byte aligned, padding filling the tail so a message never wraps. Positions are monotonic
// byte counts. The producer advances the claim marker before it touches the bytes and publishes
// the head after, so a reader's validation brackets its copy: a record below the head was
// written whole, and a claim still within one capacity of the reader means nothing the reader
// copied has been reused since.

#pragma once

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

namespace exchange::common {

class BroadcastRing {
 public:
  static constexpr std::uint64_t MAGIC = 0x45584243'53543031ULL;  // "EXBCST01"

  struct Header {
    std::uint64_t magic;
    std::uint64_t capacity;
    alignas(64) std::atomic<std::uint64_t> head;   // published bytes, a record boundary
    alignas(64) std::atomic<std::uint64_t> claim;  // bytes the producer may have touched
    alignas(64) char pad[64];
  };

  static constexpr std::uint32_t MESSAGE = 0;
  static constexpr std::uint32_t PADDING = 1;
  static constexpr std::uint64_t RECORD_HEADER = 8;

  static std::uint64_t aligned(const std::uint64_t length) { return (length + 7) & ~7ULL; }

  // The producer creates the file and owns its geometry; capacity is a power of two.
  static BroadcastRing create(const std::string& path, const std::uint64_t capacity) {
    if ((capacity & (capacity - 1)) != 0) {
      throw std::invalid_argument("ring capacity must be a power of two");
    }
    const int file = ::open(path.c_str(), O_CREAT | O_RDWR | O_TRUNC, 0644);
    if (file < 0) {
      throw std::runtime_error("cannot create ring " + path);
    }
    const std::size_t total = sizeof(Header) + capacity;
    if (::ftruncate(file, static_cast<off_t>(total)) != 0) {
      ::close(file);
      throw std::runtime_error("cannot size ring " + path);
    }
    BroadcastRing ring(file, total);
    ring.header().capacity = capacity;
    ring.header().head.store(0, std::memory_order_relaxed);
    ring.header().claim.store(0, std::memory_order_relaxed);
    // The magic lands last, so an attaching reader never trusts half-built geometry.
    ring.header().magic = MAGIC;
    return ring;
  }

  BroadcastRing(BroadcastRing&& other) noexcept
      : mapped_(other.mapped_), total_(other.total_), writePosition_(other.writePosition_) {
    other.mapped_ = nullptr;
  }

  BroadcastRing(const BroadcastRing&) = delete;
  BroadcastRing& operator=(const BroadcastRing&) = delete;
  BroadcastRing& operator=(BroadcastRing&&) = delete;

  ~BroadcastRing() {
    if (mapped_ != nullptr) {
      ::munmap(mapped_, total_);
    }
  }

  // Space for one message: the offset into buffer() to encode at. Never waits; a reader that
  // cannot keep up is lapped rather than obeyed.
  std::size_t claim(const std::size_t length) {
    const std::uint64_t capacity = header().capacity;
    const std::uint64_t recordBytes = aligned(RECORD_HEADER + length);
    if (recordBytes > capacity) {
      throw std::invalid_argument("message larger than the ring");
    }
    const std::uint64_t untilWrap = capacity - (writePosition_ & (capacity - 1));
    const bool pads = recordBytes > untilWrap;
    const std::uint64_t advance = recordBytes + (pads ? untilWrap : 0);
    // The claim moves before any byte does, so a reader that validates after copying can trust
    // a clean check. On x86 total store order already keeps these stores in program order and
    // the compiler barrier is the whole cost; elsewhere the fence pays for the same promise.
    header().claim.store(writePosition_ + advance, std::memory_order_relaxed);
#if defined(__x86_64__) || defined(_M_X64)
    std::atomic_signal_fence(std::memory_order_seq_cst);
#else
    std::atomic_thread_fence(std::memory_order_seq_cst);
#endif
    if (pads) {
      writeRecordHeader(writePosition_, static_cast<std::uint32_t>(untilWrap - RECORD_HEADER),
                        PADDING);
      writePosition_ += untilWrap;
    }
    writeRecordHeader(writePosition_, static_cast<std::uint32_t>(length), MESSAGE);
    const std::size_t at = (writePosition_ & (capacity - 1)) + RECORD_HEADER;
    writePosition_ += recordBytes;
    return at;
  }

  char* buffer() { return data(); }

  // The bytes were encoded in place; there is nothing left to do per event.
  void commit() {}

  // One release makes the whole command's events visible to every reader at once.
  void publish() { header().head.store(writePosition_, std::memory_order_release); }

 private:
  BroadcastRing(const int file, const std::size_t total) : total_(total) {
    mapped_ = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);
    ::close(file);
    if (mapped_ == MAP_FAILED) {
      mapped_ = nullptr;
      throw std::runtime_error("cannot map ring");
    }
#if defined(__linux__)
    // Rings live on tmpfs in deployment, where the kernel honours transparent huge pages for
    // shared mappings; where the filesystem cannot oblige, the kernel says no and the mapping
    // stands.
    ::madvise(mapped_, total, MADV_HUGEPAGE);
#endif
  }

  Header& header() { return *static_cast<Header*>(mapped_); }
  char* data() { return static_cast<char*>(mapped_) + sizeof(Header); }

  void writeRecordHeader(const std::uint64_t position, const std::uint32_t length,
                         const std::uint32_t kind) {
    const std::size_t at = position & (header().capacity - 1);
    std::memcpy(data() + at, &length, sizeof length);
    std::memcpy(data() + at + sizeof length, &kind, sizeof kind);
  }

  void* mapped_ = nullptr;
  std::size_t total_ = 0;
  std::uint64_t writePosition_ = 0;
};

// One reader's seat at the broadcast: its own position, its own scratch to copy records into,
// and the lap count that says how often the producer got a whole buffer ahead. A reader attaches
// at position zero, so one that arrives while the retained bytes still cover the start replays
// them for free, and a later joiner laps once and continues from the head.
class BroadcastReader {
 public:
  static BroadcastReader attach(const std::string& path) {
    const int file = ::open(path.c_str(), O_RDONLY, 0644);
    if (file < 0) {
      throw std::runtime_error("cannot open ring " + path);
    }
    struct stat status{};
    if (::fstat(file, &status) != 0) {
      ::close(file);
      throw std::runtime_error("cannot stat ring " + path);
    }
    BroadcastReader reader(file, static_cast<std::size_t>(status.st_size));
    if (reader.header().magic != BroadcastRing::MAGIC) {
      throw std::runtime_error(path + " is not a broadcast ring");
    }
    return reader;
  }

  BroadcastReader(BroadcastReader&& other) noexcept
      : mapped_(other.mapped_),
        total_(other.total_),
        readPosition_(other.readPosition_),
        laps_(other.laps_),
        scratch_(std::move(other.scratch_)) {
    other.mapped_ = nullptr;
  }

  BroadcastReader(const BroadcastReader&) = delete;
  BroadcastReader& operator=(const BroadcastReader&) = delete;
  BroadcastReader& operator=(BroadcastReader&&) = delete;

  ~BroadcastReader() {
    if (mapped_ != nullptr) {
      ::munmap(const_cast<void*>(mapped_), total_);
    }
  }

  // Every published message not yet seen, copied out and handed over as (bytes, length); returns
  // how many. A record is only delivered after the copy is proven untouched: the claim marker
  // still within one capacity of this reader means the producer has not reused these bytes. The
  // copy-then-validate order is the seqlock reader's, with the claim as the sequence.
  template <typename Handler>
  std::size_t poll(Handler&& handler) {
    const std::uint64_t capacity = header().capacity;
    const std::uint64_t head = header().head.load(std::memory_order_acquire);
    std::size_t handled = 0;
    while (readPosition_ < head) {
      if (head - readPosition_ > capacity) {
        resync();
        continue;
      }
      const std::size_t at = readPosition_ & (capacity - 1);
      std::uint32_t length = 0;
      std::uint32_t kind = 0;
      std::memcpy(&length, data() + at, sizeof length);
      std::memcpy(&kind, data() + at + sizeof length, sizeof kind);
      if (overwritten()) {
        resync();
        continue;
      }
      const std::uint64_t recordBytes =
          BroadcastRing::aligned(BroadcastRing::RECORD_HEADER + length);
      if (kind == BroadcastRing::MESSAGE) {
        std::memcpy(scratch_.data(), data() + at + BroadcastRing::RECORD_HEADER, length);
        if (overwritten()) {
          resync();
          continue;
        }
        handler(scratch_.data(), static_cast<std::size_t>(length));
        handled++;
      }
      readPosition_ += recordBytes;
    }
    return handled;
  }

  std::uint64_t laps() const { return laps_; }

 private:
  BroadcastReader(const int file, const std::size_t total) : total_(total) {
    mapped_ = ::mmap(nullptr, total, PROT_READ, MAP_SHARED, file, 0);
    ::close(file);
    if (mapped_ == MAP_FAILED) {
      mapped_ = nullptr;
      throw std::runtime_error("cannot map ring");
    }
    scratch_.resize(header().capacity);
  }

  const BroadcastRing::Header& header() const {
    return *static_cast<const BroadcastRing::Header*>(mapped_);
  }
  const char* data() const {
    return static_cast<const char*>(mapped_) + sizeof(BroadcastRing::Header);
  }

  bool overwritten() const {
    std::atomic_thread_fence(std::memory_order_acquire);
    return header().claim.load(std::memory_order_relaxed) - readPosition_ > header().capacity;
  }

  // Lapped: count it and rejoin at the head, which is always a record boundary. What was missed
  // is a fact the reader reports and the journal repairs.
  void resync() {
    laps_++;
    readPosition_ = header().head.load(std::memory_order_acquire);
  }

  const void* mapped_ = nullptr;
  std::size_t total_ = 0;
  std::uint64_t readPosition_ = 0;
  std::uint64_t laps_ = 0;
  std::vector<char> scratch_;
};

}  // namespace exchange::common
