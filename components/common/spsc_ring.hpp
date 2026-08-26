// A single-producer single-consumer ring over a memory-mapped file, so two processes on one box
// share messages with no copy and no syscall on the hot path. The shape follows the Disruptor's
// claim-then-publish (Thompson et al. 2011): the producer claims space, encodes in place, and
// publishes a whole command's events with one release store, so the consumer sees a command's
// batch or nothing. A full ring is back pressure: the producer waits, because an engine that
// could drop an event is an engine whose stream cannot rebuild a book.
//
// Records are eight byte aligned: a u32 payload length and a u32 kind (message or padding), then
// the payload. Padding records fill the tail of the buffer when a claim would wrap, so a message
// is always contiguous. Positions are monotonic byte counts; the offset in the buffer is the
// position masked by the power-of-two capacity.

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

namespace exchange::common {

class SpscRing {
 public:
  static constexpr std::uint64_t MAGIC = 0x45585249'4E473031ULL;  // "EXRING01"

  struct Header {
    std::uint64_t magic;
    std::uint64_t capacity;
    alignas(64) std::atomic<std::uint64_t> head;  // published bytes, written by the producer
    alignas(64) std::atomic<std::uint64_t> tail;  // consumed bytes, written by the consumer
    alignas(64) char pad[64];
  };

  // The producer creates the file and owns its geometry; capacity is a power of two.
  static SpscRing create(const std::string& path, const std::uint64_t capacity) {
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
    SpscRing ring(file, total);
    ring.header().magic = MAGIC;
    ring.header().capacity = capacity;
    ring.header().head.store(0, std::memory_order_relaxed);
    ring.header().tail.store(0, std::memory_order_relaxed);
    return ring;
  }

  // The consumer attaches to a ring the producer made.
  static SpscRing attach(const std::string& path) {
    const int file = ::open(path.c_str(), O_RDWR, 0644);
    if (file < 0) {
      throw std::runtime_error("cannot open ring " + path);
    }
    struct stat status{};
    if (::fstat(file, &status) != 0) {
      ::close(file);
      throw std::runtime_error("cannot stat ring " + path);
    }
    SpscRing ring(file, static_cast<std::size_t>(status.st_size));
    if (ring.header().magic != MAGIC) {
      throw std::runtime_error(path + " is not a ring");
    }
    return ring;
  }

  SpscRing(SpscRing&& other) noexcept
      : mapped_(other.mapped_),
        total_(other.total_),
        writePosition_(other.writePosition_),
        readPosition_(other.readPosition_) {
    other.mapped_ = nullptr;
  }

  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;
  SpscRing& operator=(SpscRing&&) = delete;

  ~SpscRing() {
    if (mapped_ != nullptr) {
      ::munmap(mapped_, total_);
    }
  }

  // The producer surface, the Ring the feed encodes into --------------------------------------

  // Space for one message: the offset into buffer() to encode at. Waits when the ring is full,
  // which is back pressure rather than an error.
  std::size_t claim(const std::size_t length) {
    const std::uint64_t capacity = header().capacity;
    const std::uint64_t recordBytes = aligned(RECORD_HEADER + length);
    const std::uint64_t untilWrap = capacity - (writePosition_ & (capacity - 1));
    if (recordBytes > untilWrap) {
      // A message never wraps: a padding record fills the tail and the claim starts fresh.
      awaitSpace(untilWrap);
      writeRecordHeader(writePosition_, static_cast<std::uint32_t>(untilWrap - RECORD_HEADER),
                        PADDING);
      writePosition_ += untilWrap;
    }
    awaitSpace(recordBytes);
    writeRecordHeader(writePosition_, static_cast<std::uint32_t>(length), MESSAGE);
    const std::size_t at = (writePosition_ & (capacity - 1)) + RECORD_HEADER;
    writePosition_ += recordBytes;
    return at;
  }

  char* buffer() { return data(); }

  // The bytes were encoded in place; there is nothing left to do per event.
  void commit() {}

  // One release makes the whole command's events visible at once.
  void publish() { header().head.store(writePosition_, std::memory_order_release); }

  // The consumer surface ----------------------------------------------------------------------

  // Every published message not yet seen, handed over as (bytes, length); returns how many.
  template <typename Handler>
  std::size_t poll(Handler&& handler) {
    const std::uint64_t capacity = header().capacity;
    const std::uint64_t head = header().head.load(std::memory_order_acquire);
    std::size_t handled = 0;
    while (readPosition_ < head) {
      const std::size_t at = readPosition_ & (capacity - 1);
      std::uint32_t length = 0;
      std::uint32_t kind = 0;
      std::memcpy(&length, data() + at, sizeof length);
      std::memcpy(&kind, data() + at + sizeof length, sizeof kind);
      if (kind == MESSAGE) {
        handler(data() + at + RECORD_HEADER, static_cast<std::size_t>(length));
        handled++;
      }
      readPosition_ += aligned(RECORD_HEADER + length);
    }
    header().tail.store(readPosition_, std::memory_order_release);
    return handled;
  }

  // At most one message, stepping over padding inside the same call, so an arbiter visiting many
  // rings interleaves them fairly rather than draining whichever it looked at first.
  template <typename Handler>
  std::size_t pollOne(Handler&& handler) {
    const std::uint64_t capacity = header().capacity;
    const std::uint64_t head = header().head.load(std::memory_order_acquire);
    std::size_t handled = 0;
    while (readPosition_ < head && handled == 0) {
      const std::size_t at = readPosition_ & (capacity - 1);
      std::uint32_t length = 0;
      std::uint32_t kind = 0;
      std::memcpy(&length, data() + at, sizeof length);
      std::memcpy(&kind, data() + at + sizeof length, sizeof kind);
      if (kind == MESSAGE) {
        handler(data() + at + RECORD_HEADER, static_cast<std::size_t>(length));
        handled++;
      }
      readPosition_ += aligned(RECORD_HEADER + length);
    }
    header().tail.store(readPosition_, std::memory_order_release);
    return handled;
  }

 private:
  static constexpr std::uint32_t MESSAGE = 0;
  static constexpr std::uint32_t PADDING = 1;
  static constexpr std::uint64_t RECORD_HEADER = 8;

  SpscRing(const int file, const std::size_t total) : total_(total) {
    mapped_ = ::mmap(nullptr, total, PROT_READ | PROT_WRITE, MAP_SHARED, file, 0);
    ::close(file);
    if (mapped_ == MAP_FAILED) {
      mapped_ = nullptr;
      throw std::runtime_error("cannot map ring");
    }
#if defined(__linux__)
    // Rings live on tmpfs in deployment, where the kernel honours transparent huge pages for
    // shared mappings, and fewer TLB entries per ring is a standing win. Advice is all this is:
    // where the filesystem cannot oblige, the kernel says no and the mapping stands.
    ::madvise(mapped_, total, MADV_HUGEPAGE);
#endif
  }

  static std::uint64_t aligned(const std::uint64_t length) { return (length + 7) & ~7ULL; }

  Header& header() { return *static_cast<Header*>(mapped_); }
  const Header& header() const { return *static_cast<const Header*>(mapped_); }
  char* data() { return static_cast<char*>(mapped_) + sizeof(Header); }

  void awaitSpace(const std::uint64_t needed) {
    while (writePosition_ + needed - header().tail.load(std::memory_order_acquire) >
           header().capacity) {
      // Back pressure: the consumer is behind, and waiting here is the honest response.
    }
  }

  void writeRecordHeader(const std::uint64_t position, const std::uint32_t length,
                         const std::uint32_t kind) {
    const std::size_t at = position & (header().capacity - 1);
    std::memcpy(data() + at, &length, sizeof length);
    std::memcpy(data() + at + sizeof length, &kind, sizeof kind);
  }

  void* mapped_ = nullptr;
  std::size_t total_ = 0;
  std::uint64_t writePosition_ = 0;
  std::uint64_t readPosition_ = 0;
};

}  // namespace exchange::common
