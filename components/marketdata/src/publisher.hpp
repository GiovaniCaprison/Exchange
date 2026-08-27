// The feed's carrier side: every public message is encoded in place into the retained log,
// framed, numbered implicitly by its position, batched into ranges, and sent to two sinks, the A
// feed and the B feed, identical on purpose so a consumer takes whichever packet arrives first
// and single-packet loss costs nothing (docs/PROTOCOL.md, the public feed). The log is the
// retransmission server's whole inventory and the snapshot's join point in one structure: rewind
// serves ranges from it, and the next sequence is its count plus one.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "ranges.hpp"

namespace exchange::marketdata {

template <typename SinkA, typename SinkB>
class Publisher {
 public:
  Publisher(SinkA& a, SinkB& b) : a_(a), b_(b), builder_(packet_, sizeof packet_) {
    log_.reserve(LOG_BYTES);
    offsets_.reserve(LOG_ENTRIES);
    lengths_.reserve(LOG_ENTRIES);
  }

  // Encode the next public message here; commit with the encoded size.
  char* claim(const std::size_t length) {
    if (log_.size() + PREFIX + length > LOG_BYTES || offsets_.size() == LOG_ENTRIES) {
      throw std::runtime_error("the feed log overflowed; size it for the session");
    }
    log_.resize(log_.size() + PREFIX + length);
    return log_.data() + log_.size() - length;
  }

  void commit(const std::size_t length) {
    const std::size_t start = log_.size() - PREFIX - length;
    const std::uint16_t prefix = static_cast<std::uint16_t>(length);
    std::memcpy(log_.data() + start, &prefix, sizeof prefix);
    offsets_.push_back(start);
    lengths_.push_back(length);
    char* message = log_.data() + start + PREFIX;
    if (builder_.isOpen() && !builder_.fits(length)) {
      flush();
    }
    if (!builder_.isOpen()) {
      builder_.open(offsets_.size(), EPOCH);
    }
    builder_.add(message, static_cast<std::uint16_t>(length));
  }

  // The pending range leaves as one packet on both feeds.
  void flush() {
    if (!builder_.isOpen()) {
      return;
    }
    const std::size_t built = builder_.close();
    a_.send(packet_, built);
    b_.send(packet_, built);
  }

  void heartbeat() {
    flush();
    builder_.open(next(), EPOCH);
    const std::size_t built = builder_.close();
    a_.send(packet_, built);
    b_.send(packet_, built);
  }

  void endSession() {
    flush();
    builder_.open(next(), EPOCH);
    const std::size_t built = builder_.closeEndOfSession();
    a_.send(packet_, built);
    b_.send(packet_, built);
  }

  // The retransmission server: [firstSequence, firstSequence + count) as ranges, chunked like
  // live publication, straight from the retained log.
  template <typename Sink>
  void serveRewind(const std::uint64_t firstSequence, const std::uint32_t count, Sink&& send) {
    char scratch[common::ranges::PACKET_BYTES];
    common::ranges::Builder rebuilt(scratch, sizeof scratch);
    for (std::uint64_t sequence = firstSequence;
         sequence < firstSequence + count && sequence <= offsets_.size(); sequence++) {
      const std::size_t at = offsets_[sequence - 1];
      const std::size_t length = lengths_[sequence - 1];
      if (rebuilt.isOpen() && !rebuilt.fits(length)) {
        send(scratch, rebuilt.close());
      }
      if (!rebuilt.isOpen()) {
        rebuilt.open(sequence, EPOCH);
      }
      rebuilt.add(log_.data() + at + PREFIX, static_cast<std::uint16_t>(length));
    }
    if (rebuilt.isOpen()) {
      send(scratch, rebuilt.close());
    }
  }

  // The sequence the next message will take, which is also where a snapshot tells a late joiner
  // to pick up the live feed.
  std::uint64_t next() const { return offsets_.size() + 1; }

 private:
  static constexpr std::size_t PREFIX = sizeof(std::uint16_t);
  static constexpr std::size_t LOG_BYTES = std::size_t{1} << 24;
  static constexpr std::size_t LOG_ENTRIES = std::size_t{1} << 19;
  static constexpr std::uint32_t EPOCH = 1;

  SinkA& a_;
  SinkB& b_;
  std::vector<char> log_;
  std::vector<std::size_t> offsets_;
  std::vector<std::size_t> lengths_;
  char packet_[common::ranges::PACKET_BYTES] = {};
  common::ranges::Builder builder_;
};

}  // namespace exchange::marketdata
