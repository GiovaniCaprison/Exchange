// The consumer library: every downstream process reads the sequenced stream through one of these
// sources, behind one contract: framed messages, in sequence order, gap free. The ring and the
// journal are gap free by construction; the packet feed is not, so its source detects gaps by
// sequence, asks the rewinder for what is missing, and delivers nothing out of order. Built once
// because every real feed handler is this library (docs/components/sequencer.md).

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "journal.hpp"
#include "ranges.hpp"
#include "spsc_ring.hpp"

namespace exchange::common::stream {

// The lossless on-box source: whatever the ring holds, in the order it was published.
class RingSource {
 public:
  explicit RingSource(SpscRing& ring) : ring_(ring) {}

  template <typename Handler>
  std::size_t poll(Handler&& handler) {
    return ring_.poll(handler);
  }

 private:
  SpscRing& ring_;
};

// The recovery source: a journal read forward from its start, delivered in one sweep.
class JournalSource {
 public:
  explicit JournalSource(const std::string& path) : log_(journal::read(path)) {}

  template <typename Handler>
  std::size_t poll(Handler&& handler) {
    std::size_t handled = 0;
    while (cursor_ < log_.count()) {
      handler(log_.messages.data() + log_.offsets[cursor_], log_.lengths[cursor_]);
      cursor_++;
      handled++;
    }
    return handled;
  }

 private:
  journal::Read log_;
  std::size_t cursor_ = 0;
};

// The packet source: ranges in, a contiguous stream out. A gap is repaired before anything past
// it is delivered: the packet that revealed it is parked, the rewinder is asked for the missing
// sequences, and the rewound ranges arrive through the same door. The rewind channel only
// records the request, as a transport would, so nothing here reenters itself; repair is the cold
// path and buys its simplicity with a copy. Ranges from a stale epoch are dropped outright, and
// a higher epoch is adopted, which is the consumer's half of a leadership change.
template <typename Rewind>
class PacketSource {
 public:
  PacketSource(const std::uint64_t firstExpected, const std::uint32_t epoch, Rewind& rewind)
      : next_(firstExpected), epoch_(epoch), rewind_(rewind) {}

  template <typename Handler>
  void onPacket(char* bytes, const std::size_t length, Handler&& handler) {
    consume(bytes, length, handler, true);
    // A parked packet may extend the stream now; keep going while any does.
    bool progressed = true;
    while (progressed) {
      progressed = false;
      for (std::size_t at = 0; at < parked_.size(); at++) {
        ranges::Reader reader(parked_[at].data(), parked_[at].size());
        if (reader.firstSequence() <= next_) {
          std::vector<char> packet = std::move(parked_[at]);
          parked_.erase(parked_.begin() + static_cast<long>(at));
          consume(packet.data(), packet.size(), handler, false);
          progressed = true;
          break;
        }
      }
    }
  }

  std::uint64_t nextSequence() const { return next_; }
  std::uint32_t epoch() const { return epoch_; }
  bool ended() const { return ended_; }

 private:
  template <typename Handler>
  void consume(char* bytes, const std::size_t length, Handler&& handler, const bool mayPark) {
    ranges::Reader reader(bytes, length);
    if (reader.epoch() < epoch_) {
      return;
    }
    epoch_ = reader.epoch();
    if (reader.firstSequence() > next_) {
      // The gap comes first: nothing here is deliverable until the rewinder fills it.
      rewind_.request(next_, static_cast<std::uint32_t>(reader.firstSequence() - next_));
      if (mayPark && !reader.heartbeat()) {
        parked_.emplace_back(bytes, bytes + length);
      }
      return;
    }
    if (reader.endOfSession()) {
      ended_ = true;
      return;
    }
    if (reader.heartbeat() || reader.firstSequence() + reader.count() <= next_) {
      return;
    }
    const std::uint16_t skip = static_cast<std::uint16_t>(next_ - reader.firstSequence());
    reader.forEach(handler, skip);
    next_ = reader.firstSequence() + reader.count();
  }

  std::uint64_t next_;
  std::uint32_t epoch_;
  Rewind& rewind_;
  std::vector<std::vector<char>> parked_;
  bool ended_ = false;
};

}  // namespace exchange::common::stream
