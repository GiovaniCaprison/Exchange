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

#include "broadcast_ring.hpp"
#include "exchange_protocol/MessageHeader.h"
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
      // The gap comes first: nothing here is deliverable until the rewinder fills it. The same
      // gap is asked for once, not once per twin that reveals it: A and B carry the same
      // packets, so the packet after a shared loss arrives twice, and a polite consumer does
      // not hammer the rewinder for what it already requested.
      if (next_ != askedFrom_ || reader.firstSequence() != askedTo_) {
        rewind_.request(next_, static_cast<std::uint32_t>(reader.firstSequence() - next_));
        askedFrom_ = next_;
        askedTo_ = reader.firstSequence();
      }
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
  std::uint64_t askedFrom_ = 0;
  std::uint64_t askedTo_ = 0;
  std::uint32_t epoch_;
  Rewind& rewind_;
  std::vector<std::vector<char>> parked_;
  bool ended_ = false;
};

// The event stream's twin seat: one or more broadcast rings carrying the same per-partition
// event stream, a warm matcher's beside its primary's, deduplicated by the sequence every event
// carries, gap free from one per partition. Twins are the event stream's A and B: whichever
// ring speaks first is the one that counts, a covered sequence is skipped wherever it arrives
// again, and a twin dying is therefore a non-event, because the other one was always saying the
// same thing. A seat attached late replays what the rings retain and skips what it has covered,
// which is also what re-seating after a failover means.
class SequencedSeat {
 public:
  void join(BroadcastReader reader) { readers_.push_back(std::move(reader)); }

  template <typename Handler>
  std::size_t poll(Handler&& handler) {
    std::size_t delivered = 0;
    for (BroadcastReader& reader : readers_) {
      reader.poll([&](char* message, const std::size_t length) {
        std::uint64_t sequence = 0;
        std::memcpy(&sequence, message + protocol::MessageHeader::encodedLength(), sizeof sequence);
        if (sequence <= seen_) {
          return;
        }
        seen_ = sequence;
        handler(message, length);
        delivered++;
      });
    }
    return delivered;
  }

  std::uint64_t seen() const { return seen_; }
  std::size_t twins() const { return readers_.size(); }

 private:
  std::vector<BroadcastReader> readers_;
  std::uint64_t seen_ = 0;
};

}  // namespace exchange::common::stream
