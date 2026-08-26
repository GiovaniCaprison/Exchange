// The rewinder: a consumer that missed packets names a range and receives ordinary ranges built
// from the journal (docs/PROTOCOL.md, publication and replication ranges). Rereading the journal
// per request is deliberate: rewind is the cold path, and serving it from the durable record
// keeps the sequencing thread unencumbered, which is the split MoldUDP64's re-request server
// makes (Nasdaq, public specification).

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <utility>

#include "exchange_protocol/MessageHeader.h"
#include "journal.hpp"
#include "ranges.hpp"

namespace exchange::sequencer {

class Rewinder {
 public:
  explicit Rewinder(std::string journalPath, const std::uint32_t epoch = 1)
      : path_(std::move(journalPath)), epoch_(epoch) {}

  // Serve [firstSequence, firstSequence + count) through send(bytes, length), chunked the same
  // as live publication so a repaired stream looks like the one that was missed.
  template <typename Sink>
  void serve(const std::uint64_t firstSequence, const std::uint32_t count, Sink&& send) {
    common::journal::Read log = common::journal::read(path_);
    char packet[common::ranges::PACKET_BYTES];
    common::ranges::Builder builder(packet, sizeof packet);
    for (std::size_t at = 0; at < log.count(); at++) {
      char* message = log.messages.data() + log.offsets[at];
      const std::uint64_t sequence = sequenceOf(message);
      if (sequence < firstSequence || sequence >= firstSequence + count) {
        continue;
      }
      if (builder.isOpen() && !builder.fits(log.lengths[at])) {
        send(packet, builder.close());
      }
      if (!builder.isOpen()) {
        builder.open(sequence, epoch_);
      }
      builder.add(message, static_cast<std::uint16_t>(log.lengths[at]));
    }
    if (builder.isOpen()) {
      send(packet, builder.close());
    }
  }

 private:
  static std::uint64_t sequenceOf(const char* message) {
    // Every command opens with the same context, so the sequence sits at the body's start.
    std::uint64_t sequence = 0;
    std::memcpy(&sequence, message + ::exchange::protocol::MessageHeader::encodedLength(),
                sizeof sequence);
    return sequence;
  }

  std::string path_;
  std::uint32_t epoch_;
};

}  // namespace exchange::sequencer
