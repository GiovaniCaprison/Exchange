// The session carrier's framing: a u16 length prefix and one framed message over a byte stream
// (docs/PROTOCOL.md, the session). The reassembler is the venue's first parser of untrusted
// bytes, so its whole posture is refusal: a frame shorter than a message header or longer than
// the cap poisons the connection rather than the process, partial reads accumulate until a frame
// is whole, and nothing here allocates or trusts. This is the gateway's fuzz surface
// (docs/components/gateway.md).

#pragma once

#include <cstdint>
#include <cstring>

#include "exchange_protocol/MessageHeader.h"

namespace exchange::gateway {

class Reassembler {
 public:
  // No session message approaches this; a frame claiming more is not a client having a bad day,
  // it is not the protocol.
  static constexpr std::size_t CAP = 1 << 12;

  // Feeds raw bytes; hands every whole frame to the handler as (message, length). Returns false
  // when the stream is poisoned and the connection must close.
  template <typename Handler>
  bool feed(const char* bytes, std::size_t length, Handler&& whole) {
    while (length != 0) {
      const std::size_t want = fill_ < PREFIX ? PREFIX - fill_ : PREFIX + frame_ - fill_;
      const std::size_t take = length < want ? length : want;
      std::memcpy(buffer_ + fill_, bytes, take);
      fill_ += take;
      bytes += take;
      length -= take;
      if (fill_ == PREFIX) {
        std::uint16_t announced = 0;
        std::memcpy(&announced, buffer_, sizeof announced);
        frame_ = announced;
        if (frame_ < ::exchange::protocol::MessageHeader::encodedLength() || frame_ > CAP) {
          return false;
        }
      }
      if (fill_ == PREFIX + frame_ && frame_ != 0) {
        whole(buffer_ + PREFIX, frame_);
        fill_ = 0;
        frame_ = 0;
      }
    }
    return true;
  }

  void reset() {
    fill_ = 0;
    frame_ = 0;
  }

 private:
  static constexpr std::size_t PREFIX = sizeof(std::uint16_t);
  char buffer_[PREFIX + CAP] = {};
  std::size_t fill_ = 0;
  std::size_t frame_ = 0;
};

// The outbound half: one frame into a byte sink that offers (data(), size(), append semantics
// through a raw write). Kept free so sessions and tests share it.
template <typename Sink>
void frame(Sink&& sink, const char* message, const std::uint16_t length) {
  sink(reinterpret_cast<const char*>(&length), sizeof length);
  sink(message, length);
}

}  // namespace exchange::gateway
