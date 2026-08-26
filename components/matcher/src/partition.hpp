// One matcher partition: every instrument routed here, matched on one thread, behind one inbound
// and one outbound ring. The partition owns what its instruments share, the slab, the feed and
// the id, execution and arrival counters, so the whole partition is one sequence of decisions and
// its event stream numbers gap free from 1.
//
// The partition trusts its input (P-9): commands arrive sequenced, stamped and well framed, and
// every instrument was defined before anything else addressed it. Instruments are found by a
// linear scan of a small array, because a partition serves few instruments and the common case is
// the first compare; a partition serving hundreds would earn an index by measurement first
// (P-14).

#pragma once

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "engine.hpp"
#include "exchange_protocol/CancelOrder.h"
#include "exchange_protocol/InstrumentDefinition.h"
#include "exchange_protocol/MassCancel.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/ReplaceOrder.h"
#include "exchange_protocol/SessionControl.h"
#include "feed.hpp"
#include "slab.hpp"

namespace exchange::matcher {

template <typename Ring>
class Partition {
 public:
  explicit Partition(Ring& ring) : feed_(ring), slab_(1 << 16) {}

  void onCommand(char* buffer, const std::size_t offset, const std::size_t length) {
    sbe::MessageHeader header;
    const std::size_t end = offset + length;
    header.wrap(buffer, offset, 0, end);
    const std::size_t body = offset + sbe::MessageHeader::encodedLength();
    switch (header.templateId()) {
      case sbe::NewOrder::sbeTemplateId(): {
        auto command = decoded<sbe::NewOrder>(buffer, body, header, end);
        answering(command.context());
        engineOf(command.context().instrumentId()).enter(command);
        break;
      }
      case sbe::CancelOrder::sbeTemplateId(): {
        auto command = decoded<sbe::CancelOrder>(buffer, body, header, end);
        answering(command.context());
        engineOf(command.context().instrumentId())
            .cancel(command.clientOrderId(), command.participantId());
        break;
      }
      case sbe::ReplaceOrder::sbeTemplateId(): {
        auto command = decoded<sbe::ReplaceOrder>(buffer, body, header, end);
        answering(command.context());
        engineOf(command.context().instrumentId())
            .replace(command.clientOrderId(), command.participantId(), command.quantity(),
                     command.price());
        break;
      }
      case sbe::MassCancel::sbeTemplateId(): {
        auto command = decoded<sbe::MassCancel>(buffer, body, header, end);
        answering(command.context());
        engineOf(command.context().instrumentId())
            .massCancel(command.clientOrderId(), command.participantId());
        break;
      }
      case sbe::SessionControl::sbeTemplateId(): {
        auto command = decoded<sbe::SessionControl>(buffer, body, header, end);
        answering(command.context());
        engineOf(command.context().instrumentId())
            .changeState(static_cast<std::int32_t>(command.state()));
        break;
      }
      case sbe::InstrumentDefinition::sbeTemplateId(): {
        auto command = decoded<sbe::InstrumentDefinition>(buffer, body, header, end);
        answering(command.context());
        define(command);
        break;
      }
      default:
        throw std::invalid_argument("template " + std::to_string(header.templateId()) +
                                    " is not a command (P-9)");
    }
    feed_.finish();
  }

  // Views for the tests --------------------------------------------------------------------

  const Engine<Ring>& engine(const std::uint32_t instrumentId) const {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        return *engines_[at];
      }
    }
    throw std::invalid_argument("instrument " + std::to_string(instrumentId) + " is not defined");
  }

  const Slab& slab() const { return slab_; }

 private:
  template <typename Decoder>
  static Decoder decoded(char* buffer, const std::size_t body, const sbe::MessageHeader& header,
                         const std::size_t end) {
    Decoder decoder;
    decoder.wrapForDecode(buffer, body, header.blockLength(), header.version(), end);
    return decoder;
  }

  template <typename Context>
  void answering(Context&& context) {
    feed_.answering(context.sequence(), context.timestamp(), context.instrumentId());
  }

  // The definition arrives once per instrument, before anything else addresses it, and is what
  // sizes the ladder: this is the one allocation the partition's life holds after construction
  // (P-6).
  void define(sbe::InstrumentDefinition& command) {
    instruments_.push_back(command.context().instrumentId());
    engines_.push_back(std::make_unique<Engine<Ring>>(
        slab_, feed_, counters_, command.tickSize(), command.lotSize(), command.minPrice(),
        command.maxPrice(), command.bandWidth(), command.openingReference(),
        command.allocation() == sbe::Allocation::PRO_RATA));
  }

  Engine<Ring>& engineOf(const std::uint32_t instrumentId) {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        return *engines_[at];
      }
    }
    throw std::invalid_argument("instrument " + std::to_string(instrumentId) + " is not defined");
  }

  Feed<Ring> feed_;
  Slab slab_;
  Counters counters_;
  std::vector<std::uint32_t> instruments_;
  std::vector<std::unique_ptr<Engine<Ring>>> engines_;
};

}  // namespace exchange::matcher
