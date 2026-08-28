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
#include <cstring>
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
  // One instrument's construction facts, kept so a snapshot can rebuild the engine it configures.
  struct Definition {
    std::uint32_t instrumentId = 0;
    std::int64_t tickSize = 1;
    std::int64_t lotSize = 1;
    std::int64_t minPrice = 0;
    std::int64_t maxPrice = 0;
    std::int64_t bandWidth = 0;
    std::int64_t openingReference = 0;
    bool proRata = false;
  };

 public:
  explicit Partition(Ring& ring) : feed_(ring), slab_(1 << 16) {}

  // Deployment: which instruments this partition serves, empty meaning all of them, and its
  // shard number, which namespaces every id it will ever assign in the top byte so no two
  // shards mint the same one and every consumer can merge their event streams without a thought.
  void serve(std::vector<std::uint32_t> instruments) { served_ = std::move(instruments); }

  void shard(const std::uint32_t number) {
    counters_.nextOrderId = (std::uint64_t{number} << 56) + 1;
    counters_.nextExecutionId = (std::uint64_t{number} << 56) + 1;
  }

  bool serves(const std::uint32_t instrumentId) const {
    if (served_.empty()) {
      return true;
    }
    for (const std::uint32_t mine : served_) {
      if (mine == instrumentId) {
        return true;
      }
    }
    return false;
  }

  void onCommand(char* buffer, const std::size_t offset, const std::size_t length) {
    sbe::MessageHeader header;
    const std::size_t end = offset + length;
    header.wrap(buffer, offset, 0, end);
    const std::size_t body = offset + sbe::MessageHeader::encodedLength();
    // Another shard's instrument is another shard's business; instrument zero is everyone's,
    // because the venue-wide sweep addresses every book wherever it lives.
    std::uint32_t addressed = 0;
    std::memcpy(&addressed, buffer + body + 16, sizeof addressed);
    if (addressed != 0 && !serves(addressed)) {
      return;
    }
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
        if (command.context().instrumentId() == 0) {
          // Instrument zero addresses every book: the venue-wide sweep a kill switch or a
          // cancel on disconnect means. Each engine answers under its own instrument so the
          // removals it emits carry the book they empty.
          lastSequence_ = command.context().sequence();
          for (std::size_t at = 0; at < engines_.size(); at++) {
            feed_.answering(command.context().sequence(), command.context().timestamp(),
                            instruments_[at]);
            engines_[at]->massCancel(command.clientOrderId(), command.participantId());
          }
          break;
        }
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
        refuseTemplate(header.templateId());
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

  // The last command this partition applied, which is what a snapshot is up to.
  std::uint64_t upToSequence() const { return lastSequence_; }

  // The partition's whole state as one blob, snapshots' stateVersion 1: the shared counters, the
  // feed's stream position, the slab verbatim, then per instrument its definition and its
  // engine's state. A restore rebuilds the engines from their definitions and overwrites their
  // state, so a restored partition is bit-equal to the saved one and the suffix it produces is
  // byte identical (P-2).
  void save(common::ByteSink& sink) const {
    sink.u64(lastSequence_);
    sink.u64(counters_.nextOrderId);
    sink.u64(counters_.nextExecutionId);
    sink.i64(counters_.arrival);
    feed_.save(sink);
    slab_.save(sink);
    sink.u32(static_cast<std::uint32_t>(definitions_.size()));
    for (std::size_t at = 0; at < definitions_.size(); at++) {
      const Definition& definition = definitions_[at];
      sink.u32(definition.instrumentId);
      sink.i64(definition.tickSize);
      sink.i64(definition.lotSize);
      sink.i64(definition.minPrice);
      sink.i64(definition.maxPrice);
      sink.i64(definition.bandWidth);
      sink.i64(definition.openingReference);
      sink.u32(definition.proRata ? 1 : 0);
      engines_[at]->save(sink);
    }
  }

  void restore(common::ByteSource& source) {
    lastSequence_ = source.u64();
    counters_.nextOrderId = source.u64();
    counters_.nextExecutionId = source.u64();
    counters_.arrival = source.i64();
    feed_.restore(source);
    slab_.restore(source);
    const std::uint32_t count = source.u32();
    for (std::uint32_t at = 0; at < count; at++) {
      Definition definition;
      definition.instrumentId = source.u32();
      definition.tickSize = source.i64();
      definition.lotSize = source.i64();
      definition.minPrice = source.i64();
      definition.maxPrice = source.i64();
      definition.bandWidth = source.i64();
      definition.openingReference = source.i64();
      definition.proRata = source.u32() != 0;
      define(definition);
      engines_.back()->restore(source);
    }
  }

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
    lastSequence_ = context.sequence();
    feed_.answering(context.sequence(), context.timestamp(), context.instrumentId());
  }

  // The definition arrives once per instrument, before anything else addresses it, and is what
  // sizes the ladder: this is the one allocation the partition's life holds after construction
  // (P-6).
  void define(sbe::InstrumentDefinition& command) {
    const Definition definition{command.context().instrumentId(),
                                command.tickSize(),
                                command.lotSize(),
                                command.minPrice(),
                                command.maxPrice(),
                                command.bandWidth(),
                                command.openingReference(),
                                command.allocation() == sbe::Allocation::PRO_RATA};
    define(definition);
  }

  void define(const Definition& definition) {
    definitions_.push_back(definition);
    instruments_.push_back(definition.instrumentId);
    engines_.push_back(std::make_unique<Engine<Ring>>(
        slab_, feed_, counters_, definition.tickSize, definition.lotSize, definition.minPrice,
        definition.maxPrice, definition.bandWidth, definition.openingReference,
        definition.proRata));
  }

  std::vector<std::uint32_t> served_;

  Engine<Ring>& engineOf(const std::uint32_t instrumentId) {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        return *engines_[at];
      }
    }
    refuseInstrument(instrumentId);
  }

  // The refusals live out of line on purpose: building a message is string machinery the
  // instruction cache should never hold inside the hot function, and the codegen ritual is what
  // caught it living there.
  [[noreturn]] [[gnu::noinline]] static void refuseTemplate(const std::uint16_t templateId) {
    throw std::invalid_argument("template " + std::to_string(templateId) +
                                " is not a command (P-9)");
  }

  [[noreturn]] [[gnu::noinline]] static void refuseInstrument(const std::uint32_t instrumentId) {
    throw std::invalid_argument("instrument " + std::to_string(instrumentId) + " is not defined");
  }

  Feed<Ring> feed_;
  Slab slab_;
  Counters counters_;
  std::uint64_t lastSequence_ = 0;
  std::vector<std::uint32_t> instruments_;
  std::vector<Definition> definitions_;
  std::vector<std::unique_ptr<Engine<Ring>>> engines_;
};

}  // namespace exchange::matcher
