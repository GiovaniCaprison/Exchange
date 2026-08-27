// Surveillance (docs/components/oversight.md): a consumer of the event stream watching for the
// trading patterns regulation exists to catch, every detection a pure function of the stream so
// a replayed day raises the same alerts at the same sequences. A wash trade is an execution
// whose two orders share a participant. Spoofing is executed quantity on one side answered,
// within a window of stream time, by opposite-side cancellations exceeding a configured multiple
// of it, which is the Coscia pattern (United States v. Coscia, 7th Cir. 2016); when the
// cancelled quantity stood at two or more distinct prices the alert reads layering. Alerts leave
// as SurveillanceAlert messages through the sink, bound for the alert journal, so a case is a
// replayable artifact.
//
// Ownership is learned from acceptances like every routing consumer. Orders that rested leave
// the table when they leave the book; an aggressor that filled whole leaves no removal, so
// unrested entries age out by orderId distance on a periodic sweep, which is deterministic
// because orderIds are the engine's own monotone numbering.

#pragma once

#include <cstdint>
#include <cstring>
#include <vector>

#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/SurveillanceAlert.h"

namespace exchange::oversight {

namespace sbe = ::exchange::protocol;

struct Config {
  // The window of stream time inside which fills and opposite-side cancels are one story.
  std::uint64_t windowNanos = 1'000'000'000ULL;
  // Cancelled quantity at or above this multiple of executed quantity completes the pattern.
  std::int64_t spoofMultiple = 4;
  // And an absolute floor beneath it, so a dealer trimming a small quote never reads as
  // pressure: real detectors carry a size floor for the same reason.
  std::int64_t minimumCancelled = 100;
  // Distinct cancelled price levels at or above this reads as layering rather than spoofing.
  std::uint32_t layeringLevels = 2;
};

template <typename Sink>
class Surveillance {
 public:
  static constexpr std::size_t ORDERS = 1 << 17;
  static constexpr std::size_t HISTORY = 256;
  static constexpr std::uint64_t AGE_HORIZON = 1 << 16;
  static constexpr std::size_t SWEEP_EVERY = 4096;

  Surveillance(Sink& sink, const Config config) : sink_(sink), config_(config) {
    keys_.assign(ORDERS, 0);
    orders_.resize(ORDERS);
    participants_.reserve(64);
    histories_.reserve(64);
  }

  void onEvent(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    switch (wrap.templateId()) {
      case sbe::OrderAccepted::sbeTemplateId(): {
        sbe::OrderAccepted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        Watched& order = slotFor(event.orderId());
        order.orderId = event.orderId();
        order.participant = event.participantId();
        order.instrument = event.context().instrumentId();
        order.remaining = 0;
        order.price = 0;
        order.rested = false;
        if (event.orderId() > newestOrderId_) {
          newestOrderId_ = event.orderId();
        }
        if (++accepted_ % SWEEP_EVERY == 0) {
          sweep();
        }
        break;
      }
      case sbe::OrderRested::sbeTemplateId(): {
        sbe::OrderRested event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = find(event.orderId());
        if (at >= 0) {
          Watched& order = orders_[static_cast<std::size_t>(at)];
          order.rested = true;
          order.side = static_cast<std::uint8_t>(event.side() == sbe::Side::BUY ? 0 : 1);
          order.price = event.price();
          order.remaining = event.quantity();
        }
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        sbe::OrderReduced event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        const long at = find(event.orderId());
        if (at >= 0) {
          orders_[static_cast<std::size_t>(at)].remaining = event.quantity();
        }
        break;
      }
      case sbe::OrderExecuted::sbeTemplateId(): {
        sbe::OrderExecuted event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        onExecution(event);
        break;
      }
      case sbe::OrderRemoved::sbeTemplateId(): {
        sbe::OrderRemoved event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        onRemoval(event);
        break;
      }
      default:
        break;
    }
  }

  std::uint64_t alerts() const { return alertId_; }
  std::uint64_t washTrades() const { return washTrades_; }
  std::uint64_t spoofs() const { return spoofs_; }
  std::uint64_t layerings() const { return layerings_; }

 private:
  struct Watched {
    std::uint64_t orderId = 0;
    std::uint32_t participant = 0;
    std::uint32_t instrument = 0;
    std::int64_t remaining = 0;
    std::int64_t price = 0;
    std::uint8_t side = 2;
    bool rested = false;
  };

  // One participant's recent story on one clock: fills and cancels as fixed rings.
  struct Fill {
    std::uint64_t at = 0;
    std::uint32_t instrument = 0;
    std::uint8_t side = 2;
    std::int64_t quantity = 0;
  };
  struct Cancel {
    std::uint64_t at = 0;
    std::uint32_t instrument = 0;
    std::uint8_t side = 2;
    std::int64_t price = 0;
    std::int64_t quantity = 0;
  };
  struct History {
    Fill fills[HISTORY];
    Cancel cancels[HISTORY];
    std::size_t fillIn = 0;
    std::size_t cancelIn = 0;
    std::uint64_t quietUntil = 0;
  };

  void onExecution(sbe::OrderExecuted& event) {
    const std::uint64_t when = event.context().timestamp();
    const std::uint64_t sequence = event.context().sequence();
    const std::uint32_t instrument = event.context().instrumentId();
    const long aggressorAt = find(event.aggressorOrderId());
    const long restingAt = find(event.restingOrderId());
    const std::uint32_t aggressor =
        aggressorAt < 0 ? NOBODY : orders_[static_cast<std::size_t>(aggressorAt)].participant;
    const std::uint32_t resting =
        restingAt < 0 ? NOBODY : orders_[static_cast<std::size_t>(restingAt)].participant;

    // The simplest self-dealing, caught exactly: both sides of one print, one participant.
    if (aggressor != NOBODY && aggressor == resting &&
        event.aggressorOrderId() != event.restingOrderId()) {
      washTrades_++;
      alert(sbe::AlertKind::WASH_TRADE, sequence, when, aggressor, instrument, event.quantity(), 0,
            0);
    }

    // Each owner's fill enters its history: the resting owner filled at the resting order's
    // side, the aggressor at the opposite of it.
    if (restingAt >= 0) {
      Watched& order = orders_[static_cast<std::size_t>(restingAt)];
      recordFill(resting, when, instrument, order.side, event.quantity());
      evaluate(resting, instrument, sequence, when);
      order.remaining -= event.quantity();
      if (order.rested && order.remaining <= 0) {
        erase(static_cast<std::size_t>(restingAt));
      }
      if (aggressor != NOBODY && aggressor != resting) {
        recordFill(aggressor, when, instrument, static_cast<std::uint8_t>(1 - order.side),
                   event.quantity());
        evaluate(aggressor, instrument, sequence, when);
      }
    }
  }

  void onRemoval(sbe::OrderRemoved& event) {
    const long at = find(event.orderId());
    if (at < 0) {
      return;
    }
    const Watched order = orders_[static_cast<std::size_t>(at)];
    if (event.reason() == sbe::RemoveReason::CANCELLED ||
        event.reason() == sbe::RemoveReason::MASS_CANCELLED) {
      // A cancellation of resting interest is half the pattern; record it and re-judge.
      History& history = of(order.participant);
      Cancel& cancel = history.cancels[history.cancelIn++ & (HISTORY - 1)];
      cancel.at = event.context().timestamp();
      cancel.instrument = order.instrument;
      cancel.side = order.side;
      cancel.price = order.price;
      cancel.quantity = event.quantity();
      evaluate(order.participant, order.instrument, event.context().sequence(),
               event.context().timestamp());
    }
    if (event.reason() != sbe::RemoveReason::REPLACED) {
      erase(static_cast<std::size_t>(at));
    }
  }

  void recordFill(const std::uint32_t participant, const std::uint64_t when,
                  const std::uint32_t instrument, const std::uint8_t side,
                  const std::int64_t quantity) {
    if (participant == NOBODY || side > 1) {
      return;
    }
    History& history = of(participant);
    Fill& fill = history.fills[history.fillIn++ & (HISTORY - 1)];
    fill.at = when;
    fill.instrument = instrument;
    fill.side = side;
    fill.quantity = quantity;
  }

  // The judgment: for either side, fills there against cancels opposite, inside the window, on
  // this instrument. One alert per participant per window, so a storm is one case, not fifty.
  void evaluate(const std::uint32_t participant, const std::uint32_t instrument,
                const std::uint64_t sequence, const std::uint64_t when) {
    if (participant == NOBODY) {
      return;
    }
    History& history = of(participant);
    if (when < history.quietUntil) {
      return;
    }
    for (std::uint8_t side = 0; side <= 1; side++) {
      std::int64_t executed = 0;
      for (const Fill& fill : history.fills) {
        if (fill.quantity > 0 && fill.side == side && fill.instrument == instrument &&
            when - fill.at <= config_.windowNanos) {
          executed += fill.quantity;
        }
      }
      if (executed == 0) {
        continue;
      }
      std::int64_t cancelled = 0;
      std::uint32_t levels = 0;
      std::int64_t prices[HISTORY];
      for (const Cancel& cancel : history.cancels) {
        if (cancel.quantity <= 0 || cancel.side != 1 - side || cancel.instrument != instrument ||
            when - cancel.at > config_.windowNanos) {
          continue;
        }
        cancelled += cancel.quantity;
        bool fresh = true;
        for (std::uint32_t seen = 0; seen < levels; seen++) {
          if (prices[seen] == cancel.price) {
            fresh = false;
            break;
          }
        }
        if (fresh) {
          prices[levels++] = cancel.price;
        }
      }
      if (cancelled >= config_.spoofMultiple * executed && cancelled >= config_.minimumCancelled &&
          cancelled > 0) {
        const bool layered = levels >= config_.layeringLevels;
        (layered ? layerings_ : spoofs_)++;
        alert(layered ? sbe::AlertKind::LAYERING : sbe::AlertKind::SPOOFING, sequence, when,
              participant, instrument, executed, cancelled, levels);
        history.quietUntil = when + config_.windowNanos;
        return;
      }
    }
  }

  void alert(const sbe::AlertKind::Value kind, const std::uint64_t sequence,
             const std::uint64_t when, const std::uint32_t participant,
             const std::uint32_t instrument, const std::int64_t executed,
             const std::int64_t cancelled, const std::uint32_t levels) {
    char space[128] = {};
    sbe::SurveillanceAlert out;
    out.wrapAndApplyHeader(space, 0, sizeof space);
    out.alertId(++alertId_)
        .sequence(sequence)
        .timestamp(when)
        .executedQuantity(executed)
        .cancelledQuantity(cancelled)
        .participantId(participant)
        .instrumentId(instrument)
        .priceLevels(levels);
    out.kind(kind);
    sink_(space, sbe::MessageHeader::encodedLength() + sbe::SurveillanceAlert::sbeBlockLength());
  }

  History& of(const std::uint32_t participant) {
    for (std::size_t at = 0; at < participants_.size(); at++) {
      if (participants_[at] == participant) {
        return histories_[at];
      }
    }
    participants_.push_back(participant);
    histories_.emplace_back();
    return histories_.back();
  }

  // The fixed table: linear probing on the order id, backward-shift deletion.
  static std::size_t hashOf(const std::uint64_t key) {
    std::uint64_t mixed = key * 0x9E3779B97F4A7C15ULL;
    mixed ^= mixed >> 32;
    return static_cast<std::size_t>(mixed) & (ORDERS - 1);
  }

  Watched& slotFor(const std::uint64_t key) {
    std::size_t at = hashOf(key);
    while (keys_[at] != 0 && keys_[at] != key) {
      at = (at + 1) & (ORDERS - 1);
    }
    keys_[at] = key;
    return orders_[at];
  }

  long find(const std::uint64_t key) const {
    std::size_t at = hashOf(key);
    while (keys_[at] != 0) {
      if (keys_[at] == key) {
        return static_cast<long>(at);
      }
      at = (at + 1) & (ORDERS - 1);
    }
    return -1;
  }

  void erase(std::size_t at) {
    keys_[at] = 0;
    std::size_t hole = at;
    std::size_t probe = (at + 1) & (ORDERS - 1);
    while (keys_[probe] != 0) {
      const std::size_t wants = hashOf(keys_[probe]);
      const bool movable = ((probe - wants) & (ORDERS - 1)) >= ((probe - hole) & (ORDERS - 1));
      if (movable) {
        keys_[hole] = keys_[probe];
        orders_[hole] = orders_[probe];
        keys_[probe] = 0;
        hole = probe;
      }
      probe = (probe + 1) & (ORDERS - 1);
    }
  }

  // Unrested entries are aggressors that filled whole and will never be spoken of again; they
  // age out by orderId distance, deterministically, because the numbering is the engine's own.
  void sweep() {
    if (newestOrderId_ <= AGE_HORIZON) {
      return;
    }
    const std::uint64_t oldest = newestOrderId_ - AGE_HORIZON;
    for (std::size_t at = 0; at < ORDERS; at++) {
      if (keys_[at] != 0 && !orders_[at].rested && orders_[at].orderId < oldest) {
        erase(at);
        // The shift may have pulled an entry into this slot; judge it too.
        at--;
      }
    }
  }

  static constexpr std::uint32_t NOBODY = 0xFFFFFFFF;

  Sink& sink_;
  Config config_;
  std::vector<std::uint64_t> keys_;
  std::vector<Watched> orders_;
  std::vector<std::uint32_t> participants_;
  std::vector<History> histories_;
  std::uint64_t newestOrderId_ = 0;
  std::uint64_t accepted_ = 0;
  std::uint64_t alertId_ = 0;
  std::uint64_t washTrades_ = 0;
  std::uint64_t spoofs_ = 0;
  std::uint64_t layerings_ = 0;
};

}  // namespace exchange::oversight
