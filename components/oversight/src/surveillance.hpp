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
// Ownership is learned from acceptances like every routing consumer, through the shared
// discipline in components/common/ownership.hpp.

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
#include "ownership.hpp"

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
  static constexpr std::size_t HISTORY = 256;

  Surveillance(Sink& sink, const Config config) : sink_(sink), config_(config) {
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
        owners_.accepted(event.orderId(), event.participantId(), event.context().instrumentId());
        break;
      }
      case sbe::OrderRested::sbeTemplateId(): {
        sbe::OrderRested event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        owners_.onRested(event.orderId(),
                         static_cast<std::uint8_t>(event.side() == sbe::Side::BUY ? 0 : 1),
                         event.price(), event.quantity());
        break;
      }
      case sbe::OrderReduced::sbeTemplateId(): {
        sbe::OrderReduced event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        owners_.onReduced(event.orderId(), event.quantity());
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
    const long aggressorAt = owners_.find(event.aggressorOrderId());
    const long restingAt = owners_.find(event.restingOrderId());
    const std::uint32_t aggressor = aggressorAt < 0 ? NOBODY : owners_.at(aggressorAt).participant;
    const std::uint32_t resting = restingAt < 0 ? NOBODY : owners_.at(restingAt).participant;

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
      const std::uint8_t restingSide = owners_.at(restingAt).side;
      recordFill(resting, when, instrument, restingSide, event.quantity());
      evaluate(resting, instrument, sequence, when);
      owners_.onExecuted(event.restingOrderId(), event.quantity());
      if (aggressor != NOBODY && aggressor != resting) {
        recordFill(aggressor, when, instrument, static_cast<std::uint8_t>(1 - restingSide),
                   event.quantity());
        evaluate(aggressor, instrument, sequence, when);
      }
    }
  }

  void onRemoval(sbe::OrderRemoved& event) {
    const long at = owners_.find(event.orderId());
    if (at < 0) {
      return;
    }
    const common::OwnedOrder order = owners_.at(at);
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
    owners_.onRemoved(event.orderId(), event.reason() == sbe::RemoveReason::REPLACED);
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

  static constexpr std::uint32_t NOBODY = 0xFFFFFFFF;

  Sink& sink_;
  Config config_;
  common::Ownership owners_;
  std::vector<std::uint32_t> participants_;
  std::vector<History> histories_;
  std::uint64_t alertId_ = 0;
  std::uint64_t washTrades_ = 0;
  std::uint64_t spoofs_ = 0;
  std::uint64_t layerings_ = 0;
};

}  // namespace exchange::oversight
