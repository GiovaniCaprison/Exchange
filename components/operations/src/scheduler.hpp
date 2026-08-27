// The market's clock (docs/components/operations.md): the matcher moves state only on
// SessionControl commands, and this is what sends them, on schedule and on triggers. The
// scheduler is a consumer with authority: it reads the same stream as everyone, decides, and can
// only act by submitting commands that take effect when sequenced, never by reaching into a
// matcher (P-1). Its halt decisions are a pure function of the stream, LULD-shaped bands over
// the trailing mean of executions at stream timestamps, so the same day decides the same halts;
// the calendar fires on the injected clock, which every test scripts.
//
// A halt reopens through an auction because a halted book needs a fair price to restart from,
// which is why real venues reopen that way (the LULD Plan, public); the calendar outranks the
// halt machine, so a scheduled close closes a halted instrument too.

#pragma once

#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "exchange_protocol/GatewaySubmission.h"
#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/SessionControl.h"
#include "exchange_protocol/SessionStateChanged.h"
#include "sequencer.hpp"

namespace exchange::operations {

namespace sbe = ::exchange::protocol;

struct Transition {
  std::uint64_t at = 0;
  sbe::SessionState::Value state = sbe::SessionState::CLOSED;
};

struct Config {
  std::vector<std::uint32_t> instruments;
  // Ordered by time; each applies to every instrument the venue trades.
  std::vector<Transition> calendar;
  // The band in basis points around the trailing mean, and the trailing window in stream time.
  std::int64_t bandBasisPoints = 500;
  std::uint64_t windowNanos = 300'000'000'000ULL;
  // LULD's shape: the pause, then the reopening call, then continuous trading again.
  std::uint64_t haltNanos = 300'000'000'000ULL;
  std::uint64_t auctionNanos = 15'000'000'000ULL;
  std::uint32_t gatewayId = 90;
  std::uint64_t resubmitAfterNanos = 500'000'000ULL;
};

template <typename SubmitRing, typename Clock>
class Scheduler {
 public:
  static constexpr std::uint64_t PENDING = 1 << 12;
  static constexpr std::size_t PRINTS = 1 << 12;

  Scheduler(SubmitRing& submissions, Clock& clock, Config config)
      : submissions_(submissions), clock_(clock), config_(std::move(config)) {
    instruments_.resize(config_.instruments.size());
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      Instrument& instrument = instruments_[at];
      instrument.id = config_.instruments[at];
      instrument.printTimes.assign(PRINTS, 0);
      instrument.printPrices.assign(PRINTS, 0);
    }
    pending_.resize(PENDING);
    pendingArena_.resize(std::size_t{1} << 20);
    lastAck_ = clock_.now();
  }

  // The calendar and the halt machine both advance here, on the injected clock.
  void onTick() {
    const std::uint64_t now = clock_.now();
    while (nextTransition_ < config_.calendar.size() &&
           now >= config_.calendar[nextTransition_].at) {
      const sbe::SessionState::Value state = config_.calendar[nextTransition_].state;
      for (Instrument& instrument : instruments_) {
        submit(instrument.id, state);
        // The calendar outranks the halt machine.
        instrument.phase = Phase::TRADING;
      }
      nextTransition_++;
    }
    for (Instrument& instrument : instruments_) {
      if (instrument.phase == Phase::HALTED && now >= instrument.phaseUntil) {
        submit(instrument.id, sbe::SessionState::OPENING_AUCTION);
        instrument.phase = Phase::REOPENING;
        instrument.phaseUntil = now + config_.auctionNanos;
      } else if (instrument.phase == Phase::REOPENING && now >= instrument.phaseUntil) {
        submit(instrument.id, sbe::SessionState::CONTINUOUS);
        instrument.phase = Phase::TRADING;
      }
    }
    if (nextGatewaySequence_ - 1 > ackedUpTo_ && now - lastAck_ >= config_.resubmitAfterNanos) {
      resubmitUnacked();
      lastAck_ = now;
    }
  }

  // The operator's hand: halt one instrument now, reopening on the usual machinery.
  void haltNow(const std::uint32_t instrumentId) {
    Instrument* instrument = find(instrumentId);
    if (instrument == nullptr || instrument->phase != Phase::TRADING) {
      return;
    }
    halt(*instrument);
  }

  void onAck(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    sbe::CommandSequenced ack;
    ack.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(), length);
    if (ack.gatewaySequence() > ackedUpTo_) {
      ackedUpTo_ = ack.gatewaySequence();
    }
    lastAck_ = clock_.now();
  }

  void onEvent(char* message, const std::size_t length) {
    sbe::MessageHeader wrap;
    wrap.wrap(message, 0, 0, length);
    if (wrap.templateId() == sbe::SessionStateChanged::sbeTemplateId()) {
      sbe::SessionStateChanged event;
      event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                          length);
      Instrument* instrument = find(event.context().instrumentId());
      if (instrument != nullptr) {
        instrument->confirmed = event.state();
      }
      return;
    }
    if (wrap.templateId() != sbe::OrderExecuted::sbeTemplateId()) {
      return;
    }
    sbe::OrderExecuted event;
    event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(), length);
    Instrument* instrument = find(event.context().instrumentId());
    if (instrument == nullptr) {
      return;
    }
    const std::uint64_t when = event.context().timestamp();
    const std::int64_t price = event.price();

    // The reference is the trailing mean at stream time, LULD's shape; the print that breaches
    // the band halts its instrument, judged against the reference that stood before it.
    evict(*instrument, when);
    if (instrument->confirmed == sbe::SessionState::CONTINUOUS &&
        instrument->phase == Phase::TRADING && instrument->printCount > 0) {
      const std::int64_t reference =
          instrument->printSum / static_cast<std::int64_t>(instrument->printCount);
      const std::int64_t away = price > reference ? price - reference : reference - price;
      if (away * 10'000 > reference * config_.bandBasisPoints) {
        halt(*instrument);
      }
    }
    remember(*instrument, when, price);
  }

  sbe::SessionState::Value confirmedOf(const std::uint32_t instrumentId) {
    Instrument* instrument = find(instrumentId);
    return instrument == nullptr ? sbe::SessionState::CLOSED : instrument->confirmed;
  }
  std::uint64_t submitted() const { return nextGatewaySequence_ - 1; }
  std::uint64_t acked() const { return ackedUpTo_; }
  std::uint64_t resubmitted() const { return resubmitted_; }
  std::uint64_t halts() const { return halts_; }

 private:
  enum class Phase : std::uint8_t { TRADING, HALTED, REOPENING };

  struct Instrument {
    std::uint32_t id = 0;
    sbe::SessionState::Value confirmed = sbe::SessionState::CLOSED;
    Phase phase = Phase::TRADING;
    std::uint64_t phaseUntil = 0;
    // The trailing prints as a ring: times, prices, a running sum, and the live span.
    std::vector<std::uint64_t> printTimes;
    std::vector<std::int64_t> printPrices;
    std::size_t printHead = 0;
    std::size_t printCount = 0;
    std::int64_t printSum = 0;
  };

  Instrument* find(const std::uint32_t instrumentId) {
    for (Instrument& instrument : instruments_) {
      if (instrument.id == instrumentId) {
        return &instrument;
      }
    }
    return nullptr;
  }

  void halt(Instrument& instrument) {
    submit(instrument.id, sbe::SessionState::HALTED);
    instrument.phase = Phase::HALTED;
    instrument.phaseUntil = clock_.now() + config_.haltNanos;
    // The band restarts from the reopening print rather than the panic before it.
    instrument.printHead = 0;
    instrument.printCount = 0;
    instrument.printSum = 0;
    halts_++;
  }

  void evict(Instrument& instrument, const std::uint64_t now) {
    while (instrument.printCount > 0) {
      const std::size_t oldest =
          (instrument.printHead + PRINTS - instrument.printCount) & (PRINTS - 1);
      if (now - instrument.printTimes[oldest] <= config_.windowNanos) {
        break;
      }
      instrument.printSum -= instrument.printPrices[oldest];
      instrument.printCount--;
    }
  }

  void remember(Instrument& instrument, const std::uint64_t when, const std::int64_t price) {
    if (instrument.printCount == PRINTS) {
      const std::size_t oldest = instrument.printHead;
      instrument.printSum -= instrument.printPrices[oldest];
      instrument.printCount--;
    }
    instrument.printTimes[instrument.printHead] = when;
    instrument.printPrices[instrument.printHead] = price;
    instrument.printHead = (instrument.printHead + 1) & (PRINTS - 1);
    instrument.printCount++;
    instrument.printSum += price;
  }

  void submit(const std::uint32_t instrumentId, const sbe::SessionState::Value state) {
    const std::uint64_t gatewaySequence = nextGatewaySequence_++;
    if (gatewaySequence - ackedUpTo_ > PENDING) {
      throw std::runtime_error("the scheduler's pending window overflowed; the venue is gone");
    }
    constexpr std::size_t COMMAND =
        sbe::MessageHeader::encodedLength() + sbe::SessionControl::sbeBlockLength();
    constexpr std::size_t RECORD = sequencer::SUBMISSION_BYTES + COMMAND;
    const std::size_t at = submissions_.claim(RECORD);
    sbe::GatewaySubmission envelope;
    envelope.wrapAndApplyHeader(submissions_.buffer(), at, at + RECORD);
    envelope.gatewaySequence(gatewaySequence).gatewayId(config_.gatewayId).reserved(0);
    sbe::SessionControl command;
    command.wrapAndApplyHeader(submissions_.buffer(), at + sequencer::SUBMISSION_BYTES,
                               at + RECORD);
    command.context().sequence(0).timestamp(0).instrumentId(instrumentId).reserved(0);
    command.state(state);
    submissions_.commit();
    submissions_.publish();
    // The same bytes wait for the acknowledgment that retires them.
    if (arenaIn_ + RECORD > pendingArena_.size()) {
      arenaIn_ = 0;
    }
    std::memcpy(pendingArena_.data() + arenaIn_, submissions_.buffer() + at, RECORD);
    Pending& entry = pending_[(gatewaySequence - 1) & (PENDING - 1)];
    entry.offset = static_cast<std::uint32_t>(arenaIn_);
    entry.length = static_cast<std::uint32_t>(RECORD);
    arenaIn_ += RECORD;
  }

  void resubmitUnacked() {
    for (std::uint64_t sequence = ackedUpTo_ + 1; sequence < nextGatewaySequence_; sequence++) {
      const Pending& entry = pending_[(sequence - 1) & (PENDING - 1)];
      const std::size_t at = submissions_.claim(entry.length);
      std::memcpy(submissions_.buffer() + at, pendingArena_.data() + entry.offset, entry.length);
      submissions_.commit();
      submissions_.publish();
      resubmitted_++;
    }
  }

  struct Pending {
    std::uint32_t offset = 0;
    std::uint32_t length = 0;
  };

  SubmitRing& submissions_;
  Clock& clock_;
  Config config_;
  std::vector<Instrument> instruments_;
  std::size_t nextTransition_ = 0;
  std::vector<Pending> pending_;
  std::vector<char> pendingArena_;
  std::size_t arenaIn_ = 0;
  std::uint64_t nextGatewaySequence_ = 1;
  std::uint64_t ackedUpTo_ = 0;
  std::uint64_t lastAck_ = 0;
  std::uint64_t resubmitted_ = 0;
  std::uint64_t halts_ = 0;
};

}  // namespace exchange::operations
