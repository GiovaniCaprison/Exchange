// The post-trade ledger (docs/components/posttrade.md): the purest consumer in the venue. It
// holds no authority and makes no decisions; it keeps the books. Every execution resolves to a
// buyer and a seller through the shared ownership discipline, joins the trade tape in sequence
// order, and moves two position accounts by equal and opposite amounts, which is why the
// ledger's whole correctness claim is arithmetic: positions sum to zero per instrument and cash
// sums to zero across the venue, at every moment. At the close it writes the day as the two
// files PROTOCOL.md documents, deterministic in content and in order, so a replayed day writes
// byte-identical files.

#pragma once

#include <algorithm>
#include <cstdint>
#include <ostream>
#include <stdexcept>
#include <vector>

#include "exchange_protocol/MessageHeader.h"
#include "exchange_protocol/OrderAccepted.h"
#include "exchange_protocol/OrderExecuted.h"
#include "exchange_protocol/OrderReduced.h"
#include "exchange_protocol/OrderRemoved.h"
#include "exchange_protocol/OrderRested.h"
#include "exchange_protocol/SessionStateChanged.h"
#include "ownership.hpp"

namespace exchange::posttrade {

namespace sbe = ::exchange::protocol;

struct Trade {
  std::uint64_t executionId = 0;
  std::uint64_t sequence = 0;
  std::uint64_t timestamp = 0;
  std::uint32_t instrumentId = 0;
  std::int64_t price = 0;
  std::int64_t quantity = 0;
  std::uint32_t buyer = 0;
  std::uint32_t seller = 0;
};

struct Account {
  std::uint32_t participant = 0;
  std::uint32_t instrumentId = 0;
  std::int64_t position = 0;
  std::int64_t bought = 0;
  std::int64_t sold = 0;
  std::int64_t cash = 0;
};

class Ledger {
 public:
  static constexpr std::size_t TRADES = 1 << 20;

  Ledger() {
    trades_.reserve(TRADES);
    accounts_.reserve(256);
    instruments_.reserve(64);
    closed_.reserve(64);
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
        note(event.context().instrumentId());
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
        owners_.onRemoved(event.orderId(), event.reason() == sbe::RemoveReason::REPLACED);
        break;
      }
      case sbe::SessionStateChanged::sbeTemplateId(): {
        sbe::SessionStateChanged event;
        event.wrapForDecode(message, wrap.encodedLength(), wrap.blockLength(), wrap.version(),
                            length);
        note(event.context().instrumentId());
        setClosed(event.context().instrumentId(), event.state() == sbe::SessionState::CLOSED);
        break;
      }
      default:
        break;
    }
  }

  // The day is over when every instrument the stream ever mentioned is closed.
  bool dayIsDone() const {
    if (instruments_.empty()) {
      return false;
    }
    for (const bool closed : closed_) {
      if (!closed) {
        return false;
      }
    }
    return true;
  }

  // The files, exactly as PROTOCOL.md states them ----------------------------------------------

  void writeTrades(std::ostream& out) const {
    out << "executionId,sequence,timestamp,instrumentId,price,quantity,buyer,seller\n";
    for (const Trade& trade : trades_) {
      out << trade.executionId << ',' << trade.sequence << ',' << trade.timestamp << ','
          << trade.instrumentId << ',' << trade.price << ',' << trade.quantity << ',' << trade.buyer
          << ',' << trade.seller << '\n';
    }
  }

  void writePositions(std::ostream& out) const {
    std::vector<Account> sorted = accounts_;
    std::sort(sorted.begin(), sorted.end(), [](const Account& a, const Account& b) {
      if (a.participant != b.participant) {
        return a.participant < b.participant;
      }
      return a.instrumentId < b.instrumentId;
    });
    out << "participantId,instrumentId,position,bought,sold,cash\n";
    for (const Account& account : sorted) {
      out << account.participant << ',' << account.instrumentId << ',' << account.position << ','
          << account.bought << ',' << account.sold << ',' << account.cash << '\n';
    }
  }

  // The questions the suites ask ----------------------------------------------------------------

  const std::vector<Trade>& trades() const { return trades_; }
  const std::vector<Account>& accounts() const { return accounts_; }

  std::int64_t positionOf(const std::uint32_t participant, const std::uint32_t instrumentId) const {
    for (const Account& account : accounts_) {
      if (account.participant == participant && account.instrumentId == instrumentId) {
        return account.position;
      }
    }
    return 0;
  }

  std::int64_t cashOf(const std::uint32_t participant) const {
    std::int64_t total = 0;
    for (const Account& account : accounts_) {
      if (account.participant == participant) {
        total += account.cash;
      }
    }
    return total;
  }

 private:
  void onExecution(sbe::OrderExecuted& event) {
    const long restingAt = owners_.find(event.restingOrderId());
    const long aggressorAt = owners_.find(event.aggressorOrderId());
    if (restingAt < 0 || aggressorAt < 0) {
      unresolved_++;
      return;
    }
    const common::OwnedOrder& resting = owners_.at(restingAt);
    const common::OwnedOrder& aggressor = owners_.at(aggressorAt);
    // The resting order's side names the trade's sides: its owner bought if it was a bid.
    const bool restingBought = resting.side == 0;
    if (trades_.size() == TRADES) {
      throw std::runtime_error("the trade tape overflowed; size it for the session");
    }
    Trade& trade = trades_.emplace_back();
    trade.executionId = event.executionId();
    trade.sequence = event.context().sequence();
    trade.timestamp = event.context().timestamp();
    trade.instrumentId = event.context().instrumentId();
    trade.price = event.price();
    trade.quantity = event.quantity();
    trade.buyer = restingBought ? resting.participant : aggressor.participant;
    trade.seller = restingBought ? aggressor.participant : resting.participant;
    book(trade.buyer, trade.instrumentId, trade.quantity, trade.price, true);
    book(trade.seller, trade.instrumentId, trade.quantity, trade.price, false);
    owners_.onExecuted(event.restingOrderId(), event.quantity());
  }

  void book(const std::uint32_t participant, const std::uint32_t instrumentId,
            const std::int64_t quantity, const std::int64_t price, const bool bought) {
    for (Account& account : accounts_) {
      if (account.participant == participant && account.instrumentId == instrumentId) {
        apply(account, quantity, price, bought);
        return;
      }
    }
    Account& account = accounts_.emplace_back();
    account.participant = participant;
    account.instrumentId = instrumentId;
    apply(account, quantity, price, bought);
  }

  static void apply(Account& account, const std::int64_t quantity, const std::int64_t price,
                    const bool bought) {
    if (bought) {
      account.position += quantity;
      account.bought += quantity;
      account.cash -= quantity * price;
    } else {
      account.position -= quantity;
      account.sold += quantity;
      account.cash += quantity * price;
    }
  }

  void note(const std::uint32_t instrumentId) {
    for (const std::uint32_t known : instruments_) {
      if (known == instrumentId) {
        return;
      }
    }
    instruments_.push_back(instrumentId);
    closed_.push_back(false);
  }

  void setClosed(const std::uint32_t instrumentId, const bool closed) {
    for (std::size_t at = 0; at < instruments_.size(); at++) {
      if (instruments_[at] == instrumentId) {
        closed_[at] = closed;
        return;
      }
    }
  }

  common::Ownership owners_;
  std::vector<Trade> trades_;
  std::vector<Account> accounts_;
  std::vector<std::uint32_t> instruments_;
  std::vector<bool> closed_;
  std::uint64_t unresolved_ = 0;
};

}  // namespace exchange::posttrade
