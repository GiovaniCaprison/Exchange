// One instrument's engine: the book, the trigger book, the auction, and the decisions between
// them. The partition routes sequenced commands here and owns everything shared across
// instruments: the slab, the feed, and the order, execution and arrival counters.
//
// The hot path is shaped so the common case, an order that fills or rests against the best few
// levels, does the least possible work. The taker lives in registers for the whole walk and only
// touches the slab if it rests, so an aggressive order that fills completely never writes an order
// anywhere. The crossing test is one comparison because limits arrive as ranks in the resting
// side's own space. Allocation and every other per-instrument fact bind at definition (P-5), and
// nothing here allocates after definition (P-6).

#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <vector>

#include "book.hpp"
#include "exchange_protocol/NewOrder.h"
#include "exchange_protocol/RejectReason.h"
#include "exchange_protocol/RemoveReason.h"
#include "exchange_protocol/SessionState.h"
#include "feed.hpp"
#include "slab.hpp"
#include "triggers.hpp"

namespace exchange::matcher {

// The counters one partition shares across its instruments: ids and arrivals are per partition,
// so every event stream the partition emits numbers from one sequence of decisions.
struct Counters {
  std::uint64_t nextOrderId = 1;
  std::uint64_t nextExecutionId = 1;
  std::int64_t arrival = 0;
};

template <typename Ring>
class Engine {
 public:
  Engine(Slab& slab, Feed<Ring>& feed, Counters& counters, const std::int64_t tickSize,
         const std::int64_t lotSize, const std::int64_t minPrice, const std::int64_t maxPrice,
         const std::int64_t bandWidth, const std::int64_t openingReference, const bool proRata)
      : slab_(slab),
        feed_(feed),
        counters_(counters),
        book_(slab, tickSize, (minPrice + tickSize - 1) / tickSize,
              static_cast<std::int32_t>(maxPrice / tickSize - (minPrice + tickSize - 1) / tickSize +
                                        1)),
        triggers_(slab),
        tickSize_(tickSize),
        lotSize_(lotSize),
        minPrice_(minPrice),
        maxPrice_(maxPrice),
        bandWidth_(bandWidth),
        baseTick_((minPrice + tickSize - 1) / tickSize),
        reference_(openingReference),
        proRata_(proRata) {
    pending_.reserve(1024);
    snapshot_.reserve(1024);
    gathered_.reserve(1024);
    buys_.reserve(4096);
    sells_.reserve(4096);
  }

  // Order entry -------------------------------------------------------------------------------

  void enter(sbe::NewOrder& command) {
    const std::uint64_t clientOrderId = command.clientOrderId();
    const std::uint32_t participantId = command.participantId();
    Taker taker;
    taker.clientOrderId = clientOrderId;
    taker.participantId = participantId;
    taker.smpId = command.smpId();
    taker.remaining = command.quantity();
    taker.executed = 0;
    taker.minQuantity = command.minQuantity();
    taker.displaySize = command.displayQuantity();
    taker.side = static_cast<std::uint8_t>(command.side());
    taker.pricing = static_cast<std::uint8_t>(command.pricing());
    taker.timeInForce = static_cast<std::uint8_t>(command.timeInForce());
    taker.postOnly = command.flags().postOnly();
    const std::int64_t price = command.price();
    const std::int64_t triggerPrice = command.triggerPrice();

    const std::int64_t verdict = refusalOrTick(taker, price, triggerPrice);
    if (verdict < 0) {
      feed_.rejected(clientOrderId, participantId, reasonOf(verdict));
      return;
    }
    taker.tick = static_cast<std::int32_t>(verdict);
    taker.limitRank = limitRankOf(taker);
    taker.id = counters_.nextOrderId++;
    feed_.accepted(taker.id, clientOrderId, participantId);

    if (triggerPrice != 0) {
      // A stop rests in the trigger book and is not book liquidity. It is the one entry path that
      // writes the slab before matching, because a waiting stop has to live somewhere.
      const std::int32_t slot = place(taker, ++counters_.arrival, triggerPrice);
      triggers_.add(slot);
      // A stop whose price the market has already reached is due now.
      fireTriggers();
      return;
    }
    if (state_ == CONTINUOUS) {
      match(taker);
      fireTriggers();
    }
    settle(taker);
  }

  void cancel(const std::uint64_t clientOrderId, const std::uint32_t participantId) {
    if (state_ == CLOSED) {
      feed_.rejected(clientOrderId, participantId, sbe::RejectReason::STATE_NOT_PERMITTED);
      return;
    }
    const std::int32_t resting = book_.named(participantId, clientOrderId);
    if (resting != 0) {
      const std::int32_t side = slab_.cold(resting).side;
      const std::uint64_t id = slab_.hot(resting).id;
      const std::int64_t displayed = slab_.hot(resting).displayed;
      book_.remove(side, resting);
      slab_.release(resting);
      feed_.removed(id, displayed, sbe::RemoveReason::CANCELLED);
      reportIndicative();
      return;
    }
    const std::int32_t stop = triggers_.named(participantId, clientOrderId);
    if (stop != 0) {
      // A stop never appeared as resting, so what it takes with it is its whole quantity.
      triggers_.remove(stop);
      const std::uint64_t id = slab_.hot(stop).id;
      const std::int64_t remaining = slab_.hot(stop).remaining;
      slab_.release(stop);
      feed_.removed(id, remaining, sbe::RemoveReason::CANCELLED);
      return;
    }
    feed_.rejected(clientOrderId, participantId, sbe::RejectReason::UNKNOWN_ORDER);
  }

  void replace(const std::uint64_t clientOrderId, const std::uint32_t participantId,
               const std::int64_t quantity, const std::int64_t price) {
    if (state_ == CLOSED) {
      feed_.rejected(clientOrderId, participantId, sbe::RejectReason::STATE_NOT_PERMITTED);
      return;
    }
    const std::int32_t resting = book_.named(participantId, clientOrderId);
    if (resting == 0) {
      feed_.rejected(clientOrderId, participantId, sbe::RejectReason::UNKNOWN_ORDER);
      return;
    }
    const std::int64_t verdict = refusalOrTickForReplace(resting, quantity, price);
    if (verdict < 0) {
      feed_.rejected(clientOrderId, participantId, reasonOf(verdict));
      return;
    }
    const std::int32_t newTick = static_cast<std::int32_t>(verdict);
    Slab::Hot& hot = slab_.hot(resting);
    const Slab::Cold& cold = slab_.cold(resting);
    const std::int64_t remainder = quantity - hot.executed;
    if (newTick == hot.tick && remainder < hot.remaining) {
      // Lowering quantity at the same price keeps queue position.
      const std::int64_t shownBefore = hot.displayed;
      const std::int64_t remainingBefore = hot.remaining;
      hot.remaining = remainder;
      hot.displayed = cold.displaySize == 0 ? remainder : std::min(cold.displaySize, remainder);
      book_.quantitiesChanged(cold.side, resting, hot.displayed - shownBefore,
                              remainder - remainingBefore);
      feed_.reduced(hot.id, hot.displayed);
      reportIndicative();
      return;
    }
    // Anything else is a removal and a fresh rest, keeping both ids and the display size the
    // order was entered with. The old slot goes back before the walk, since the taker needs
    // nothing from it once copied.
    Taker taker;
    taker.id = hot.id;
    taker.clientOrderId = clientOrderId;
    taker.participantId = participantId;
    taker.smpId = hot.smpId;
    taker.remaining = remainder;
    taker.executed = hot.executed;
    taker.minQuantity = cold.minQuantity;
    taker.displaySize = cold.displaySize;
    taker.tick = newTick;
    taker.side = cold.side;
    taker.pricing = cold.pricing;
    taker.timeInForce = cold.timeInForce;
    taker.postOnly = cold.postOnly;
    taker.limitRank = limitRankOf(taker);
    const std::int64_t shown = hot.displayed;
    book_.remove(cold.side, resting);
    slab_.release(resting);
    feed_.removed(taker.id, shown, sbe::RemoveReason::REPLACED);
    if (state_ == CONTINUOUS) {
      match(taker);
      fireTriggers();
    }
    settle(taker);
  }

  void massCancel(const std::uint64_t clientOrderId, const std::uint32_t participantId) {
    if (state_ == CLOSED) {
      feed_.rejected(clientOrderId, participantId, sbe::RejectReason::STATE_NOT_PERMITTED);
      return;
    }
    gathered_.clear();
    book_.of(participantId, gathered_);
    triggers_.of(participantId, gathered_);
    sortByArrival(gathered_);
    for (const std::int32_t slot : gathered_) {
      const std::uint64_t id = slab_.hot(slot).id;
      if (slab_.cold(slot).triggerPrice != 0) {
        triggers_.remove(slot);
        feed_.removed(id, slab_.hot(slot).remaining, sbe::RemoveReason::MASS_CANCELLED);
      } else {
        book_.remove(slab_.cold(slot).side, slot);
        feed_.removed(id, slab_.hot(slot).displayed, sbe::RemoveReason::MASS_CANCELLED);
      }
      slab_.release(slot);
    }
    reportIndicative();
  }

  void changeState(const std::int32_t entering) {
    if (callPhase(state_) && entering != state_) {
      // Leaving a call phase is what runs the uncrossing, before the new state is reported.
      uncross();
    }
    state_ = entering;
    feed_.stateChanged(state_);
    indicativePrice_ = 0;
    indicativeQuantity_ = 0;
    if (callPhase(state_)) {
      reportIndicative();
    }
  }

  // Views for the tests, allocating freely because tests own their time -------------------------

  const Book& book() const { return book_; }
  const Slab& slab() const { return slab_; }
  std::int32_t sessionState() const { return state_; }
  std::vector<std::int32_t> waitingStops() const {
    std::vector<std::int32_t> all;
    for (std::int32_t slot = triggers_.headSlot(); slot != 0;
         slot = static_cast<std::int32_t>(slab_.hot(slot).next)) {
      all.push_back(slot);
    }
    return all;
  }

 private:
  // The order being worked, in registers for the whole walk. It reaches the slab only if it
  // rests, so a taker that fills completely never writes an order anywhere.
  struct Taker {
    std::uint64_t id;
    std::uint64_t clientOrderId;
    std::uint64_t smpId;
    std::int64_t remaining;
    std::int64_t executed;
    std::int64_t minQuantity;
    std::int64_t displaySize;
    std::uint32_t participantId;
    std::int32_t tick;
    std::int32_t limitRank;
    std::uint8_t side;
    std::uint8_t pricing;
    std::uint8_t timeInForce;
    bool postOnly;
  };

  static constexpr std::int32_t LIMIT = sbe::Pricing::LIMIT;
  static constexpr std::int32_t MARKET = sbe::Pricing::MARKET;
  static constexpr std::int32_t DAY = sbe::TimeInForce::DAY;
  static constexpr std::int32_t IOC = sbe::TimeInForce::IMMEDIATE_OR_CANCEL;
  static constexpr std::int32_t FOK = sbe::TimeInForce::FILL_OR_KILL;
  static constexpr std::int32_t PRE_OPEN = sbe::SessionState::PRE_OPEN;
  static constexpr std::int32_t OPENING_AUCTION = sbe::SessionState::OPENING_AUCTION;
  static constexpr std::int32_t CONTINUOUS = sbe::SessionState::CONTINUOUS;
  static constexpr std::int32_t CLOSING_AUCTION = sbe::SessionState::CLOSING_AUCTION;
  static constexpr std::int32_t CLOSED = sbe::SessionState::CLOSED;

  static bool callPhase(const std::int32_t state) {
    return state == OPENING_AUCTION || state == CLOSING_AUCTION;
  }

  // Matching ----------------------------------------------------------------------------------

  void match(Taker& taker) {
    const bool proRata = proRata_;
    while (taker.remaining > 0) {
      const std::int32_t resting = book_.nextToTake(taker.side, taker.limitRank);
      if (resting == 0) {
        return;
      }
      if (taker.smpId != 0 && slab_.hot(resting).smpId == taker.smpId) {
        // The resting order goes and the walk continues into whatever was behind it.
        prevented(resting, taker.side);
        continue;
      }
      if (proRata) {
        proRataTake(taker, slab_.hot(resting).tick);
      } else {
        takeExactly(taker, resting, std::min(taker.remaining, slab_.hot(resting).displayed));
      }
    }
  }

  void prevented(const std::int32_t resting, const std::int32_t takerSide) {
    const std::uint64_t id = slab_.hot(resting).id;
    const std::int64_t displayed = slab_.hot(resting).displayed;
    book_.remove(takerSide ^ 1, resting);
    slab_.release(resting);
    feed_.removed(id, displayed, sbe::RemoveReason::SELF_MATCH_PREVENTED);
  }

  // One execution, at the price the resting order named.
  void takeExactly(Taker& taker, const std::int32_t resting, const std::int64_t quantity) {
    Slab::Hot& hot = slab_.hot(resting);
    const std::int64_t price = book_.priceOfTick(hot.tick);
    taker.remaining -= quantity;
    taker.executed += quantity;
    const std::int64_t shownBefore = hot.displayed;
    hot.remaining -= quantity;
    hot.executed += quantity;
    hot.displayed -= quantity;
    const bool replenishes = hot.displayed == 0 && hot.remaining > 0;
    feed_.executed(counters_.nextExecutionId++, taker.id, hot.id, price, quantity);
    reference_ = price;
    lastExecuted_ = price;
    const std::int32_t restingSide = taker.side ^ 1;
    if (hot.remaining == 0) {
      book_.quantitiesChanged(restingSide, resting, -shownBefore, -quantity);
      book_.remove(restingSide, resting);
      slab_.release(resting);
      return;
    }
    if (replenishes) {
      // One delta covers the take and the reveal: the level goes from holding what this order
      // showed before the execution to holding its fresh tranche, at the back of its queue.
      const std::int64_t size = slab_.cold(resting).displaySize;
      hot.displayed = size == 0 ? hot.remaining : std::min(size, hot.remaining);
      hot.arrival = ++counters_.arrival;
      book_.requeued(restingSide, resting, hot.displayed - shownBefore, -quantity);
      feed_.rested(hot.id, price, hot.displayed, restingSide);
      return;
    }
    book_.quantitiesChanged(restingSide, resting, hot.displayed - shownBefore, -quantity);
  }

  // Pro-rata at one price: shares in proportion to displayed quantity, rounded down to a whole
  // lot, and whatever rounding left over goes in arrival order. The queue is copied out before
  // anything trades, because a fill unlinks and a replenish re-queues, and the allocation is owed
  // to the queue as it stood.
  void proRataTake(Taker& taker, const std::int32_t tick) {
    const std::int32_t restingSide = taker.side ^ 1;
    const std::int32_t head = book_.headAtRank(restingSide, book_.rankOf(restingSide, tick));
    if (head == 0) {
      return;
    }
    snapshot_.clear();
    std::int64_t available = 0;
    for (std::int32_t resting = head; resting != 0;
         resting = static_cast<std::int32_t>(slab_.hot(resting).next)) {
      snapshot_.push_back(resting);
      available += slab_.hot(resting).displayed;
    }
    if (available == 0) {
      return;
    }
    const std::int64_t wanted = std::min(taker.remaining, available);
    for (const std::int32_t resting : snapshot_) {
      if (taker.remaining == 0) {
        break;
      }
      const std::int64_t displayed = slab_.hot(resting).displayed;
      const std::int64_t share = wanted * displayed / available / lotSize_ * lotSize_;
      const std::int64_t quantity = std::min(std::min(share, displayed), taker.remaining);
      if (quantity > 0) {
        takeExactly(taker, resting, quantity);
      }
    }
    // Rounding leaves a remainder, and arrival order decides it.
    while (taker.remaining > 0) {
      const std::int32_t next = book_.nextToTake(taker.side, taker.limitRank);
      if (next == 0 || slab_.hot(next).tick != tick) {
        return;
      }
      takeExactly(taker, next, std::min(taker.remaining, slab_.hot(next).displayed));
    }
  }

  // What becomes of whatever the walk left: the book, a removal, or nothing at all.
  void settle(Taker& taker) {
    if (taker.remaining == 0) {
      return;
    }
    if (taker.pricing == LIMIT && taker.timeInForce <= DAY) {
      const std::int32_t slot = place(taker, ++counters_.arrival, 0);
      book_.add(taker.side, slot);
      feed_.rested(taker.id, book_.priceOfTick(taker.tick), slab_.hot(slot).displayed, taker.side);
      reportIndicative();
      return;
    }
    feed_.removed(taker.id, taker.remaining, sbe::RemoveReason::IMMEDIATE_OR_CANCEL_REMAINDER);
  }

  // The taker's one write into the slab: what it shows and where it stands are settled now, so an
  // order that crossed on the way in queues behind everything that joined while it was walking.
  std::int32_t place(const Taker& taker, const std::int64_t arrival,
                     const std::int64_t triggerPrice) {
    const std::int32_t slot = slab_.acquire();
    Slab::Hot& hot = slab_.hot(slot);
    hot.remaining = taker.remaining;
    hot.displayed =
        taker.displaySize == 0 ? taker.remaining : std::min(taker.displaySize, taker.remaining);
    hot.executed = taker.executed;
    hot.id = taker.id;
    hot.arrival = arrival;
    hot.smpId = taker.smpId;
    hot.tick = taker.tick;
    Slab::Cold& cold = slab_.cold(slot);
    cold.clientOrderId = taker.clientOrderId;
    cold.minQuantity = taker.minQuantity;
    cold.triggerPrice = triggerPrice;
    cold.displaySize = taker.displaySize;
    cold.participantId = taker.participantId;
    cold.side = taker.side;
    cold.pricing = taker.pricing;
    cold.timeInForce = taker.timeInForce;
    cold.postOnly = taker.postOnly ? 1 : 0;
    return slot;
  }

  // Triggers ------------------------------------------------------------------------------------

  // A cascade runs to completion before the next command is applied.
  void fireTriggers() {
    if (lastExecuted_ == 0) {
      return;
    }
    triggers_.fire(lastExecuted_, pending_);
    while (pendingNext_ < pending_.size()) {
      const std::int32_t fired = pending_[pendingNext_++];
      const Slab::Hot& hot = slab_.hot(fired);
      const Slab::Cold& cold = slab_.cold(fired);
      feed_.triggered(hot.id);
      // A fired stop becomes an ordinary order of its own pricing, in registers like any taker.
      Taker taker;
      taker.id = hot.id;
      taker.clientOrderId = cold.clientOrderId;
      taker.participantId = cold.participantId;
      taker.smpId = hot.smpId;
      taker.remaining = hot.remaining;
      taker.executed = hot.executed;
      taker.minQuantity = cold.minQuantity;
      taker.displaySize = cold.displaySize;
      taker.tick = hot.tick;
      taker.side = cold.side;
      taker.pricing = cold.pricing;
      taker.timeInForce = cold.timeInForce;
      taker.postOnly = cold.postOnly != 0;
      taker.limitRank = limitRankOf(taker);
      slab_.release(fired);
      if (state_ == CONTINUOUS) {
        match(taker);
      }
      settle(taker);
      triggers_.fire(lastExecuted_, pending_);
    }
    pending_.clear();
    pendingNext_ = 0;
  }

  // Auctions ------------------------------------------------------------------------------------

  void uncross() {
    auctionUncross();
    if (auctionQuantity_ == 0) {
      return;
    }
    const std::int64_t price = auctionPrice_;
    std::int64_t left = auctionQuantity_;
    willing(0, price, buys_);
    willing(1, price, sells_);
    std::size_t sell = 0;
    for (const std::int32_t buy : buys_) {
      // A filled order's slot goes back to the free list inside cross, and nothing reacquires it
      // before these reads, so its quantities still say what they said when it died.
      while (slab_.hot(buy).remaining > 0 && left > 0 && sell < sells_.size()) {
        const std::int32_t resting = sells_[sell];
        left -= cross(buy, resting, price, left);
        if (slab_.hot(resting).remaining == 0) {
          sell++;
        }
      }
    }
    reference_ = price;
    lastExecuted_ = price;
    fireTriggers();
  }

  std::int64_t cross(const std::int32_t buy, const std::int32_t sell, const std::int64_t price,
                     const std::int64_t left) {
    const std::int64_t quantity =
        std::min(std::min(slab_.hot(buy).displayed, slab_.hot(sell).displayed), left);
    const std::int64_t buyShown = slab_.hot(buy).displayed;
    const std::int64_t sellShown = slab_.hot(sell).displayed;
    take(buy, quantity);
    take(sell, quantity);
    feed_.executed(counters_.nextExecutionId++, slab_.hot(buy).id, slab_.hot(sell).id, price,
                   quantity);
    reveal(buy, 0, buyShown, quantity);
    reveal(sell, 1, sellShown, quantity);
    return quantity;
  }

  void take(const std::int32_t slot, const std::int64_t quantity) {
    Slab::Hot& hot = slab_.hot(slot);
    hot.remaining -= quantity;
    hot.executed += quantity;
    hot.displayed -= quantity;
  }

  // Hidden quantity is displayed before it executes, in an auction as in continuous trading.
  void reveal(const std::int32_t slot, const std::int32_t side, const std::int64_t shownBefore,
              const std::int64_t quantity) {
    Slab::Hot& hot = slab_.hot(slot);
    if (hot.remaining == 0) {
      book_.quantitiesChanged(side, slot, -shownBefore, -quantity);
      book_.remove(side, slot);
      slab_.release(slot);
      return;
    }
    if (hot.displayed == 0) {
      const std::int64_t size = slab_.cold(slot).displaySize;
      hot.displayed = size == 0 ? hot.remaining : std::min(size, hot.remaining);
      hot.arrival = ++counters_.arrival;
      book_.requeued(side, slot, hot.displayed - shownBefore, -quantity);
      feed_.rested(hot.id, book_.priceOfTick(hot.tick), hot.displayed, side);
      return;
    }
    book_.quantitiesChanged(side, slot, hot.displayed - shownBefore, -quantity);
  }

  // Everyone on one side willing at the price, earliest first, into the caller's space.
  void willing(const std::int32_t side, const std::int64_t price, std::vector<std::int32_t>& into) {
    into.clear();
    const std::int32_t limit = book_.willingLimitRank(side, price);
    for (std::int32_t rank = book_.firstRank(side); rank <= limit;
         rank = book_.rankAfter(side, rank)) {
      for (std::int32_t slot = book_.headAtRank(side, rank); slot != 0;
           slot = static_cast<std::int32_t>(slab_.hot(slot).next)) {
        into.push_back(slot);
      }
    }
    sortByArrival(into);
  }

  // Reported whenever it changes, and only while there is an auction to report on.
  void reportIndicative() {
    if (!callPhase(state_)) {
      return;
    }
    auctionUncross();
    if (auctionPrice_ == indicativePrice_ && auctionQuantity_ == indicativeQuantity_) {
      return;
    }
    indicativePrice_ = auctionPrice_;
    indicativeQuantity_ = auctionQuantity_;
    feed_.indicative(indicativePrice_, indicativeQuantity_);
  }

  // Every occupied price is a candidate, priced by prefix sums over the cached remaining totals,
  // so no order is visited however often the indicative is recomputed. Hidden quantity counts:
  // an iceberg's concealed part is real liquidity and a price that ignored it would leave the
  // book crossed.
  void auctionUncross() {
    have_ = false;
    consider(0);
    consider(1);
    auctionPrice_ = have_ ? bestPrice_ : 0;
    auctionQuantity_ = have_ ? bestTradeable_ : 0;
  }

  void consider(const std::int32_t side) {
    for (std::int32_t rank = book_.firstRank(side); rank != Ladder::EMPTY;
         rank = book_.rankAfter(side, rank)) {
      const std::int64_t candidate = book_.priceOfRank(side, rank);
      const std::int64_t demand = quantityWilling(0, candidate);
      const std::int64_t supply = quantityWilling(1, candidate);
      const std::int64_t tradeable = std::min(demand, supply);
      if (tradeable == 0) {
        continue;
      }
      const std::int64_t surplus = std::abs(demand - supply);
      const std::int32_t pressure = demand == supply ? -1 : demand > supply ? 0 : 1;
      if (!have_ || better(candidate, tradeable, surplus, pressure)) {
        have_ = true;
        bestPrice_ = candidate;
        bestTradeable_ = tradeable;
        bestSurplus_ = surplus;
        bestPressure_ = pressure;
      }
    }
  }

  // Which of two candidates the venue chooses: volume first, then the smaller surplus, then the
  // side the surplus is on (unfilled demand settles high, unfilled supply settles low), then
  // proximity to the reference price, and finally the higher price, so nothing is left to the
  // order the candidates were walked in.
  bool better(const std::int64_t candidate, const std::int64_t tradeable,
              const std::int64_t surplus, const std::int32_t pressure) const {
    if (tradeable != bestTradeable_) {
      return tradeable > bestTradeable_;
    }
    if (surplus != bestSurplus_) {
      return surplus < bestSurplus_;
    }
    if (pressure == bestPressure_ && pressure == 0) {
      return candidate > bestPrice_;
    }
    if (pressure == bestPressure_ && pressure == 1) {
      return candidate < bestPrice_;
    }
    const std::int64_t distance = std::abs(candidate - reference_);
    const std::int64_t bestDistance = std::abs(bestPrice_ - reference_);
    if (distance != bestDistance) {
      return distance < bestDistance;
    }
    return candidate > bestPrice_;
  }

  std::int64_t quantityWilling(const std::int32_t side, const std::int64_t candidate) const {
    const std::int32_t limit = book_.willingLimitRank(side, candidate);
    std::int64_t total = 0;
    for (std::int32_t rank = book_.firstRank(side); rank <= limit;
         rank = book_.rankAfter(side, rank)) {
      total += book_.remainingAtRank(side, rank);
    }
    return total;
  }

  // Small shared machinery ------------------------------------------------------------------

  std::int32_t limitRankOf(const Taker& taker) const {
    return taker.pricing == MARKET ? book_.marketLimit() : book_.rankOf(taker.side ^ 1, taker.tick);
  }

  // Earliest first, which is every tie-break and every report order in the venue.
  void sortByArrival(std::vector<std::int32_t>& slots) const {
    std::sort(slots.begin(), slots.end(),
              [this](const std::int32_t left, const std::int32_t right) {
                return slab_.hot(left).arrival < slab_.hot(right).arrival;
              });
  }

  // Validation ----------------------------------------------------------------------------------
  // The precedence is docs/PROTOCOL.md's, cheapest checks first, and a refusal is encoded below
  // zero so one verdict carries either the reason or the tick the ladder wants.

  static constexpr std::int64_t refusal(const sbe::RejectReason::Value reason) {
    return -1 - static_cast<std::int64_t>(reason);
  }

  static sbe::RejectReason::Value reasonOf(const std::int64_t verdict) {
    return static_cast<sbe::RejectReason::Value>(-1 - verdict);
  }

  std::int64_t refusalOrTick(const Taker& taker, const std::int64_t price,
                             const std::int64_t triggerPrice) const {
    if (state_ == CLOSED) {
      return refusal(sbe::RejectReason::STATE_NOT_PERMITTED);
    }
    if (taker.remaining <= 0) {
      return refusal(sbe::RejectReason::NON_POSITIVE_QUANTITY);
    }
    if (lotSize_ != 1 && taker.remaining % lotSize_ != 0) {
      return refusal(sbe::RejectReason::LOT_VIOLATION);
    }
    if (taker.minQuantity > taker.remaining) {
      return refusal(sbe::RejectReason::MINIMUM_QUANTITY_ABOVE_ORDER);
    }
    if (taker.displaySize > taker.remaining) {
      return refusal(sbe::RejectReason::DISPLAY_QUANTITY_ABOVE_ORDER);
    }
    if (inconsistent(taker)) {
      return refusal(sbe::RejectReason::INVALID_FIELDS);
    }
    std::int64_t tick = 0;
    if (taker.pricing == LIMIT) {
      tick = tickOrRefusal(price);
      if (tick < 0) {
        return tick;
      }
      if (std::abs(price - reference_) > bandWidth_) {
        return refusal(sbe::RejectReason::DYNAMIC_BAND_VIOLATION);
      }
    }
    if (triggerPrice != 0) {
      // A stop is placed away from where the market is, so the dynamic band does not apply.
      const std::int64_t triggerTick = tickOrRefusal(triggerPrice);
      if (triggerTick < 0) {
        return triggerTick;
      }
    }
    const std::int32_t fromTheBook = refusalFromTheBook(taker, triggerPrice, tick);
    return fromTheBook >= 0 ? refusal(static_cast<sbe::RejectReason::Value>(fromTheBook)) : tick;
  }

  // The price's tick index, or the refusal that keeps it off the ladder, in one division: the
  // quotient that proves the price on tick is the index the ladder wants.
  std::int64_t tickOrRefusal(const std::int64_t price) const {
    if (price <= 0) {
      return refusal(sbe::RejectReason::NON_POSITIVE_PRICE);
    }
    const std::int64_t ticks = price / tickSize_;
    if (ticks * tickSize_ != price) {
      return refusal(sbe::RejectReason::TICK_VIOLATION);
    }
    if (price < minPrice_ || price > maxPrice_) {
      return refusal(sbe::RejectReason::STATIC_BAND_VIOLATION);
    }
    return ticks - baseTick_;
  }

  // Combinations that contradict themselves: a market order told to rest, and a post-only order
  // told never to rest, are each an instruction that cannot be followed.
  static bool inconsistent(const Taker& taker) {
    if (taker.pricing == MARKET) {
      return taker.postOnly || taker.timeInForce <= DAY;
    }
    return taker.postOnly && taker.timeInForce >= IOC;
  }

  // The refusals that have to ask the book first. Negative means nothing is wrong, so the reason
  // values, which start at zero, pass through undisturbed.
  std::int32_t refusalFromTheBook(const Taker& taker, const std::int64_t triggerPrice,
                                  const std::int64_t tick) const {
    if (triggerPrice != 0 || state_ != CONTINUOUS) {
      // A stop is not going near the book yet, and outside continuous trading nothing executes on
      // entry, so a fill-or-kill or a minimum quantity cannot be satisfied.
      if (triggerPrice == 0 && taker.timeInForce == FOK) {
        return sbe::RejectReason::FILL_OR_KILL_UNFILLABLE;
      }
      if (triggerPrice == 0 && taker.minQuantity > 0) {
        return sbe::RejectReason::MINIMUM_QUANTITY_NOT_MET;
      }
      return -1;
    }
    const std::int32_t limitRank =
        taker.pricing == MARKET ? book_.marketLimit()
                                : book_.rankOf(taker.side ^ 1, static_cast<std::int32_t>(tick));
    if (taker.postOnly && book_.nextToTake(taker.side, limitRank) != 0) {
      return sbe::RejectReason::WOULD_CROSS;
    }
    if (taker.timeInForce == FOK || taker.minQuantity > 0) {
      const std::int64_t fillable = book_.fillable(taker.side, limitRank, taker.smpId);
      if (taker.timeInForce == FOK && fillable < taker.remaining) {
        return sbe::RejectReason::FILL_OR_KILL_UNFILLABLE;
      }
      if (taker.minQuantity > 0 && fillable < taker.minQuantity) {
        return sbe::RejectReason::MINIMUM_QUANTITY_NOT_MET;
      }
    }
    return -1;
  }

  std::int64_t refusalOrTickForReplace(const std::int32_t resting, const std::int64_t quantity,
                                       const std::int64_t price) const {
    if (quantity <= 0) {
      return refusal(sbe::RejectReason::NON_POSITIVE_QUANTITY);
    }
    if (quantity <= slab_.hot(resting).executed) {
      // Nothing can un-trade what has traded.
      return refusal(sbe::RejectReason::QUANTITY_BELOW_EXECUTED);
    }
    if (lotSize_ != 1 && quantity % lotSize_ != 0) {
      return refusal(sbe::RejectReason::LOT_VIOLATION);
    }
    const std::int64_t tick = tickOrRefusal(price);
    if (tick < 0) {
      return tick;
    }
    if (std::abs(price - reference_) > bandWidth_) {
      return refusal(sbe::RejectReason::DYNAMIC_BAND_VIOLATION);
    }
    const std::int32_t side = slab_.cold(resting).side;
    if (slab_.cold(resting).postOnly != 0 && state_ == CONTINUOUS &&
        book_.nextToTake(side, book_.rankOf(side ^ 1, static_cast<std::int32_t>(tick))) != 0) {
      // A replace refused by a liquidity flag leaves the original order resting.
      return refusal(sbe::RejectReason::WOULD_CROSS);
    }
    return tick;
  }

  Slab& slab_;
  Feed<Ring>& feed_;
  Counters& counters_;
  Book book_;
  Triggers triggers_;

  // The working space the large commands keep between them (P-6). The fired queue is drained by
  // index, so a cascade appends behind the cursor without shuffling anything.
  std::vector<std::int32_t> pending_;
  std::size_t pendingNext_ = 0;
  std::vector<std::int32_t> snapshot_;
  std::vector<std::int32_t> gathered_;
  std::vector<std::int32_t> buys_;
  std::vector<std::int32_t> sells_;

  std::int64_t tickSize_;
  std::int64_t lotSize_;
  std::int64_t minPrice_;
  std::int64_t maxPrice_;
  std::int64_t bandWidth_;
  std::int64_t baseTick_;
  std::int64_t reference_;
  std::int64_t lastExecuted_ = 0;
  bool proRata_;
  std::int32_t state_ = PRE_OPEN;

  std::int64_t indicativePrice_ = 0;
  std::int64_t indicativeQuantity_ = 0;

  // The auction's tally: a handful of fields, recomputed whole each time it is asked.
  bool have_ = false;
  std::int64_t auctionPrice_ = 0;
  std::int64_t auctionQuantity_ = 0;
  std::int64_t bestPrice_ = 0;
  std::int64_t bestTradeable_ = 0;
  std::int64_t bestSurplus_ = 0;
  std::int32_t bestPressure_ = -1;
};

}  // namespace exchange::matcher
