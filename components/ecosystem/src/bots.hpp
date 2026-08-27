// The participants that make the venue trade (docs/components/ecosystem.md). A market maker in
// the shape of Avellaneda and Stoikov 2008: a reservation price skewed off the mid by inventory,
// quotes a half spread either side of it, and hard inventory limits, which is what a dealer is
// paid for (Harris 2003). A noise taker arriving randomly, because real flow mostly is. A
// momentum taker that chases runs of prints, because bursts are what make the tails interesting.
// Every bot is a pure function of the feed it reads, its own fills, and a seeded generator, so a
// simulated day driven twice is the same day, and the venue's determinism claim extends over the
// people trading on it.

#pragma once

#include <cstdint>

#include "feedhandler.hpp"

namespace exchange::ecosystem {

// xorshift64*, seeded and byte-stable across platforms, which the standard distributions are not.
class Rng {
 public:
  explicit Rng(const std::uint64_t seed) : state_(seed == 0 ? 0x9E3779B97F4A7C15ULL : seed) {}

  std::uint64_t next() {
    state_ ^= state_ << 13;
    state_ ^= state_ >> 7;
    state_ ^= state_ << 17;
    return state_ * 0x2545F4914F6CDD1DULL;
  }

  // Uniform in [0, bound), by rejection so small bounds carry no modulo bias worth arguing over.
  std::uint64_t below(const std::uint64_t bound) { return bound == 0 ? 0 : next() % bound; }

 private:
  std::uint64_t state_;
};

// The dealer: quotes both sides of a reservation price that leans away from its inventory, so
// the book pays it to mean revert. Quotes are replaced when the target moves and left standing
// when it does not, which keeps queue priority when nothing changed.
template <typename Feed, typename Entry>
class Maker {
 public:
  struct Config {
    std::uint32_t instrumentId = 1;
    // The fair value quoted around before the market prints one of its own.
    std::int64_t fairValue = 100'000;
    std::int64_t tick = 5;
    std::int64_t halfSpread = 10;
    // Price units the reservation leans per lot of inventory: gamma, flattened to a constant.
    std::int64_t skewPerLot = 2;
    std::int64_t quoteSize = 10;
    std::int64_t inventoryLimit = 100;
  };

  Maker(Feed& feed, Entry& entry, const Config config)
      : feed_(feed), entry_(entry), config_(config) {}

  void onTick() {
    if (!entry_.established() || !tradable()) {
      return;
    }
    const std::int64_t inventory = entry_.positionOf(config_.instrumentId);
    const std::int64_t reservation = fair() - inventory * config_.skewPerLot;
    // The receding side moves first and each side clamps against the other's standing quote, so
    // the maker never crosses itself on the way to a new reservation.
    const bool rising = reservation >= lastReservation_;
    lastReservation_ = reservation;
    const auto quoteBid = [&] {
      std::int64_t bid = rounded(reservation - config_.halfSpread);
      if (askId_ != 0 && entry_.liveOf(askId_) && bid >= askPrice_) {
        bid = askPrice_ - config_.tick;
      }
      quote(bidId_, bidPrice_, bid, inventory < config_.inventoryLimit, sbe::Side::BUY);
    };
    const auto quoteAsk = [&] {
      std::int64_t ask = roundedUp(reservation + config_.halfSpread);
      if (bidId_ != 0 && entry_.liveOf(bidId_) && ask <= bidPrice_) {
        ask = bidPrice_ + config_.tick;
      }
      quote(askId_, askPrice_, ask, inventory > -config_.inventoryLimit, sbe::Side::SELL);
    };
    if (rising) {
      quoteAsk();
      quoteBid();
    } else {
      quoteBid();
      quoteAsk();
    }
  }

  std::uint64_t quotesPlaced() const { return placed_; }
  std::uint64_t quotesMoved() const { return moved_; }

 private:
  bool tradable() const {
    return feed_.stateOf(config_.instrumentId) == sbe::SessionState::CONTINUOUS;
  }

  // The anchor must be exogenous to the maker's own quoting: skewing off a mid this maker wrote
  // itself compounds the lean every tick and walks away from the market. The last print is the
  // market's own word; flow that leans one way moves it, and balanced flow holds it still.
  std::int64_t fair() const {
    const LastTrade last = feed_.lastTradeOf(config_.instrumentId);
    return last.count > 0 ? last.price : config_.fairValue;
  }

  std::int64_t rounded(const std::int64_t price) const { return price - (price % config_.tick); }
  std::int64_t roundedUp(const std::int64_t price) const {
    const std::int64_t down = rounded(price);
    return down == price ? price : down + config_.tick;
  }

  void quote(std::uint64_t& standingId, std::int64_t& standingPrice, const std::int64_t price,
             const bool wanted, const sbe::Side::Value side) {
    const bool alive = standingId != 0 && entry_.liveOf(standingId);
    if (!wanted) {
      if (alive) {
        entry_.cancel(standingId, config_.instrumentId);
        standingId = 0;
      }
      return;
    }
    if (alive && standingPrice == price) {
      return;
    }
    if (alive) {
      entry_.replace(standingId, config_.instrumentId, config_.quoteSize, price);
      moved_++;
    } else {
      standingId = entry_.newOrder(config_.instrumentId, side, sbe::Pricing::LIMIT,
                                   sbe::TimeInForce::GOOD_TILL_CANCEL, price, config_.quoteSize);
      placed_++;
    }
    standingPrice = price;
  }

  Feed& feed_;
  Entry& entry_;
  Config config_;
  std::uint64_t bidId_ = 0;
  std::uint64_t askId_ = 0;
  std::int64_t bidPrice_ = 0;
  std::int64_t askPrice_ = 0;
  std::int64_t lastReservation_ = 0;
  std::uint64_t placed_ = 0;
  std::uint64_t moved_ = 0;
};

// The crowd: arrives at random, crosses the spread, and leaves. Rate is one arrival per
// oneInEvery ticks in expectation.
template <typename Feed, typename Entry>
class NoiseTaker {
 public:
  struct Config {
    std::uint32_t instrumentId = 1;
    std::uint64_t oneInEvery = 8;
    std::int64_t maxSize = 5;
  };

  NoiseTaker(Feed& feed, Entry& entry, const Config config, const std::uint64_t seed)
      : feed_(feed), entry_(entry), config_(config), rng_(seed) {}

  void onTick() {
    if (!entry_.established() ||
        feed_.stateOf(config_.instrumentId) != sbe::SessionState::CONTINUOUS ||
        rng_.below(config_.oneInEvery) != 0) {
      return;
    }
    const Touch touch = feed_.touchOf(config_.instrumentId);
    const bool buys = rng_.below(2) == 0;
    const std::int64_t size =
        1 + static_cast<std::int64_t>(rng_.below(static_cast<std::uint64_t>(config_.maxSize)));
    if (buys && touch.askQuantity > 0) {
      entry_.newOrder(config_.instrumentId, sbe::Side::BUY, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::IMMEDIATE_OR_CANCEL, touch.askPrice, size);
      fired_++;
    } else if (!buys && touch.bidQuantity > 0) {
      entry_.newOrder(config_.instrumentId, sbe::Side::SELL, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::IMMEDIATE_OR_CANCEL, touch.bidPrice, size);
      fired_++;
    }
  }

  std::uint64_t fired() const { return fired_; }

 private:
  Feed& feed_;
  Entry& entry_;
  Config config_;
  Rng rng_;
  std::uint64_t fired_ = 0;
};

// The chaser: watches the prints, and a run of them in one direction is what it buys into,
// which is how bursts happen for real.
template <typename Feed, typename Entry>
class MomentumTaker {
 public:
  struct Config {
    std::uint32_t instrumentId = 1;
    std::uint64_t streakToFire = 3;
    std::int64_t size = 3;
    std::uint64_t cooldownTicks = 16;
  };

  MomentumTaker(Feed& feed, Entry& entry, const Config config)
      : feed_(feed), entry_(entry), config_(config) {}

  void onTick() {
    if (cooling_ > 0) {
      cooling_--;
    }
    if (!entry_.established() ||
        feed_.stateOf(config_.instrumentId) != sbe::SessionState::CONTINUOUS) {
      return;
    }
    const LastTrade last = feed_.lastTradeOf(config_.instrumentId);
    if (last.count == seenTrades_) {
      return;
    }
    seenTrades_ = last.count;
    if (lastPrice_ != 0 && last.price != lastPrice_) {
      const bool up = last.price > lastPrice_;
      streak_ = (streak_ != 0 && up == upward_) ? streak_ + 1 : 1;
      upward_ = up;
    }
    lastPrice_ = last.price;
    if (streak_ < config_.streakToFire || cooling_ > 0) {
      return;
    }
    const Touch touch = feed_.touchOf(config_.instrumentId);
    if (upward_ && touch.askQuantity > 0) {
      entry_.newOrder(config_.instrumentId, sbe::Side::BUY, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::IMMEDIATE_OR_CANCEL, touch.askPrice, config_.size);
    } else if (!upward_ && touch.bidQuantity > 0) {
      entry_.newOrder(config_.instrumentId, sbe::Side::SELL, sbe::Pricing::LIMIT,
                      sbe::TimeInForce::IMMEDIATE_OR_CANCEL, touch.bidPrice, config_.size);
    } else {
      return;
    }
    fired_++;
    streak_ = 0;
    cooling_ = config_.cooldownTicks;
  }

  std::uint64_t fired() const { return fired_; }

 private:
  Feed& feed_;
  Entry& entry_;
  Config config_;
  std::uint64_t seenTrades_ = 0;
  std::int64_t lastPrice_ = 0;
  std::uint64_t streak_ = 0;
  bool upward_ = false;
  std::uint64_t cooling_ = 0;
  std::uint64_t fired_ = 0;
};

}  // namespace exchange::ecosystem
