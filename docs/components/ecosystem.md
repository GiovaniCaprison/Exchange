# Component: the ecosystem

The venue becomes a market when someone trades on it. Real exchanges do not build their own
participants; this project does, because the participant's seat is half the education: the feed
handler, the order entry client, and the strategies are what most trading technology careers
actually build, and running them against your own venue closes the loop.

## The design

A feed handler client consumes the public feed, A/B arbitrated with gap recovery and
snapshot-then-join, and maintains books, which is the consumer library's packet source doing its
real job. An order entry client speaks the gateway's session protocol with the reconnect and
replay semantics exercised for real. On top of both sit bots: a market maker quoting both sides
around a fair value with inventory limits (the canonical starting model is Avellaneda and
Stoikov 2008, high-frequency trading in a limit order book), noise takers arriving randomly, and
a momentum taker to create the bursts that make the tails interesting. A day simulator starts
the whole venue, gateways, sequencer, matchers, feed, operations, and N bots, runs a scheduled
session from open cross to close cross, and writes every journal and every measurement.

The campaign this enables is the venue's headline: wire to wire, client order entry in to public
feed packet out, decomposed hop by hop with the timestamp chain, on the metal box, under flow
that reacts to the book because the bots do. Every earlier component measured itself; this is
where the venue is measured as a venue.

## How it lands

The clients on the shared consumer library; the bots with deterministic seeds so simulated days
replay; the simulator harness; and the wire-to-wire campaign with its manifest discipline.

## Where to learn

- Avellaneda and Stoikov, High-frequency trading in a limit order book, Quantitative Finance,
  2008. The market-making model everyone starts from.
- Cartea, Jaimungal and Penalva, Algorithmic and High-Frequency Trading, Cambridge, 2015. The
  broader strategy mathematics, for when the bots deserve better brains.
- Larry Harris, Trading and Exchanges, 2003, on dealers and liquidity supply. What the market
  maker is actually paid for.
- Gil Tene, How NOT to Measure Latency, and HdrHistogram. The campaign's discipline, again,
  because wire-to-wire is where coordinated omission does its best hiding.
