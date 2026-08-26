# Component: pre-trade risk

The controls that stand between a client's message and the sequence, because a bad order that
reaches the book is already a trade. The citable driver is the market access rule (SEC Rule
15c3-5, 2010), which is why every real venue and broker runs this layer and why it runs inline
with a nanosecond budget rather than as an afterthought.

## The design

The checks sit in the gateway, after translation and before the command is forwarded to the
sequencer, so a refused order never consumes a place in the global order. The set follows what
venues actually run: price collars against a reference (fat-finger protection, distinct from the
matcher's banding because it fires before sequencing and on the client's own terms), maximum
order and notional size, message rate throttles per session, duplicate-order heuristics, and
credit: a running position and open-order exposure per participant, maintained by consuming the
venue's own event stream, which keeps the gateway stateless about anything the sequence does not
say (P-1). Refusals are typed and reported on the session, in the same failure-is-a-value shape
the matcher uses (P-10).

The depth questions are consistency and cost. Exposure maintained from the event stream lags the
orders in flight by the round trip, so the layer tracks its own in-flight reservations and
reconciles them against acknowledgments, which is a small, sharp lesson in eventual consistency
on a latency budget. And every check is on the client's critical path, so the layer is measured
the way the matcher is, with the budget stated and held.

## How it lands

Protocol additions for the risk refusal reasons; the checks with a corpus of refused and admitted
cases; the exposure tracker held to the event stream by invariants; throttle behaviour under
generated bursts; and the measured cost per admitted order.

## Where to learn

- SEC Rule 15c3-5 (Risk Management Controls for Brokers or Dealers with Market Access, 2010). The
  regulatory floor, short enough to read in full.
- CME Globex credit controls and risk management documentation (public). What an exchange-side
  implementation exposes to firms.
- Larry Harris, Trading and Exchanges, 2003, on principal-agent problems and error trades. Why
  these controls exist beyond the regulation.
