# Protocol

What crosses a process boundary. One SBE schema in `schema/` owns every message here (FIX Trading
Community, Simple Binary Encoding specification); this document is the contract the schema
implements and the semantics the matcher answers to. The command vocabulary follows the
conventions of OUCH (Nasdaq, public specification) and iLink 3 (CME Group, public documentation);
the event stream follows the convention of TotalView-ITCH (Nasdaq, public specification), extended
with the attribution fields a private stream needs.

## Framing and alignment

Every message begins with the standard SBE header: blockLength u16, templateId u16, schemaId u16,
version u16, eight bytes. Rings, the journal and every other carrier place messages at eight byte
boundaries, and the field layouts below keep every 64 bit field on an eight byte boundary from
there. Fields are ordered for decode cost and alignment rather than for prose: wider fields first,
the fields a consumer reads earliest nearest the front.

## The command context

Every command opens with the same 24 byte composite, written by the sequencer:

| Field | Type | Meaning |
|---|---|---|
| sequence | u64 | the command's place in the global order, assigned exactly once |
| timestamp | u64 | nanoseconds since the epoch, stamped by the sequencer at sequencing (P-3) |
| instrumentId | u32 | which book the command addresses |
| reserved | u32 | alignment padding, always zero |

The matcher trusts the context (P-9's boundary sits above it): sequence is gap free within the
partition's subscription, the timestamp is the venue's one clock reading for this command, and the
instrument routes to a book this partition owns. A gateway submits the command with these two
fields zeroed, and the sequencer writes them in place at sequencing, which is why they sit at the
context's front: the submission plane below names the mechanism.

## The event context

Every event opens with the same 32 byte composite, written by the matcher:

| Field | Type | Meaning |
|---|---|---|
| sequence | u64 | the event's place in this partition's event stream, gap free from 1 |
| inputSequence | u64 | the sequence of the command this event answers |
| timestamp | u64 | the answered command's timestamp, carried unchanged (P-3) |
| instrumentId | u32 | which book the event describes |
| reserved | u32 | alignment padding, always zero |

Carrying the input sequence prices eight bytes and a store per event, and buys attribution:
execution reports, drop copy and surveillance all need to know which command an event answers, and
a gateway acknowledges its clients by matching input sequences. The public feed derivation strips
the field, so the public convention stays ITCH-shaped. Carrying the timestamp rather than stamping
again is what keeps replay byte exact (P-2, P-3).

The event sequence is also the continuity rule: a consumer skips any event whose sequence it has
covered, wherever it arrives. Two matchers fed the same commands emit byte-identical streams, so
their two rings are the event stream's A and B, a seat over both hears one stream, and a twin
dying, or a consumer re-seating after one died, changes nothing delivered.

## Commands

| Message | Body after the context | Notes |
|---|---|---|
| InstrumentDefinition | tickSize i64, lotSize i64, minPrice i64, maxPrice i64, bandWidth i64, openingReference i64, priceScale u8, allocation Allocation | precedes every other command for its instrument; trusted reference data |
| NewOrder | clientOrderId u64, price i64, quantity i64, minQuantity i64, displayQuantity i64, triggerPrice i64, smpId u64, participantId u32, side Side, pricing Pricing, timeInForce TimeInForce, flags OrderFlags | the four qualifiers are absent when zero |
| CancelOrder | clientOrderId u64, participantId u32 | names the order by the id its owner gave it |
| ReplaceOrder | clientOrderId u64, quantity i64, price i64, participantId u32 | carries full intent rather than a delta |
| MassCancel | clientOrderId u64, participantId u32 | removes everything the participant has resting or waiting; instrument zero in its context sweeps every book |
| SessionControl | state SessionState | the trading state moves on this command and on nothing else |

Commands name orders by the pair (participantId, clientOrderId), unique per participant for the
session and the sender's to keep unique. A client can cancel the moment it decides to, without
waiting to learn any engine assigned id, and a recorded command stream replays without knowing
what any engine did with it.

## Events

| Message | Body after the context | Meaning |
|---|---|---|
| OrderAccepted | orderId u64, clientOrderId u64, participantId u32 | admitted, with its engine order id |
| OrderRejected | clientOrderId u64, participantId u32, reason RejectReason | refused, and no state changed |
| OrderRested | orderId u64, price i64, quantity i64, side Side | entered the book; quantity is displayed quantity only |
| OrderExecuted | executionId u64, aggressorOrderId u64, restingOrderId u64, price i64, quantity i64 | one execution, at the resting order's price |
| OrderReduced | orderId u64, quantity i64 | new displayed quantity, queue position kept |
| OrderRemoved | orderId u64, quantity i64, reason RemoveReason | left the book, with the quantity removed |
| OrderTriggered | orderId u64 | a stop's condition was met and it left the trigger book |
| SessionStateChanged | state SessionState | the state now in effect |
| AuctionIndicative | price i64, quantity i64 | uncrossing price and volume if the auction ran now |

Hidden quantity is never reported: what rests, reduces and removes is displayed quantity, and a
consumer's book is the visible book. Engine order ids and execution ids are assigned from
per-partition counters, unique within the partition for the session. Applying any prefix of the
event stream yields a valid book, so a consumer applies events one at a time and is never asked to
hold one back. An execution reduces whichever of its two orders the consumer's book holds and
removes one it reduces to zero: in continuous trading that is the resting order alone, since an
aggressor is never in the book while it takes, and in an uncrossing it is both, since neither side
aggressed.

## Enumerations

| Enum | Values |
|---|---|
| Side | BUY 0, SELL 1 |
| Pricing | LIMIT 0, MARKET 1, PEGGED 2 |
| TimeInForce | GOOD_TILL_CANCEL 0, DAY 1, IMMEDIATE_OR_CANCEL 2, FILL_OR_KILL 3 |
| OrderFlags | bit 0 postOnly, bit 1 auctionOnly |
| Allocation | PRICE_TIME 0, PRO_RATA 1 |
| SessionState | PRE_OPEN 0, OPENING_AUCTION 1, CONTINUOUS 2, CLOSING_AUCTION 3, HALTED 4, CLOSED 5 |
| RejectReason | NON_POSITIVE_QUANTITY 0, LOT_VIOLATION 1, NON_POSITIVE_PRICE 2, TICK_VIOLATION 3, STATIC_BAND_VIOLATION 4, DYNAMIC_BAND_VIOLATION 5, INVALID_FIELDS 6, MINIMUM_QUANTITY_ABOVE_ORDER 7, DISPLAY_QUANTITY_ABOVE_ORDER 8, MINIMUM_QUANTITY_NOT_MET 9, WOULD_CROSS 10, FILL_OR_KILL_UNFILLABLE 11, STATE_NOT_PERMITTED 12, UNKNOWN_ORDER 13, QUANTITY_BELOW_EXECUTED 14 |
| RemoveReason | CANCELLED 0, REPLACED 1, MASS_CANCELLED 2, IMMEDIATE_OR_CANCEL_REMAINDER 3, SELF_MATCH_PREVENTED 4, EXPIRED 5 |
| AlertKind | WASH_TRADE 0, SPOOFING 1, LAYERING 2 |

## Order semantics

Prices and quantities are scaled integers (P-11); priceScale on the instrument names the implied
decimal places. A limit order's unmatched remainder rests at its price under GOOD_TILL_CANCEL or
DAY, and the close expires everything resting or waiting under DAY with the reason EXPIRED, in
arrival order, while GOOD_TILL_CANCEL stands for tomorrow through the snapshot; an
IMMEDIATE_OR_CANCEL remainder is removed; a FILL_OR_KILL order executes in full on entry
or is refused whole; a market order never rests. A post-only order never takes liquidity and is
refused if it would. An order carrying minQuantity executes at least that much on entry or is
refused without executing. An auction-only order is limit-on-open or limit-on-close: it may only
be entered outside continuous trading, rests until its call, participates in the uncrossing, and
whatever the call did not fill expires with the reason EXPIRED the moment continuous trading
begins, or at the close; it cannot be a market order, an immediacy demand or a stop, because it
exists to rest until its call. A pegged order is a primary peg: it rests at the best displayed
price on its own side and follows it, its reference excluding its own quote so it tracks the
market down instead of ratcheting on itself, with the order's price field as an optional limit
cap it never crosses. Every move is a removal reasoned REPLACED and a fresh rest under the same
id, so queue priority is honestly lost. A peg with no reference parks off the book and rests
again when one appears; a peg lives only in continuous trading, is refused immediacy, triggers,
post-only, minimums and replaces, and expires whenever continuous trading ends.

An order with displayQuantity below quantity is an iceberg: only the displayed tranche rests
visibly, displayed quantity at a price is consumed before hidden, and when a tranche is exhausted
with quantity remaining the next tranche is displayed and joins the back of the queue at its
price. What an order displays is a tranche of what it has left, and a replace preserves the
display size it was entered with.

An order with a non-zero triggerPrice is a stop of whichever pricing it carries. It rests in the
trigger book, is not book liquidity, and fires when the last executed price reaches its trigger in
the direction its side implies; a stop whose trigger the last executed price has already reached
fires at once. A fired stop enters the book as an ordinary order of its pricing, triggers are
evaluated after each command's executions, and a cascade runs to completion before the next
command is applied.

Orders sharing a non-zero smpId never execute against each other: the resting order is removed
with SELF_MATCH_PREVENTED and the walk continues.

## Matching

Resting liquidity is consumed best price first, and an execution happens at the resting order's
price. Within a price, allocation follows the instrument: PRICE_TIME consumes in arrival order;
PRO_RATA apportions in proportion to displayed quantity, rounded down to a whole lot, with the
undistributed remainder allocated in arrival order one lot at a time. An order whose remainder
rests joins the queue behind everything that joined while it was matching, including tranches
other icebergs replenished on the way.

## Amend and cancel

A replace names the order's whole intended quantity, so what should still be working is that
quantity less what has already executed, and a replace naming a quantity at or below what has
executed is refused. A replace lowering quantity at the same price keeps queue position and is
reported as a reduction; any other replace is a removal and a fresh rest, keeping both of the
order's ids. A replace refused for any reason, a liquidity flag included, leaves the original
order resting untouched. A mass cancel removes every resting order and waiting stop the
participant has, reported in arrival order.

## Trading state and auctions

The state moves only on SessionControl. Order entry, replacement and cancellation are legal in
every state except CLOSED; continuous matching happens only in CONTINUOUS; until a state command
arrives the engine is in PRE_OPEN. During a call phase an indicative uncrossing price and volume
are reported whenever they change. Leaving a call phase uncrosses before the new state is
reported: the auction executes all matched quantity at the one price that maximises executable
volume, breaking ties by minimum surplus, then by the side the surplus sits on (unfilled demand
settles high, unfilled supply settles low), then by proximity to the reference price, and finally
by taking the higher price. Hidden quantity counts toward uncrossing volume and is displayed
before it executes, in an auction as in continuous trading. A halt cancels nothing and the book is
intact on resumption.

The reference price is the last price executed in the session, and before the first execution it
is the instrument's openingReference. The dynamic band is bandWidth either side of the reference;
the static band is minPrice to maxPrice; a priced order satisfies both. A trigger price satisfies
tick and the static band and is exempt from the dynamic band, since a stop is placed away from
where the market is.

## Validation and refusal precedence

Validation happens once, before any state is touched (P-9), and the checks run cheapest first.
The precedence is part of this contract: state permission, then quantity positive, on lot,
minQuantity and displayQuantity no larger than quantity, field consistency (a market order cannot
be told to rest, post-only cannot be paired with an immediate time in force), then price positive,
on tick, inside the static band, inside the dynamic band, then trigger price checks, and last the
checks that read the book: post-only crossing, FILL_OR_KILL fillability, minQuantity fillability.
A refusal reports the first failing check's reason and changes nothing.

## The session

A client meets the venue over a session whose shape follows SoupBinTCP and whose messages are
this schema's, the same move iLink 3 made (both public specifications). The carrier is a byte
stream; each message travels as a u16 length prefix and one framed message, the framing ranges
use for their contents. A connection opens with LoginRequest naming the participant, a
credential, and the session sequence the client expects next, zero on a first connection. The
gateway answers LoginAccepted and replays the session's sequenced stream from that point, or
LoginRejected with the reason and closes the connection; asking past the stream's end is
SEQUENCE_AHEAD, because a replay cannot invent what never happened.

Downstream, the session is a sequenced stream in SoupBinTCP's sense: every message the gateway
sends after acceptance, heartbeats aside, occupies the next session sequence implicitly, nothing
carries the number on the wire, and both sides count. The stream's content is the venue's own
event vocabulary filtered to what the session's participant owns, which is what an execution
report is here, so a reconnecting client resumes byte exactly by logging in with the sequence it
reached, the client-side mirror of the venue's rewind. Upstream, the client speaks the command
vocabulary itself, with the context's sequence and timestamp zeroed and participantId zeroed;
the gateway writes the session's participantId in place before forwarding, so identity is the
session's fact and a client cannot speak as anyone else. Client messages are unsequenced in the
session sense: their delivery contract is the path behind the gateway, retry plus deduplication,
rather than session replay, and after a sequencer failover the gateway resubmits everything
unacknowledged under its original numbering, which the leadership section makes harmless.

Heartbeats pulse both ways on an idle interval, and silence past it is a dead peer. LogoutRequest
asks for a clean end; SessionEnded is the gateway's last word on a session either way.

Two session-level risk controls guard the door. Cancel on disconnect, granted per credential, is
the service real venues sell under exactly this name: any unclean session death, a dropped
transport, a heartbeat death, a poisoned stream, makes the gateway submit one venue-wide
MassCancel under the participant's identity, through the same acknowledged carrier as
everything, so the sweep is sequenced, journaled and replayable like any command; an asked-for
logout leaves the books standing. The kill switch is the operator's hand SEC Rule 15c3-5
demands: a killed participant's session ends, its logins answer KILLED, and its books are swept
once, whatever its disconnect setting; reviving it reopens the door and nothing more. Neither
sweep consults the risk gate, because unwinding risk is never refused.

The retained stream behind each session is sized for the session, and a session that outgrows it
ends, politely and alone: the venue does not die of one participant's day, byte-exact replay
cannot be promised once retention stops, and the door answers EXHAUSTED until the next session.
The unclean end sweeps a cancel-on-disconnect participant's books, which is the safe reading of a
participant who can no longer be told anything. Drop copy scopes carry the same posture.

## The gate's risk checks

Before a command is forwarded to the sequencer it passes the gateway's risk gate, so what the
gate refuses never takes a place in the global order; the modelled driver is the market access
rule (SEC Rule 15c3-5, 2010). The checks run cheapest first and the refusal reports the first
failing check's reason as CommandRefused on the session: the message rate throttle, a token
bucket per session; maximum order quantity; maximum notional, price times quantity; a duplicate
check, refusing a clientOrderId that is still alive; the price collar, a configured width around
the instrument's last execution as the gate has seen it on the event stream, unchecked until a
first execution exists; and credit. Cancellations and mass cancellations pass every check but
the throttle, because a venue never blocks the message that reduces risk. The venue's own
validation and its precedence stand unchanged behind the gate.

Credit is a ledger accounted from the admission side, because the event stream shows displayed
quantity and a hidden remainder is exposure all the same: an admitted order reserves its full
notional at its admitted price, a replace re-prices the reservation, executions drain it at the
admitted price as the venue's events arrive, and a rejection or removal releases what remains.
The gate admits while the sum of its ledger and the magnitude of the participant's executed
position stays within the configured credit. Between admission and the venue's answer the ledger
is deliberately ahead of the truth, which is the safe side of eventual consistency; the events
reconcile it, and when every order has closed the ledger drains to zero, which the invariants
hold as conservation.

## The submission plane

A gateway submits commands to the sequencer as a framed GatewaySubmission message, gatewaySequence
u64, gatewayId u32, reserved u32, 24 bytes with its header, followed by one framed command whose
context carries its instrument and zeroes for sequence and timestamp. The
sequencer deduplicates on (gatewayId, gatewaySequence), assigns the next global sequence, stamps
the venue's one timestamp, and writes both into the command's context in place, so the journaled
and published stream is made of ordinary commands and no consumer knows the submission plane
exists. Exactly-once is retry plus deduplication: a gateway that never saw an acknowledgment
resubmits under the same gatewaySequence, and the sequencer's dedupe makes the retry harmless.

The acknowledgment is CommandSequenced: the gateway's own sequence, the global sequence it
received, and the stamped timestamp. It is sent only once the command is durable under the
configured policy, so an acknowledged command survives the sequencer's death.

## Publication and replication ranges

The sequenced stream travels as ranges, on the public carrier and the replication link alike,
each opened by a framed RangeHeader message, firstSequence u64, epoch u32, count u16, reserved
u16, 24 bytes with its header, followed by count framed messages each prefixed by its u16 length.
A count of zero is a heartbeat carrying the next sequence, so silence is distinguishable from
loss; a count of 65535 is end of session. The shape follows MoldUDP64 (Nasdaq, public specification), with the epoch in
place of the session name. A consumer that misses packets asks the rewinder with RewindRequest,
naming a first sequence and a count, and receives ordinary ranges from the journal.

Replication is the same ranges over a private link, each message a whole submission, envelope
and stamped command together, because the standby mirrors the dedupe windows from the envelopes;
its journal stores the command alone, so the two journals stay byte identical. Ranges are
acknowledged by ReplicationAck naming the epoch and the highest contiguous sequence held, one
acknowledgment per range, and the link's empty ranges carry the published watermark, which is the
floor a takeover republishes from. Two durability policies exist and every
measurement's manifest names the one it ran under: safe, where the primary publishes a command
only after the standby's acknowledgment covers it, and local, where publication follows the
journal write alone. Under the safe policy the invariant that failover rests on holds by
construction: published is a subset of replicated, so the standby holds everything any consumer
has ever seen.

On the wire between machines the link is UDP, one range per datagram. Loss and reorder are the
repair conversation's business: the standby's relink parks what arrives early and asks for what
is missing with a RewindRequest on the repair port, the primary answers any request, and a
stalled pipeline reships its whole unacknowledged suffix on its own clock, which the standby's
skip-what-is-covered rule makes idempotent. The standby itself never sees a gap, so its contract
is unchanged by the wire.

## Leadership

At most one sequencer extends the log, and epochs are how that is enforced. Every range carries
the epoch it was published under; gateways and consumers reject a stale epoch outright. A witness
process arbitrates the lease: LeaseRequest names the requesting node, the epoch it wants (its
current one to renew, a higher one to take over) and the highest sequence it holds; LeaseResponse
names the holder, the epoch and the remaining time. The witness grants a higher epoch only when
the standing lease has expired, so a live primary cannot be usurped, and an expired primary must
stop publishing before its lease can be granted away, which is what makes the handover safe. A
standby that wins epoch E+1 first publishes any replicated suffix the old primary never
published, from the floor the link's markers named, then continues sequencing on the journal it
already holds. It inherits the dedupe windows it mirrored as the standby, so a gateway's
resubmission of anything replicated is re-answered with the place the command already holds, and
exactly-once survives the failover; the stitched stream across epochs is gap free and duplicate
free, and the chaos suite holds it to that. A total failure of both nodes ends the session: the
journals hold the record, recovery is the next session started fresh, and gateways resynchronise
at the boundary, which is the deployment real venues run; MoldUDP64's session name is that
boundary made visible on the wire.

## The market's clock

SessionControl enters the sequence from exactly one place: the operations process, a consumer
with authority that reads the same stream as everyone and can only act by submitting commands on
its own gateway carrier, which take effect when sequenced. It owns the calendar, pre-open
through the closing auction to the close, and it owns halts: an instrument whose print lands
outside a configured band around the trailing mean of its own executions is halted, and a halted
instrument reopens through an auction, because a halted book needs a fair price to restart from.
The shape follows the Limit Up-Limit Down plan (public); the band, its trailing window, the
pause and the reopening call are configuration. Above the single-instrument bands sits the
market-wide circuit breaker in NYSE Rule 7.12's shape: declines in a configured reference
instrument from the session's first print trip three levels, the first two pausing every
instrument once per session each, the third closing the day, and the calendar outranks all of
it, because tomorrow is a calendar event. A replayed day reproduces every transition, every halt
and every break at the same sequences, because the commands are in the journal like everything
else.

## The oversight plane

The venue is answerable through two read-only consumers. Drop copy is a session server beside
the gateway: a firm responsible for a participant logs in over the session plane naming that
participant as its scope, hears the participant's events framed and implicitly numbered exactly
as the trading session does, and reconnects by naming the sequence it reached, replayed byte
exactly from the retained per-scope stream. A drop copy session speaks only the session plane;
any command poisons it. The shape follows the drop copy services real venues sell (CME Drop
Copy, public); its purpose is that the client's risk owner hears about the client's doings on a
channel the client cannot touch.

Surveillance consumes the event stream and emits SurveillanceAlert messages into an alert
journal in the standard journal format, so a case is a replayable artifact. A wash trade is an
execution whose aggressor and resting orders share a participant. Spoofing is executed quantity
on one side answered, within a configured window of stream time, by cancelled resting quantity
on the other side exceeding both a configured multiple of it and an absolute floor (the Coscia
pattern); when that
cancelled quantity stood at two or more distinct price levels the alert reads layering instead.
Every detection is a pure function of the stream, so a replayed day raises the same alerts at
the same sequences.

## The public feed

What the world sees is derived: a builder consumes the matcher's events and publishes the public
vocabulary, messages 40 through 47, each opening with the public context, timestamp and
instrument and nothing else, so the feed structurally cannot carry attribution rather than
merely choosing to strip it. The mapping is the consumer rules made public: a rested order is
added, an execution names each book order it reduced with its price and quantity, a reduction
names the new displayed quantity, a removal deletes, and trading states and auction indications
pass through; the close publishes PublicSessionSummary, the session's official numbers, open,
high, low, close, volume and prints, the close being the last print, which is the closing cross
when one ran; acceptances and refusals are private to their sessions and never appear. The feed
renumbers onto its own sequence, carried by its ranges the way every stream here carries one,
and travels as the same MoldUDP64-shaped packets on two identical feeds, A and B, so a consumer
takes whichever packet arrives first and single-packet loss costs nothing; what both feeds lose
the retransmission server replays by range. The shape follows TotalView-ITCH and the delivery
follows MoldUDP64 (Nasdaq, public specifications). On the box each feed is UDP, unicast when one
consumer sits on each twin or a multicast group every participant joins, which is how the real
delivery reaches a crowd with one send.

A late joiner recovers by snapshot: the current visible book as a sequence of PublicOrderAdded
messages per instrument, in queue priority order so replaying them rebuilds not just the shape
of the book but its fairness, preceded by each instrument's trading state and closed by
SnapshotComplete naming the feed sequence to join live at, Glimpse's shape (Nasdaq, public
specification). A conflated top of book derives from the same builder on an interval, because
most consumers want the touch and full depth is the price of the few who want everything.

## The end of the day

The venue's statement to the clearing house is two files, written at the close by the post-trade
ledger, deterministic in content and in order so a replayed day writes byte-identical files. A
file an external party settles against is a protocol, so the formats live here beside the
journal's.

The trades file is CSV with a header row, one row per print in sequence order:

| Column | Meaning |
|---|---|
| executionId | the print's engine id |
| sequence | the answering command's place in the global order |
| timestamp | the sequencer's stamp, nanoseconds |
| instrumentId | the book it printed on |
| price | scaled integer, priceScale implied decimals |
| quantity | lots |
| buyer | participantId of the buying side |
| seller | participantId of the selling side |

The positions file is CSV with a header row, one row per (participant, instrument) that traded,
ordered by participant then instrument:

| Column | Meaning |
|---|---|
| participantId | whose account |
| instrumentId | which book |
| position | signed net lots, bought minus sold |
| bought | lots bought |
| sold | lots sold |
| cash | signed scaled-integer cash flow at traded prices, sells positive |

Conservation is the file's own audit: positions sum to zero per instrument and cash sums to zero
across the venue, because every trade moved two accounts by equal and opposite amounts.

## The ring

On-box transport is a single-producer single-consumer ring over a mapped file. Records are eight
byte aligned: a u32 payload length, a u32 kind (0 a message, 1 padding), then the payload, which
is one framed message; on the submission carrier alone it is a framed GatewaySubmission followed
by the framed command it prefixes, one record because they are one submission. Padding records fill the tail of the buffer when a claim would wrap, so a
message is always contiguous. The producer claims space, encodes in place, and publishes a whole
command's events with one release of its position, so a consumer sees a command's batch or
nothing; a full ring is back pressure and the producer waits, because a dropped event is a stream
that cannot rebuild a book.

The matcher's event stream is the exception, because it fans out: the gateway, the market data
publisher and the operations scheduler all read it, and the matcher must never wait on its
slowest listener. That carrier is a single-producer broadcast ring with the same record shape and
no back pressure, following Aeron's broadcast buffer (Real Logic, open source messaging). The
producer advances a claim marker before touching any byte and publishes its head after; a reader
keeps its own position, copies a record out, and validates the copy against the claim marker, so
a record is delivered whole or not at all. A reader that falls a whole buffer behind is lapped:
it counts the lap, rejoins at the head, and recovers what it missed from the journal, never from
guesswork.

## The journal

The sequencer's journal is the append-only record of the sequenced command stream, and a matcher
keeps the same format for the stream it consumed. A journal file is:

| Part | Layout |
|---|---|
| header | magic "EXJRNL01" (8 bytes), schemaId u32, schemaVersion u32 |
| record, repeated | length u32, one framed message of that length, crc u32 (CRC32C of the message bytes) |

Records are eight byte aligned by padding after the crc; the pad bytes are zero. Recovery reads
records until the file ends or a record is torn (short, or failing its crc), truncates the torn
tail, and verifies that command sequences are contiguous. A torn tail is the expected result of a
process dying mid-append and is repaired by truncation rather than guesswork.

## Snapshots

A snapshot is the full state of one matcher partition as a versioned blob, so recovery is snapshot
plus journal suffix rather than a full replay. A snapshot file is:

| Part | Layout |
|---|---|
| header | magic "EXSNAP01" (8 bytes), schemaId u32, schemaVersion u32, stateVersion u32, reserved u32, upToSequence u64, stateLength u64 |
| state | stateLength bytes, the partition state at stateVersion |
| trailer | crc u32 (CRC32C of the state bytes) |

upToSequence names the last command the snapshot includes. Restoring the snapshot and replaying
the journal from upToSequence + 1 produces an event stream byte identical to the run that never
stopped, and the determinism suite proves exactly that. The state layout is versioned by
stateVersion and documented beside the matcher that writes it.

## Determinism rules

Every process behind the sequencer obeys these, and the determinism suite enforces them: no clock
reads, no randomness, no iteration over anything without a defined order, no behaviour depending
on addresses, thread timing or environment. The same sequenced input produces byte identical
output, always (P-2).
