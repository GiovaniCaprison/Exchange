# Component: market data

The public feed: a consumer of the matcher's event streams that maintains books and publishes
what the world sees. Nothing here executes inside the matcher; the feed is derived, which is also
the standing proof that the event stream is sufficient to rebuild the state it describes (P-1).

## The design

A book builder consumes each partition's events through the sequencer's consumer library and
maintains the visible book per instrument, applying the same rules PROTOCOL.md states for any
consumer. The publisher emits an ITCH-shaped full-depth feed (TotalView-ITCH, Nasdaq, public
specification): add, execute, cancel, delete, replace, trade and state messages, renumbered onto
the feed's own sequence, carrying no private attribution, which is what the public feed's
derivation strips.

Delivery is the venue's own mechanism again: MoldUDP64-shaped packets on an A feed and a B feed
(two identical streams, the consumer arbitrating whichever packet arrives first, which is how
real feeds hide single-packet loss), a retransmission server for gap re-requests, and a snapshot
service for late joiners in the shape of Nasdaq's Glimpse: connect, receive the current book as a
message sequence, and join the live feed at the sequence the snapshot names. CME's MDP 3.0 is the
citable alternative shape, incremental feed plus snapshot recovery over SBE, and reading both
against this design shows the space of conventions.

A conflated top-of-book feed derives from the same builder, because most consumers want the
touch and the depth feed's bandwidth is the price of the few who want everything; publishing both
from one builder teaches the bandwidth-versus-latency trade every real feed makes.

## The questions it answers

Whether the event stream truly rebuilds the book under every mechanic the matcher has, at feed
rates; what A/B arbitration, gap recovery and snapshot-then-join cost a consumer to implement,
learned by implementing both sides; and what a feed's bandwidth actually is under generated load.

## How it lands

Protocol and doc additions for the public message set and the snapshot service; the book builder
held to the matcher's own state by the invariants machinery; the publisher with A/B, rewinder and
snapshot; the consumer-side arbitration and recovery in the shared library; loss-injection tests;
and the measured event-in to packet-out hop.

## Where to learn

- Nasdaq TotalView-ITCH 5.0 specification (public). The full-depth feed convention, message by
  message.
- Nasdaq Glimpse specification (public). Snapshot-then-join, the recovery path every late
  consumer uses.
- Nasdaq MoldUDP64 specification (public). The packet and rewind layer, shared with the
  sequencer.
- CME MDP 3.0 documentation (public). The other major convention: incremental plus snapshot
  recovery, over SBE, with books keyed differently; the comparison sharpens both.
- Larry Harris, Trading and Exchanges, 2003, on transparency and market data. Why venues publish
  what they publish.
