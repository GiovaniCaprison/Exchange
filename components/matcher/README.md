# The matcher

The venue's hot core: one thread on one pinned core, consuming sequenced commands from a ring,
emitting events into another, allocating nothing after an instrument is defined (P-6). One
partition process serves every instrument routed to it, each with its own book, all sharing one
slab, one feed and one set of id counters, so the partition is one sequence of decisions and its
event stream numbers gap free from 1.

## The representation

An order is an int slot in a slab of two arrays: a hot struct of exactly one cache line carrying
what the match loop touches per fill (quantities, id, tick, links, arrival, self match id), and a
cold struct carrying identity and the entry-time qualifiers, touched once per command at most. The
free list threads through the same links the queues use, so a slot is always in exactly one chain
(P-8).

A side's price levels are a flat array indexed by rank, where the ask ladder ranks ticks ascending
and the bid ladder descending: rank zero is the best price on either side, a taker's limit is one
rank bound, and crossing is a single comparison with no side branch in the loop. A level is two
words, the queue's head and tail packed into one and the cached displayed and remaining totals
interleaved in the other (P-7). A three level occupancy bitmap sits over the ladder, so the best
rank is a cached int and its successor after a level empties is three trailing-zero counts away.
The name index a cancel probes is open addressing over one interleaved array, two words per entry,
with backward-shift deletion so the steady state never rehashes.

## Where the nanoseconds go

The whole engine is header-only templates the process binary instantiates once, so claim, commit
and every book operation inline and no dispatched call survives anywhere in the match loop (P-13).
The taker lives in registers for the whole walk and writes the slab only if it rests, so an
aggressive order that fills completely never writes an order anywhere. Validation runs the
protocol's refusal precedence, cheapest checks first, and the one division that proves a price on
tick is the division that yields the ladder index. Events encode in place into claimed ring space
(P-12) and the ring publishes once per command, so a burst of executions costs one visibility
operation rather than one per event. Per-instrument facts bind at definition (P-5): the allocation
algorithm is a branch on a field held in a register across the walk, and what an instrument cannot
express costs its orders nothing.

## What holds it to its word

The corpus in `corpus/` replays fixtures of commands against their blessed events and diffs word
for word; it covers the remit from price-time and pro-rata allocation through icebergs, stops and
their cascades, self match prevention, replace semantics, mass cancel, session states, both
auctions and the refusal precedence. The invariants suite replays generated flow, with and without
call phases, and after every command holds the caches to the queues they summarise: level totals
to the orders under them, the bitmap and the cached best to the ladder, the name index to the
queues, uniqueness of ids across the book and the trigger book, and the visible book a consumer
rebuilds from events alone to the engine's own, with a full-ladder sweep at the end of every flow.
The attribution suite holds every event to its context: gap free sequence, the input sequence it
answers, the carried timestamp (P-3), and one publish per command.
