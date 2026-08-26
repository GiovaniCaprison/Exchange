# Principles

Why the code is shaped the way it is. These are the decisions that are cheap to make by accident
and expensive to reverse, so they are written once and referenced from source by id. When a
principle is violated, either the code is fixed or this document changes and says why.

## P-1: Every component is a consumer of the sequence

The sequencer assigns every command one place in a single global order, and every process computes
its state by consuming that order. No component holds a fact that did not arrive through the
sequence, and no pair of components coordinates through any other channel.

This is state machine replication (Lamport 1978; Schneider 1990), and it buys three things at
once: replicas agree because they consume the same input, recovery is replay because the input is
journaled, and failover is continuation because the standby was already consuming. A side channel
between two components would break all three in exchange for a shortcut.

## P-2: Determinism is the availability story

Given the same sequenced input, a component produces byte identical output: no clock, no
randomness, no dependence on wall time, thread interleaving or iteration order of anything
unordered. This is a hard product requirement, held by tests that replay identical input and diff
the bytes.

The warm standby is only a standby because of this. If the primary and the secondary could diverge
on the same input, takeover would need reconciliation, and reconciliation under failure is the
hardest code a venue can carry. Determinism deletes that code.

## P-3: Time is stamped once

The sequencer stamps each command as it sequences it. Every event carries the stamp of the command
it answers, and no process behind the sequencer reads a clock.

A timestamp taken inside a consumer is a nondeterministic input and breaks P-2 the moment it
influences anything. Stamping once also makes the venue's notion of time coherent: every consumer,
the matcher, the feed, surveillance, sees one time per command rather than three opinions.

## P-4: One thread owns a book

Exactly one thread mutates a book. No locks, no concurrent collections, no atomics inside the
matching path. Concurrency comes from partitioning instruments across matcher processes.

The single writer is the fast option as well as the correct one: the book stays in one core's
cache, nothing fences, nothing retries (Fowler 2011, The LMAX Architecture; Thompson et al. 2011,
Disruptor technical paper). It also makes P-2 nearly free inside the matcher, since one writer
applying one ordered stream has one possible history.

## P-5: The matcher pays for nothing it does not need

A field, a check or an indirection on the matching path exists because some order on this
instrument can need it. What is knowable at instrument definition time is bound at instrument
definition time: allocation algorithm, banding, and every other per-instrument fact select their
path once, and the continuous path pays no branch for a feature the instrument cannot express.

The hot path is priced in nanoseconds and the price is paid on every command. A cost that exists
for convenience, symmetry or a feature nobody routed here is a tax on every order in the book.

## P-6: The hot path allocates at initialisation and never again

Every structure the matching path touches is sized and allocated when the process starts or when
an instrument is defined. The steady state allocates nothing, and the claim is proved by a probe
whose allocator counts every request after initialisation and fails on one.

Allocation is structural: it cannot be optimised out of a design that returns objects or grows
containers mid-command, so it is designed out at the signature level and then enforced.

## P-7: Every fact has one owner

Every piece of state has one authoritative home. If two structures both know a fact, one of them
is a cache with an explicit update rule, and an invariants suite holds the two together after
every command.

Duplicated state with independent lifetimes is the defect class that survives every unit test,
because each copy is locally correct and only the relationship is wrong. Caches are still worth
having on a nanosecond path; unpoliced caches are not.

## P-8: Removal means detachment

When an order leaves a structure, every link into and out of it is cleared in the same operation.
A slot's state is a function of its most recent initialisation, which is what makes reuse safe in
a system that never allocates.

A half detached entry is the closest thing a slab has to a use after free: reads still succeed,
arithmetic still works, and a later walk carries a dead order into a live book.

## P-9: Validate at the boundary; below it, preconditions

Business validation happens once, where the command enters the matcher, and an invalid command
produces a typed refusal and changes nothing. Below that boundary, contracts are preconditions:
documented, relied on, and never re-checked at runtime.

A defensive check below the boundary costs a branch on every command to catch a case the boundary
already excluded, and it muddies the one question refusal handling must keep free: was any state
modified before the refusal. Checking strictly before touching state keeps the answer no.

## P-10: Failure is a value with a reason

Every operation that can fail as a matter of business reports a machine readable reason. No
exceptions for expected outcomes, no boolean success flags, no sentinel that conflates two
failures. Reasons are split finely enough for the client to act on: a tick violation tells a
client to fix its rounding, where a generic refusal tells it nothing.

## P-11: No floating point near a price

Prices and quantities are scaled integers throughout, with the scale a property of the instrument.
Binary floating point cannot represent most decimal prices, and the consequences are equality bugs
that split a price level in two, drift in aggregates, and behaviour that breaks P-2. Conversion to
decimal happens at the venue's outermost edges and nowhere else.

## P-12: Events are pushed into claimed space

The matcher never returns a collection and never fills a private buffer to copy from. It claims
space in the outbound ring, encodes one event in place, and commits. The consumer reads the same
bytes the encoder wrote (Thompson et al. 2011; the same claim-commit shape Aeron carries between
machines).

A returned collection is an allocation the caller cannot decline; a filled private buffer is a
copy per event on the path the copies hurt most. Claiming in place is also what puts the consumer
on another core, so counting, checksumming and publishing never execute inside the match.

## P-13: Abstract only for a substitution you can name

An interface exists to support a substitution that actually exists, or to narrow a capability. A
new interface needs a named second implementation; without one, write the class. On the hot path
this is also a performance rule: variation is a field and a branch bound at definition time rather
than a call dispatched inside the match loop.

## P-14: Measured, never trusted

Every performance claim names its mechanism: a latency figure names the harness, the machine and
the manifest of settings it ran under; a zero allocation claim names the probe that failed on one
request; a determinism claim names the replay that diffed the bytes. A number without a mechanism
is an anecdote, and anecdotes about nanoseconds are wrong more often than they are right.

Measurement runs anywhere; results come from the dedicated box, where the environment is
controlled, verified and recorded. A run on an uncontrolled machine is exploratory and labelled as
such by its own manifest.
