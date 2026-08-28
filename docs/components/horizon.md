# The horizon

Everything worth building after the arc, each entry small here and owed its own page when its
turn comes. Nothing on this list blocks anything above it.

## Off-box transport

The replication link already crosses machines: ranges over UDP, the repair conversation riding
back on one port, loss reshipped and reorder relinked, with the two-box runbook in PRACTICE.md.
What remains: the witness's own UDP carrier, so automated failover crosses boxes too; Aeron
(Real Logic) as the built reference for the general stream between machines; kernel bypass
(OpenOnload, DPDK) for the network path; and PTP discipline as a measured fact rather than a
setup step.

## Other engines

The matcher is a continuous limit order book. Venues that are good at other mechanisms run
purpose-built engines: implied and spread matching where orders in one book derive from another
(CME runs a separate implied engine), periodic auction books, midpoint dark books, and request
for quote. Each is a different engine at the same process boundary, which the architecture
already prices in.

## Hardware

The step past software: FPGA feed handlers and risk gates are what the fastest firms and some
venues deploy at the edge. Out of scope for building; worth reading about once wire-to-wire
numbers exist, because the comparison says what the remaining software costs are.
