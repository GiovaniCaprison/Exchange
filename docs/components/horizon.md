# The horizon

Everything worth building after the arc, each entry small here and owed its own page when its
turn comes. Nothing on this list blocks anything above it.

## Post-trade

Trade capture into end-of-day files toward a clearing house, and a position ledger, both derived
from the event stream. The venue hands the hard part to a CCP in reality; the study ground is
CPMI-IOSCO, Principles for Financial Market Infrastructures, 2012, which describes what the CCP
owes the world, and DTCC's public materials for how equities actually clear and settle.

## Off-box transport

The sequenced stream between machines: Aeron (Real Logic) as the built reference, kernel bypass
(OpenOnload, DPDK) for the network path, and time itself as an engineering problem once two
machines are involved, which is IEEE 1588 precision time protocol territory. This is also where
the measurement story grows up: one-way latency between machines cannot be measured without
synchronized clocks, and learning why is half the value.

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
