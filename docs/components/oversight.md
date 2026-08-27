# Component: oversight

A venue is answerable for what happens on it. Two consumers carry that answer: drop copy, which
gives the firms responsible for a participant an independent copy of everything that participant
did, and surveillance, which watches the whole stream for the trading patterns regulation exists
to catch. The architecture makes both of them stream-processing problems, because the evidence
is already one totally ordered log with one timestamp per command: a surveillance case is a
range of sequence numbers, and a drop copy session is a filter over the same events the trading
session already heard.

## The design

Drop copy is a read-only session server beside the gateway: a clearing firm or a risk desk logs
in naming the participant it is entitled to watch, receives that participant's events framed and
implicitly numbered exactly as the trading session does, and reconnects by naming the sequence
it reached, replayed byte-exactly from the retained per-scope stream. The vocabulary is the
session plane the gateway already speaks; the one difference is that a drop copy session may
say nothing but session plane, and a command poisons it. The shape follows the drop copy
services real venues sell (CME Drop Copy, Nasdaq post-trade copies): the client's risk owner
hears about the client's executions on a channel the client cannot touch.

Surveillance consumes the event stream, learns order ownership from acceptances the way every
routing consumer does, and holds per-participant, per-instrument recent history in fixed rings.
Three detections, each a pure function of the stream so a replayed day raises the same alerts
at the same sequences:

- A wash trade is an execution whose aggressor and resting orders share a participant: the
  simplest self-dealing, caught exactly.
- Spoofing is pressure that was never meant to trade: a participant executes on one side and,
  within a configured window of stream time, cancels resting quantity on the other side that
  dwarfs what it executed. The detector keys on the Coscia pattern: large away-side interest,
  small executions, prompt cancellation.
- Layering is the same intent spread across price levels: the cancelled away-side interest in
  the window stood at several distinct prices.

Alerts are SBE messages like everything else, written to an alert journal in the standard
journal format, so a case file is a replayable artifact rather than a log line. The flagship
negative control is the ecosystem's own seeded day: honest participants raise nothing, and the
scripted abuser raises exactly its own alerts, both byte-stable across runs.

## How it lands

The contract first: the alert vocabulary in the schema, the oversight plane in PROTOCOL.md, and
the architecture's consumer list moving from named to built. Then the component: the drop copy
machine and the surveillance engine as headers with their binaries, the drop copy session suite
held to the gateway's session law, the surveillance corpus of scripted abuse driven through the
real matcher, the innocent-day negative control, the allocation probe and the benchmark.

## Where to learn

- United States v. Coscia (7th Cir. 2016). The first criminal spoofing conviction; the pattern
  the detector looks for, described by the court that upheld it.
- EU Market Abuse Regulation, Article 12, and SEC Rule 15c3-5. What manipulation is in law, and
  why the gate and the watchers both exist.
- FINRA CAT (Consolidated Audit Trail) technical specifications. What regulators actually
  collect, which is this venue's journal by another name.
- CME Drop Copy service documentation (public). The real product the drop copy server mirrors.
- Aitken, Cumming and Zhan, Exchange trading rules, surveillance and suspected insider trading,
  Journal of Corporate Finance, 2015. Surveillance as market design rather than afterthought.
