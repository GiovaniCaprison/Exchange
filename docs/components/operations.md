# Component: market operations

The market's clock. The matcher moves state only on SessionControl commands (FR-style mechanics
already built); something has to send them, on schedule and on triggers, and that something is
this component: a scheduler that is itself just another participant in the sequence, submitting
commands through the same path everything else uses (P-1).

## The design

The scheduler owns the trading day: pre-open at a configured time, the opening auction's call
phase and its uncross, continuous trading, the closing call, the close. It owns halts: manual
halts by operator command, and volatility halts computed from the venue's own feed. The citable
mechanics are the Limit Up-Limit Down plan (the LULD National Market System Plan) for
single-instrument price bands that pause trading when the reference moves too far too fast, and
the market-wide circuit breakers that halt everything on index-level declines; this venue models
LULD-shaped halts per instrument, computed from the event stream, and reopens through an auction,
which is what real venues do because a halted book needs a fair price to restart from.

The depth lesson is that operations is a consumer with authority: it reads the same sequence as
everyone, decides, and can only act by submitting commands that take effect when sequenced, never
by reaching into a matcher. Determinism therefore covers it too: a replayed day reproduces the
same halts at the same sequences, because the scheduler's inputs are the stream and a configured
calendar rather than a wall clock read at runtime; its clock arrives as timestamps already in the
stream (P-3).

## How it lands

The calendar and schedule format; the scheduler process with its command submissions; LULD-shaped
band tracking from the event stream with halts and auction reopens; a corpus of scripted days;
and the determinism suite over a whole simulated session including its halts.

## Where to learn

- The LULD Plan (Limit Up-Limit Down National Market System Plan, public). The real mechanism,
  bands, states and reopenings.
- Market-wide circuit breaker rules (public, in the exchanges' rulebooks). The index-level halt.
- Nasdaq's opening and closing cross specifications (public). How the scheduled auctions actually
  run and print.
- Larry Harris, Trading and Exchanges, 2003, on trading sessions and volatility. The reasoning
  under the mechanisms.
