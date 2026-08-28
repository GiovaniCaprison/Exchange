# Component: post-trade

A trade is not finished when it prints; it is finished when both sides have settled, and the
venue's part of that is handing an exact account of the day to the people who make settlement
happen. In reality a central counterparty takes the hard part, novation, netting and default
management, and what the venue owes it is the day: every trade with both sides named, and the
positions they net to. Both are derived facts of the event stream, which makes post-trade the
purest consumer in the venue: it holds no authority, it makes no decisions, it just keeps the
books, and its whole correctness claim is arithmetic.

## The design

The ledger consumes the event stream from its broadcast seat, resolves both sides of every
execution through the same ownership discipline every routing consumer keeps, and maintains two
accounts. The trade tape: every print with its executionId, instrument, price, quantity, buyer
and seller, sequence and timestamp, retained in arrival order. The position ledger: per
participant per instrument, the signed net position, the bought and sold volumes, and the signed
cash flow at traded prices. The law the ledger lives under is conservation: every trade moves
two accounts by equal and opposite amounts, so positions sum to zero per instrument and cash
sums to zero across the venue, at every moment, and the suites hold that invariant over
generated flow rather than asserting it.

At the close the ledger writes the day: a trades file and a positions file, deterministic in
content and in order, so a replayed day writes byte-identical files, and the files themselves
are the venue's statement to the clearing house. The formats are documented field by field in
PROTOCOL.md the way the journal and snapshot formats are, because a file an external party
settles against is a protocol, not an artifact.

The second proof is the one that matters most: in the whole-room day, the ledger's position for
every participant equals the position that participant's own order entry client computed from
its own fills. The venue's books and the participants' books agree because both are functions of
the same stream, which is the architecture keeping its central promise in the place money would
notice.

## How it lands

The contract first: the file formats in PROTOCOL.md and this page. Then the component: the
ledger machine and its binary, the conservation invariants over generated flow, the
venue-agrees-with-its-participants proof in the room, byte-identical files from a replayed day,
the allocation probe and the benchmark. The ownership table the ledger needs is the same one
three components already carry, so it lands as the shared discipline in `components/common/` and
the older copies converge on it in the cleanup that follows.

## Where to learn

- CPMI-IOSCO, Principles for Financial Market Infrastructures, 2012. What the CCP owes the
  world, and therefore what the venue owes the CCP.
- DTCC, public materials on equities clearing and settlement. How the day actually settles in
  the US, continuous net settlement included.
- Norman, The Risk Controllers: Central Counterparty Clearing in Globalised Financial Markets,
  2011. Why the CCP exists, told through the crises that built it.
- Pirrong, The Economics of Central Clearing, ISDA discussion paper, 2011. The netting arithmetic
  and what it buys, which is the ledger's conservation law with the units filled in.
