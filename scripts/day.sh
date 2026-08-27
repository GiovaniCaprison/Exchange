#!/bin/bash
# The day simulator: the whole venue and its participants on one box, every process real, every
# journal kept, every measurement written. This is the run-a-venue quickstart automated; the
# literal step-by-step lives in README.md and teaches what this script does.
#
#   scripts/day.sh [DIR]
#
#   DURATION=30        seconds of trading (default 30)
#   NOISE=2            noise takers (default 2); each takes a seat and a seed
#   MULTICAST=239.7.7.7  set to a group to deliver the feed as multicast every bot joins;
#                        unset, the maker takes the A feed and the first noise bot takes B
#   BUILD=build        build directory holding the binaries
#   SPIN=1             busy-poll the gateway (the box's posture; pins a core's worth of fan)
#
# Everything lands in DIR: rings, journals, per-bot summaries, and the maker's wire-to-wire
# series and manifest. The venue is torn down when the bots finish.

set -euo pipefail

DIR=${1:-day-$(date +%Y%m%d-%H%M%S)}
DURATION=${DURATION:-30}
NOISE=${NOISE:-2}
BUILD=${BUILD:-build}
MULTICAST=${MULTICAST:-}
mkdir -p "$DIR"
DIR=$(cd "$DIR" && pwd)

GATEWAY_PORT=36201
FEED_A=36202
FEED_B=36203
REWIND=36204

# Rings live on tmpfs where the box has one, which is where deployment puts them anyway.
RINGS="$DIR"
if [ -d /dev/shm ]; then
  RINGS=$(mktemp -d /dev/shm/exchange-day-XXXXXX)
fi

PIDS="$DIR/pids"
: > "$PIDS"
cleanup() {
  kill $(cat "$PIDS") 2>/dev/null || true
  sleep 1
  [ "$RINGS" != "$DIR" ] && rm -rf "$RINGS"
}
trap cleanup EXIT

# Participants: 7 the dealer, 8.. the crowd. Credentials are the gateway's table.
PARTICIPANTS="7:42"
for n in $(seq 1 "$NOISE"); do
  PARTICIPANTS="$PARTICIPANTS,$((7 + n)):$((42 + n))"
done

# The carriers who create their rings first, then everyone downstream in dependency order.
"$BUILD/components/gateway/gateway" --listen $GATEWAY_PORT \
  --submissions "$RINGS/gw.ring" --acks "$RINGS/gwacks.ring" --events "$RINGS/events.ring" \
  --participants "$PARTICIPANTS" --gateway-id 0 ${SPIN:+--spin} >"$DIR/gateway.log" 2>&1 &
echo $! >> "$PIDS"
"$BUILD/components/operations/operations" \
  --submissions "$RINGS/ops.ring" --acks "$RINGS/opsacks.ring" --events "$RINGS/events.ring" \
  --instruments 1 --define 1:5:1:5:1000000:100000000:100000 \
  --calendar 0:CONTINUOUS --gateway-id 1 >"$DIR/operations.log" 2>&1 &
echo $! >> "$PIDS"
until [ -e "$RINGS/gw.ring" ] && [ -e "$RINGS/ops.ring" ]; do sleep 0.1; done

"$BUILD/components/sequencer/sequencer" \
  --in "$RINGS/gw.ring,$RINGS/ops.ring" --acks "$RINGS/gwacks.ring,$RINGS/opsacks.ring" \
  --out "$RINGS/seq.ring" --journal "$DIR/seq.exj" >"$DIR/sequencer.log" 2>&1 &
echo $! >> "$PIDS"
until [ -e "$RINGS/seq.ring" ]; do sleep 0.1; done

"$BUILD/components/matcher/matcher" --in "$RINGS/seq.ring" --out "$RINGS/events.ring" \
  --journal "$DIR/matcher.exj" >"$DIR/matcher.log" 2>&1 &
echo $! >> "$PIDS"
until [ -e "$RINGS/events.ring" ]; do sleep 0.1; done

if [ -n "$MULTICAST" ]; then
  FEED_TO_A="$MULTICAST:$FEED_A"
  FEED_TO_B="$MULTICAST:$FEED_B"
  MAKER_FEED="$MULTICAST:$FEED_A"
  NOISE_FEED="$MULTICAST:$FEED_A"
else
  FEED_TO_A="127.0.0.1:$FEED_A"
  FEED_TO_B="127.0.0.1:$FEED_B"
  MAKER_FEED="$FEED_A"
  NOISE_FEED="$FEED_B"
fi
"$BUILD/components/marketdata/marketdata" --in "$RINGS/events.ring" \
  --a "$FEED_TO_A" --b "$FEED_TO_B" --rewind $REWIND >"$DIR/marketdata.log" 2>&1 &
echo $! >> "$PIDS"

# The participants. Without multicast, unicast feeds seat one consumer each: the dealer takes A
# and the first noise taker takes B; more noise takers need a multicast group.
if [ -z "$MULTICAST" ] && [ "$NOISE" -gt 1 ]; then
  echo "unicast feeds seat two consumers; NOISE>1 wants MULTICAST=239.x.y.z" >&2
  NOISE=1
fi
"$BUILD/components/ecosystem/bot" --connect $GATEWAY_PORT --participant 7 --secret 42 \
  --feed "$MAKER_FEED" --rewind $REWIND --role maker --duration-s "$DURATION" \
  --results "$DIR" --label wire-to-wire >"$DIR/maker.log" 2>&1 &
echo $! >> "$PIDS"
MAKER=$!
for n in $(seq 1 "$NOISE"); do
  "$BUILD/components/ecosystem/bot" --connect $GATEWAY_PORT --participant $((7 + n)) \
    --secret $((42 + n)) --feed "$NOISE_FEED" --rewind $REWIND --role noise --seed $((n * 7)) \
    --duration-s "$DURATION" --expect-trades >"$DIR/noise-$n.log" 2>&1 &
  echo $! >> "$PIDS"
done

wait "$MAKER"
echo "the day is done; journals, logs and the wire-to-wire series are in $DIR"
cat "$DIR"/wire-to-wire-manifest.json 2>/dev/null || true
