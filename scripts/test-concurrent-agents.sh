#!/bin/sh
##################################################################################################################################
# Starts N agent_client processes in the background, piping each log into separate file.
# Shows the numbe rof running agent_client processes.
# Once done, use "consolidate-logs_test-concurrent-agents.sh" script to collect all logs into one file: logs/data.txt
##################################################################################################################################

AGENTS=$1

LOG=$RUST_LOG
export RUST_LOG=info

LOGS="./logs/log-$AGENTS"

rm -rf $LOGS
mkdir -p $LOGS

CMD="cargo run --example agent_client --release"

for i in $(seq $AGENTS)
do
	echo "Starting agent ${i}: ${CMD}" && \
		${CMD} &> "$LOGS/agent-log-${i}.txt" && \
		cat "$LOGS/agent-log-${i}.txt" | grep FutureProducer > "$LOGS/agent-log-${i}.txt.log" &
done

watch -n 1 "ps -a | grep agent_client | grep examples | grep -v watch"