#!/bin/sh

AGENTS=$1

for d in $(seq $AGENTS)
do
	LOG_DIR="logs/log-${d}"
	for i in $(seq $d)
	do
		LOG_FILE="${LOG_DIR}/agent-log-${i}.txt.log"
		echo "copying $LOG_FILE >> logs/data.txt" 
		cat $LOG_FILE >> logs/data.txt
	done
	echo "" >> logs/data.txt
done