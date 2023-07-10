#!/bin/sh

TARGET_DIR=$1
DB_LOG=$2

echo "Collecting log files ..."

COHORT_LOGS="talos-certifier-git/logs"
DB_LOGS="/Library/PostgreSQL/13/data/log"

if [ -n "$DB_LOG" ]
then
    cp $DB_LOGS/$DB_LOG $TARGET_DIR/
fi

cp $COHORT_LOGS/* $TARGET_DIR/

ls -lah

echo "Extracting data ..."

cat cohort.log | grep "METRIC agent-spans: " | sed 's/.*spans: //' > stats-agent-spans.csv
cat cohort.log | grep "METRIC (batch-executor)" | sed 's/.*batch-executor): //' > stats-installer.csv
cat cohort.log | grep "METRIC (installer)" | sed 's/.*METRIC (installer) : //' > stats-installer-lag.csv
cat cohort.log | grep "METRIC (cohort)" | sed 's/.*METRIC (cohort) :  //' > stats-cohort.csv
cat cohort.log | grep "Counts. Remaining" | sed 's/.*Counts. Remaining: //' | sed 's/, processed.*//' > stats-remaining-counts.csv
cat cohort.log | grep "Counts. Remaining" | sed 's/.*installer: //' > stats-installed-counts.csv
cat cohort.log | grep "long-safepoint-wait" | sed 's/.*long-safepoint-wait: //' > stats-safepoint-wait.csv

cat hist.log | grep "bucket-published" | sed 's/.*bucket-published: //' > stats-hist-published.csv
cat hist.log | grep "histograms MAX" | sed 's/.*histograms MAX): //' > stats-hist-max.csv

cat talos.log | grep "all-times" | sed 's/.*all-times: //' | sed 's/",".*//' > stats-talos-decision-processing.csv

if [ -n "$DB_LOG" ]
then
    cat ./*.csv | grep "talos-sample-cohort-dev" | grep "duration: " | sed 's/.*duration: //' | sed 's/ ms .*//' > stats-db-durations-cohort.csv
    cat ./*.csv | grep "talos-certifier-dev" | grep "duration: " | sed 's/.*duration: //' | sed 's/ ms .*//' > stats-db-durations-talos.csv
    chown -R marekfedorovic:staff .
fi

ls -lah