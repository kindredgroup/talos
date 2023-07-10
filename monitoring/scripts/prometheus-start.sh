#!/bin/sh

PROMETHEUS_HOME=/Users/marekfedorovic/kindred/local-infra/prometheus-2.45.0.darwin-amd64

config_file=$1

echo "starting prometheus " & \
    $PROMETHEUS_HOME/prometheus \
        --log.level=warn \
        --config.file=$config_file \
        --storage.tsdb.path=./prometheus_data