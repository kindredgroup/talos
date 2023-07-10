#!/bin/sh

GRAFANA_HOME=/Users/marekfedorovic/kindred/local-infra/grafana-10.0.1

config_file=$1

echo "starting grafana " & \
    $GRAFANA_HOME/bin/grafana server \
        --config=$config_file \
        --homepath=$GRAFANA_HOME