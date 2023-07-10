#!/bin/sh

JEAGER_HOME=/Users/marekfedorovic/kindred/local-infra/jaeger-1.46.0-darwin-amd64
COL_HOME=/Users/marekfedorovic/kindred/local-infra/otelcol-contrib_0.81.0_darwin_amd64

col_config=/Users/marekfedorovic/kindred/work/talos/opentelemetry/jeager-otel-collector-config.yml

config_file=$1

echo "starting jaeger"

METRICS_STORAGE_TYPE=prometheus & \
PROMETHEUS_SERVER_URL=127.0.0.1:9090 & \
$JEAGER_HOME/jaeger-all-in-one \
    --query.ui-config=$config_file \
    --collector.queue-size=10000 \
    --metrics-backend=prometheus \
    --collector.otlp.enabled=false \
    --log-level=warn &> logs/jaeger.log &

echo "starting collector"

$COL_HOME/otelcol-contrib --config $col_config &> logs/collector.log &
