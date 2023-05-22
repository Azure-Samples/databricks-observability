#!/usr/bin/env bash

set -euxo pipefail  # stop on error

if [[ ${DB_IS_DRIVER:-TRUE} = "TRUE" ]]; then
  type="driver"
else
  type="executor"
fi

echo "Installing envsubst"
apt-get install -y gettext

echo "Generating /tmp/applicationinsights.json"
envsubst < /dbfs/observability/applicationinsights-$type.json > /tmp/applicationinsights.json

echo "Copying /tmp/applicationinsights-agent.jar"
cp /dbfs/observability/applicationinsights-agent.jar /tmp/
