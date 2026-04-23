#!/bin/bash
# Hive Metastore startup: initialize schema on first run, then start service.
# Using a script file avoids argument re-splitting by the bde2020/hive entrypoint.
set -x

# schematool fails harmlessly on subsequent runs (schema already exists)
/opt/hive/bin/schematool -dbType postgres -initSchema || true

# Start the metastore service (replaces this shell)
exec /opt/hive/bin/hive --service metastore
