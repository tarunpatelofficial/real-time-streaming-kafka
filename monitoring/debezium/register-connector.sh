#!/bin/bash

echo "Waiting for Debezium to be ready..."
until curl -s http://debezium:8083/connectors > /dev/null; do
    sleep 2
done

echo "Checking if connector already exists..."
STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://debezium:8083/connectors/orders-connector/status)

if [ "$STATUS" == "404" ]; then
    echo "Registering orders-connector..."
    curl -X POST http://debezium:8083/connectors \
        -H "Content-Type: application/json" \
        -d '{
            "name": "orders-connector",
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.hostname": "postgres",
                "database.port": "5432",
                "database.user": "admin",
                "database.password": "password",
                "database.dbname": "pipeline_source",
                "database.server.name": "pgserver",
                "table.include.list": "public.orders",
                "plugin.name": "pgoutput",
                "topic.prefix": "pgserver"
            }
        }'
    echo "Connector registered."
else
    echo "Connector already exists, skipping registration."
fi