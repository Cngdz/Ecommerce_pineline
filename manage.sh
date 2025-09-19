#!/bin/bash

case $1 in
  build)
    docker compose build
    ;;
  start)
    docker compose up -d
    ;;
  setup)
    docker-compose exec trino sh -c "trino --server localhost:8080 --catalog iceberg -f /setup/setup_tables.sql"
    ;;
  stop)
    docker compose down 
    ;;
  stop-remove)
    docker compose down -v
    ;;
  *)
    echo "Usage: $0 {start|setup|stop}"
    exit 1
    ;;
esac
