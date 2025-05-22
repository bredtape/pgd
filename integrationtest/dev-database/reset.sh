#!/bin/bash

set -e
set -x

docker compose down --volumes
docker compose up -d --remove-orphans
docker compose logs -f
