#!/usr/bin/env bash
set -euo pipefail
docker-compose up -d
echo "Services are starting. Use 'docker-compose ps' to view status."