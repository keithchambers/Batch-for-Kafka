#!/usr/bin/env bash
set -euo pipefail
CLEAN=false
for arg in "$@"; do
  case "$arg" in
    -c|--clean)
      CLEAN=true
      shift
      ;;
  esac
done

if [ "${CLEAN}" = true ]; then
  docker-compose down -v --remove-orphans
else
  docker-compose down --remove-orphans
fi