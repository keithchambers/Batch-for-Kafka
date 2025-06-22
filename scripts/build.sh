#!/usr/bin/env bash
set -euo pipefail
ROOT=$(dirname "${BASH_SOURCE[0]}")/..
pushd "${ROOT}" > /dev/null

echo "Building server and CLI binaries..."
docker build -f Dockerfile.server -t batch-ingest-api:latest .
docker build -f Dockerfile.cli -t batch-cli:latest .

echo "Copying CLI binary to ./bin for local use..."
mkdir -p bin
docker run --rm batch-cli:latest batch --help > /dev/null
CID=$(docker create batch-cli:latest)
docker cp "${CID}":/usr/local/bin/batch ./bin/batch
docker rm "${CID}"

chmod +x ./bin/batch
echo "Build complete."
popd > /dev/null