#!/bin/bash

# Ensure we are in the correct directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

echo "Starting services locally..."
docker compose up -d --build

echo "Services started!"
