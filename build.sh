#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
set -e

echo "Building the Go-Redis server..."

# Create a bin/ directory if it doesn't exist
mkdir -p bin

# Compile the Go source files from the app/ directory.
# The output binary will be named 'go-redis' and placed in the bin/ directory.
go build -o ./bin/go-redis ./app/*.go

echo "Build successful!"
