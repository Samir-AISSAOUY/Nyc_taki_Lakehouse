#!/bin/bash

set -e

echo "🚀 Starting Spark container..."

# Execute the command passed to the container
exec "$@"