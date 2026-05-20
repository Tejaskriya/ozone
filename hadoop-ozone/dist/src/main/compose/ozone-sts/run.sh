#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#
# Simple runner script for the Ozone STS test environment
# This script starts the environment and provides an interactive shell
#

set -e

COMPOSE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
export COMPOSE_DIR

echo "Starting Ozone S3 Gateway STS Test Environment..."
echo "=================================================="

# Start the environment
docker-compose up -d

echo ""
echo "Waiting for services to start..."

# Wait for STS server
echo -n "Waiting for STS server..."
while ! curl -s http://localhost:8080/health > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " ✓"

# Install npm dependencies
echo "Installing STS server dependencies..."
docker-compose exec sts-server npm install

# Wait for S3 Gateway
echo -n "Waiting for S3 Gateway..."
while ! curl -s http://localhost:9878 > /dev/null 2>&1; do
    echo -n "."
    sleep 2
done
echo " ✓"

echo ""
echo "🎉 Environment is ready!"
echo ""
echo "Services available:"
echo "  - STS Server:    http://localhost:8080"
echo "  - S3 Gateway:    http://localhost:9878"
echo "  - STS Health:    http://localhost:8080/health"
echo ""
echo "To run the STS authentication test:"
echo "  docker-compose exec aws-cli /scripts/test-sts-auth.sh"
echo ""
echo "To get an interactive shell in the AWS CLI container:"
echo "  docker-compose exec aws-cli bash"
echo ""
echo "To view logs:"
echo "  docker-compose logs -f sts-server"
echo "  docker-compose logs -f s3g"
echo ""
echo "To stop the environment:"
echo "  docker-compose down"
echo ""

# Keep the script running
echo "Press Ctrl+C to stop the environment and exit..."
trap 'echo ""; echo "Stopping environment..."; docker-compose down; exit 0' INT

# Wait indefinitely
while true; do
    sleep 1
done
