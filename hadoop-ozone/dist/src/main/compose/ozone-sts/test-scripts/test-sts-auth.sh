#!/bin/bash
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
# Test script for Ozone S3 Gateway STS authentication
# This script demonstrates the complete STS authentication flow:
# 1. Assume a role to get temporary credentials
# 2. Use those credentials to perform S3 operations
#

set -e

# Configuration
STS_ENDPOINT="http://sts-server:8080"
S3_ENDPOINT="http://s3g:9878"
ROLE_ARN="arn:aws:iam::123456789012:role/OzoneS3User"
ROLE_SESSION_NAME="ozone-test-session"
BUCKET_NAME="sts-test-bucket"
TEST_FILE="test-file.txt"
TEST_CONTENT="Hello from Ozone S3 Gateway with STS authentication!"

echo "=========================================="
echo "Ozone S3 Gateway STS Authentication Test"
echo "=========================================="
echo "STS Endpoint: $STS_ENDPOINT"
echo "S3 Endpoint: $S3_ENDPOINT"
echo "Role ARN: $ROLE_ARN"
echo "Bucket: $BUCKET_NAME"
echo ""

# Function to check if service is ready
wait_for_service() {
    local service_name=$1
    local endpoint=$2
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for $service_name to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s -f "$endpoint" > /dev/null 2>&1; then
            echo "$service_name is ready!"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service_name not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    done
    
    echo "ERROR: $service_name failed to become ready after $max_attempts attempts"
    return 1
}

# Wait for services to be ready
echo "Step 1: Waiting for services to be ready..."
wait_for_service "STS Server" "$STS_ENDPOINT/health"
wait_for_service "S3 Gateway" "$S3_ENDPOINT"

echo ""
echo "Step 2: Assuming role to get temporary credentials..."

# Assume role using AWS CLI
ASSUME_ROLE_OUTPUT=$(aws sts assume-role \
    --role-arn "$ROLE_ARN" \
    --role-session-name "$ROLE_SESSION_NAME" \
    --duration-seconds 3600 \
    --endpoint-url "$STS_ENDPOINT" \
    --output json)

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to assume role"
    exit 1
fi

echo "✓ Successfully assumed role"

# Extract credentials from the response
ACCESS_KEY_ID=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.AccessKeyId')
SECRET_ACCESS_KEY=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.SecretAccessKey')
SESSION_TOKEN=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.SessionToken')
EXPIRATION=$(echo "$ASSUME_ROLE_OUTPUT" | jq -r '.Credentials.Expiration')

echo "Access Key ID: $ACCESS_KEY_ID"
echo "Session Token: ${SESSION_TOKEN:0:20}..."
echo "Expiration: $EXPIRATION"

# Export credentials for AWS CLI
export AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY"
export AWS_SESSION_TOKEN="$SESSION_TOKEN"

echo ""
echo "Step 3: Verifying credentials with GetCallerIdentity..."

# Verify credentials work
CALLER_IDENTITY=$(aws sts get-caller-identity \
    --endpoint-url "$STS_ENDPOINT" \
    --output json)

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to get caller identity"
    exit 1
fi

echo "✓ Credentials verified successfully"
echo "User ID: $(echo "$CALLER_IDENTITY" | jq -r '.UserId')"
echo "Account: $(echo "$CALLER_IDENTITY" | jq -r '.Account')"
echo "ARN: $(echo "$CALLER_IDENTITY" | jq -r '.Arn')"

echo ""
echo "Step 4: Testing S3 operations with temporary credentials..."

# Create test content
echo "$TEST_CONTENT" > "/tmp/$TEST_FILE"

# Test S3 operations
echo "Creating bucket: $BUCKET_NAME"
aws s3 mb "s3://$BUCKET_NAME" --endpoint-url "$S3_ENDPOINT" || {
    echo "Note: Bucket may already exist, continuing..."
}

echo "Uploading test file..."
aws s3 cp "/tmp/$TEST_FILE" "s3://$BUCKET_NAME/$TEST_FILE" --endpoint-url "$S3_ENDPOINT"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to upload file"
    exit 1
fi

echo "✓ File uploaded successfully"

echo "Listing bucket contents..."
aws s3 ls "s3://$BUCKET_NAME/" --endpoint-url "$S3_ENDPOINT"

echo "Downloading test file..."
aws s3 cp "s3://$BUCKET_NAME/$TEST_FILE" "/tmp/downloaded-$TEST_FILE" --endpoint-url "$S3_ENDPOINT"

if [ $? -ne 0 ]; then
    echo "ERROR: Failed to download file"
    exit 1
fi

echo "✓ File downloaded successfully"

# Verify file content
DOWNLOADED_CONTENT=$(cat "/tmp/downloaded-$TEST_FILE")
if [ "$DOWNLOADED_CONTENT" = "$TEST_CONTENT" ]; then
    echo "✓ File content verification successful"
else
    echo "ERROR: File content mismatch"
    echo "Expected: $TEST_CONTENT"
    echo "Got: $DOWNLOADED_CONTENT"
    exit 1
fi

echo "Cleaning up test file..."
aws s3 rm "s3://$BUCKET_NAME/$TEST_FILE" --endpoint-url "$S3_ENDPOINT"

echo ""
echo "Step 5: Testing credential expiration handling..."

# Test with expired/invalid credentials
echo "Testing with invalid session token..."
export AWS_SESSION_TOKEN="invalid-token"

# This should fail
aws s3 ls "s3://$BUCKET_NAME/" --endpoint-url "$S3_ENDPOINT" 2>/dev/null && {
    echo "ERROR: Operation should have failed with invalid token"
    exit 1
} || {
    echo "✓ Invalid credentials properly rejected"
}

echo ""
echo "=========================================="
echo "✅ STS Authentication Test PASSED"
echo "=========================================="
echo "All tests completed successfully:"
echo "✓ Role assumption"
echo "✓ Credential validation"
echo "✓ S3 operations with temporary credentials"
echo "✓ File upload/download"
echo "✓ Content verification"
echo "✓ Invalid credential rejection"
echo ""
echo "The Ozone S3 Gateway STS authentication is working correctly!"
