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
# Manual STS Testing Script
# This script demonstrates individual steps of the STS authentication process
# Useful for debugging and understanding the flow
#

set -e

STS_ENDPOINT="http://sts-server:8080"
S3_ENDPOINT="http://s3g:9878"
ROLE_ARN="arn:aws:iam::123456789012:role/OzoneS3User"

echo "Manual STS Authentication Test"
echo "=============================="
echo ""

echo "1. Check STS Server Health"
echo "--------------------------"
curl -s "$STS_ENDPOINT/health" | jq '.'
echo ""

echo "2. Assume Role (Get Temporary Credentials)"
echo "------------------------------------------"
echo "Requesting temporary credentials for role: $ROLE_ARN"

ASSUME_ROLE_RESPONSE=$(aws sts assume-role \
    --role-arn "$ROLE_ARN" \
    --role-session-name "manual-test-session" \
    --duration-seconds 3600 \
    --endpoint-url "$STS_ENDPOINT" \
    --output json)

echo "Response:"
echo "$ASSUME_ROLE_RESPONSE" | jq '.'
echo ""

# Extract credentials
ACCESS_KEY_ID=$(echo "$ASSUME_ROLE_RESPONSE" | jq -r '.Credentials.AccessKeyId')
SECRET_ACCESS_KEY=$(echo "$ASSUME_ROLE_RESPONSE" | jq -r '.Credentials.SecretAccessKey')
SESSION_TOKEN=$(echo "$ASSUME_ROLE_RESPONSE" | jq -r '.Credentials.SessionToken')

echo "Extracted Credentials:"
echo "Access Key ID: $ACCESS_KEY_ID"
echo "Secret Access Key: ${SECRET_ACCESS_KEY:0:10}..."
echo "Session Token: ${SESSION_TOKEN:0:20}..."
echo ""

echo "3. Set Environment Variables"
echo "----------------------------"
export AWS_ACCESS_KEY_ID="$ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$SECRET_ACCESS_KEY"
export AWS_SESSION_TOKEN="$SESSION_TOKEN"

echo "AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID"
echo "AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:0:10}..."
echo "AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN:0:20}..."
echo ""

echo "4. Verify Credentials with GetCallerIdentity"
echo "--------------------------------------------"
CALLER_IDENTITY=$(aws sts get-caller-identity \
    --endpoint-url "$STS_ENDPOINT" \
    --output json)

echo "Response:"
echo "$CALLER_IDENTITY" | jq '.'
echo ""

echo "5. Test S3 Operations"
echo "--------------------"
BUCKET_NAME="manual-test-bucket-$(date +%s)"

echo "Creating bucket: $BUCKET_NAME"
aws s3 mb "s3://$BUCKET_NAME" --endpoint-url "$S3_ENDPOINT"
echo ""

echo "Listing buckets:"
aws s3 ls --endpoint-url "$S3_ENDPOINT"
echo ""

echo "Creating test file..."
echo "Hello from manual STS test!" > /tmp/manual-test.txt

echo "Uploading test file..."
aws s3 cp /tmp/manual-test.txt "s3://$BUCKET_NAME/manual-test.txt" --endpoint-url "$S3_ENDPOINT"
echo ""

echo "Listing bucket contents:"
aws s3 ls "s3://$BUCKET_NAME/" --endpoint-url "$S3_ENDPOINT"
echo ""

echo "Downloading test file..."
aws s3 cp "s3://$BUCKET_NAME/manual-test.txt" /tmp/downloaded-manual-test.txt --endpoint-url "$S3_ENDPOINT"

echo "Verifying file content:"
cat /tmp/downloaded-manual-test.txt
echo ""

echo "6. Cleanup"
echo "----------"
echo "Removing test file..."
aws s3 rm "s3://$BUCKET_NAME/manual-test.txt" --endpoint-url "$S3_ENDPOINT"

echo "Removing bucket..."
aws s3 rb "s3://$BUCKET_NAME" --endpoint-url "$S3_ENDPOINT"

echo ""
echo "✅ Manual STS test completed successfully!"
echo ""
echo "Key points demonstrated:"
echo "• STS server issued temporary credentials"
echo "• Credentials were validated successfully"
echo "• S3 operations worked with temporary credentials"
echo "• File upload/download completed successfully"
