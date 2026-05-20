/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Mock AWS STS Server for testing Ozone S3 Gateway STS authentication
 * 
 * This server implements the AWS STS API endpoints needed for testing:
 * - AssumeRole: Issues temporary credentials
 * - GetCallerIdentity: Validates credentials and returns user info
 */

const express = require('express');
const bodyParser = require('body-parser');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

// In-memory storage for issued credentials
const issuedCredentials = new Map();
const userSessions = new Map();

// Mock user database - in real STS server, this would be integrated with identity provider
const mockUsers = {
  'testuser': {
    userId: 'AIDACKCEVSQ6C2EXAMPLE',
    arn: 'arn:aws:iam::123456789012:user/testuser',
    account: '123456789012',
    roles: ['arn:aws:iam::123456789012:role/OzoneS3User']
  },
  'admin': {
    userId: 'AIDACKCEVSQ6C2ADMIN',
    arn: 'arn:aws:iam::123456789012:user/admin',
    account: '123456789012',
    roles: ['arn:aws:iam::123456789012:role/OzoneS3Admin']
  }
};

// Utility functions
function generateAccessKeyId() {
  return 'ASIA' + Math.random().toString(36).substring(2, 15).toUpperCase();
}

function generateSecretAccessKey() {
  return Math.random().toString(36).substring(2, 42);
}

function generateSessionToken() {
  return 'FwoGZXIvYXdzE' + Math.random().toString(36).substring(2, 50);
}

function getExpirationTime(durationSeconds = 3600) {
  const expiration = new Date();
  expiration.setSeconds(expiration.getSeconds() + durationSeconds);
  return expiration.toISOString();
}

function parseFormData(body) {
  const params = {};
  const pairs = body.split('&');
  for (const pair of pairs) {
    const [key, value] = pair.split('=');
    params[decodeURIComponent(key)] = decodeURIComponent(value || '');
  }
  return params;
}

function buildXMLResponse(action, result) {
  const requestId = uuidv4();
  return `<?xml version="1.0" encoding="UTF-8"?>
<${action}Response xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <${action}Result>
${result}
  </${action}Result>
  <ResponseMetadata>
    <RequestId>${requestId}</RequestId>
  </ResponseMetadata>
</${action}Response>`;
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ 
    status: 'healthy', 
    service: 'mock-sts-server',
    timestamp: new Date().toISOString(),
    activeCredentials: issuedCredentials.size
  });
});

// AWS STS AssumeRole endpoint
app.post('/', (req, res) => {
  const contentType = req.headers['content-type'] || '';
  let params;

  if (contentType.includes('application/x-www-form-urlencoded')) {
    params = parseFormData(req.body.toString());
  } else {
    params = req.body;
  }

  const action = params.Action;
  console.log(`[${new Date().toISOString()}] STS Request: ${action}`, params);

  switch (action) {
    case 'AssumeRole':
      handleAssumeRole(req, res, params);
      break;
    case 'GetCallerIdentity':
      handleGetCallerIdentity(req, res, params);
      break;
    default:
      res.status(400).send(`<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidAction</Code>
    <Message>Invalid action: ${action}</Message>
  </Error>
  <RequestId>${uuidv4()}</RequestId>
</ErrorResponse>`);
  }
});

function handleAssumeRole(req, res, params) {
  const roleArn = params.RoleArn;
  const roleSessionName = params.RoleSessionName || 'ozone-s3-session';
  const durationSeconds = parseInt(params.DurationSeconds) || 3600;

  // Validate role ARN (simplified validation)
  if (!roleArn || !roleArn.startsWith('arn:aws:iam::')) {
    return res.status(400).send(`<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidParameterValue</Code>
    <Message>Invalid role ARN: ${roleArn}</Message>
  </Error>
  <RequestId>${uuidv4()}</RequestId>
</ErrorResponse>`);
  }

  // Generate temporary credentials
  const accessKeyId = generateAccessKeyId();
  const secretAccessKey = generateSecretAccessKey();
  const sessionToken = generateSessionToken();
  const expiration = getExpirationTime(durationSeconds);

  // Store credentials for validation
  const credentialInfo = {
    accessKeyId,
    secretAccessKey,
    sessionToken,
    expiration: new Date(expiration),
    roleArn,
    roleSessionName,
    userId: 'AROACKCEVSQ6C2EXAMPLE',
    arn: `${roleArn}/assumed-role/${roleSessionName}`,
    account: '123456789012'
  };

  issuedCredentials.set(accessKeyId, credentialInfo);
  issuedCredentials.set(sessionToken, credentialInfo);

  console.log(`[${new Date().toISOString()}] Issued credentials for role: ${roleArn}`);

  const result = `    <Credentials>
      <AccessKeyId>${accessKeyId}</AccessKeyId>
      <SecretAccessKey>${secretAccessKey}</SecretAccessKey>
      <SessionToken>${sessionToken}</SessionToken>
      <Expiration>${expiration}</Expiration>
    </Credentials>
    <AssumedRoleUser>
      <AssumedRoleId>AROACKCEVSQ6C2EXAMPLE:${roleSessionName}</AssumedRoleId>
      <Arn>${roleArn}/assumed-role/${roleSessionName}</Arn>
    </AssumedRoleUser>`;

  res.set('Content-Type', 'text/xml');
  res.send(buildXMLResponse('AssumeRole', result));
}

function handleGetCallerIdentity(req, res, params) {
  // Extract credentials from Authorization header or session token
  const authHeader = req.headers.authorization || '';
  const sessionToken = req.headers['x-amz-security-token'] || params['X-Amz-Security-Token'];

  console.log(`[${new Date().toISOString()}] GetCallerIdentity request:`, {
    authHeader: authHeader.substring(0, 50) + '...',
    sessionToken: sessionToken ? sessionToken.substring(0, 20) + '...' : 'none'
  });

  let credentialInfo = null;

  // Try to find credentials by session token first
  if (sessionToken) {
    credentialInfo = issuedCredentials.get(sessionToken);
  }

  // If not found by session token, try to extract access key from auth header
  if (!credentialInfo && authHeader) {
    const credentialMatch = authHeader.match(/Credential=([^/,]+)/);
    if (credentialMatch) {
      const accessKeyId = credentialMatch[1];
      credentialInfo = issuedCredentials.get(accessKeyId);
    }
  }

  // Check if credentials are expired
  if (credentialInfo && credentialInfo.expiration < new Date()) {
    console.log(`[${new Date().toISOString()}] Credentials expired for: ${credentialInfo.accessKeyId}`);
    issuedCredentials.delete(credentialInfo.accessKeyId);
    issuedCredentials.delete(credentialInfo.sessionToken);
    credentialInfo = null;
  }

  if (!credentialInfo) {
    console.log(`[${new Date().toISOString()}] Invalid or expired credentials`);
    return res.status(403).send(`<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidUserID.NotFound</Code>
    <Message>The security token included in the request is invalid.</Message>
  </Error>
  <RequestId>${uuidv4()}</RequestId>
</ErrorResponse>`);
  }

  console.log(`[${new Date().toISOString()}] Valid credentials found for: ${credentialInfo.arn}`);

  const result = `    <UserId>${credentialInfo.userId}</UserId>
    <Account>${credentialInfo.account}</Account>
    <Arn>${credentialInfo.arn}</Arn>`;

  res.set('Content-Type', 'text/xml');
  res.send(buildXMLResponse('GetCallerIdentity', result));
}

// Error handling middleware
app.use((err, req, res, next) => {
  console.error(`[${new Date().toISOString()}] Error:`, err);
  res.status(500).send(`<?xml version="1.0" encoding="UTF-8"?>
<ErrorResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <Error>
    <Type>Receiver</Type>
    <Code>InternalError</Code>
    <Message>An internal error occurred</Message>
  </Error>
  <RequestId>${uuidv4()}</RequestId>
</ErrorResponse>`);
});

// Cleanup expired credentials periodically
setInterval(() => {
  const now = new Date();
  let cleaned = 0;
  
  for (const [key, cred] of issuedCredentials.entries()) {
    if (cred.expiration < now) {
      issuedCredentials.delete(key);
      cleaned++;
    }
  }
  
  if (cleaned > 0) {
    console.log(`[${new Date().toISOString()}] Cleaned up ${cleaned} expired credentials`);
  }
}, 60000); // Clean up every minute

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[${new Date().toISOString()}] Mock STS Server running on port ${PORT}`);
  console.log(`[${new Date().toISOString()}] Health check: http://localhost:${PORT}/health`);
  console.log(`[${new Date().toISOString()}] STS endpoint: http://localhost:${PORT}/`);
});
