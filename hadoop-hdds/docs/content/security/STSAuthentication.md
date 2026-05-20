---
title: "STS Authentication for S3 Gateway"
date: "2024-10-07"
summary: Configure and use AWS STS (Security Token Service) authentication with Ozone S3 Gateway
weight: 6
menu:
   main:
      parent: Security
icon: key
---
<!---
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# STS Authentication for S3 Gateway

The Ozone S3 Gateway supports AWS STS (Security Token Service) authentication, allowing clients to use temporary credentials issued by an external STS server. This feature enables the S3 Gateway to authenticate users without requiring interaction with the Ozone Manager for credential validation.

## Overview

STS authentication provides several benefits:

- **Temporary Credentials**: Use time-limited access keys and session tokens
- **External Authentication**: Integrate with existing AWS STS-compatible identity providers
- **Reduced OM Load**: Authentication happens at the S3 Gateway level without OM interaction
- **AWS Compatibility**: Full compatibility with AWS STS credential format and protocols

## Architecture

When STS authentication is enabled, the authentication flow works as follows:

1. Client sends S3 request with temporary credentials (access key, secret key, session token)
2. S3 Gateway's `STSAuthenticationFilter` extracts the credentials from the request
3. Gateway validates credentials against the configured STS server using `GetCallerIdentity` API
4. If valid, the request proceeds with the authenticated user context
5. If invalid, the request is rejected with appropriate error

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│   S3 Client │───▶│   S3 Gateway     │───▶│ STS Server  │
│             │    │ (STS Auth Filter)│    │             │
└─────────────┘    └──────────────────┘    └─────────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │  Ozone Cluster   │
                   │ (Data Operations)│
                   └──────────────────┘
```

## Configuration

### Enable STS Authentication

Add the following configuration to your `ozone-site.xml`:

```xml
<configuration>
  <!-- Enable STS authentication -->
  <property>
    <name>ozone.s3g.sts.enabled</name>
    <value>true</value>
    <description>Enable STS authentication for S3 Gateway</description>
  </property>

  <!-- STS server endpoint -->
  <property>
    <name>ozone.s3g.sts.endpoint</name>
    <value>https://sts.amazonaws.com</value>
    <description>STS server endpoint URL</description>
  </property>

  <!-- AWS region for STS calls -->
  <property>
    <name>ozone.s3g.sts.region</name>
    <value>us-east-1</value>
    <description>AWS region for STS API calls</description>
  </property>
</configuration>
```

### Advanced Configuration Options

```xml
<configuration>
  <!-- Connection timeout for STS calls -->
  <property>
    <name>ozone.s3g.sts.connection.timeout</name>
    <value>5000</value>
    <description>Connection timeout for STS server calls in milliseconds</description>
  </property>

  <!-- Read timeout for STS calls -->
  <property>
    <name>ozone.s3g.sts.read.timeout</name>
    <value>10000</value>
    <description>Read timeout for STS server calls in milliseconds</description>
  </property>

  <!-- Cache TTL for validation results -->
  <property>
    <name>ozone.s3g.sts.cache.ttl</name>
    <value>300000</value>
    <description>Cache TTL for STS validation results in milliseconds (5 minutes)</description>
  </property>

  <!-- Cache size for validation results -->
  <property>
    <name>ozone.s3g.sts.cache.size</name>
    <value>1000</value>
    <description>Maximum number of entries in STS validation cache</description>
  </property>
</configuration>
```

## Usage Examples

### Using AWS CLI with STS Credentials

1. **Obtain temporary credentials from your STS server:**

```bash
# Example using AWS STS (replace with your STS server)
aws sts assume-role \
  --role-arn "arn:aws:iam::123456789012:role/MyRole" \
  --role-session-name "ozone-session" \
  --endpoint-url https://your-sts-server.com
```

2. **Configure AWS CLI with temporary credentials:**

```bash
export AWS_ACCESS_KEY_ID="ASIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."
```

3. **Use AWS CLI with Ozone S3 Gateway:**

```bash
# List buckets
aws s3 ls --endpoint-url http://localhost:9878

# Create bucket
aws s3 mb s3://my-bucket --endpoint-url http://localhost:9878

# Upload file
aws s3 cp myfile.txt s3://my-bucket/ --endpoint-url http://localhost:9878
```

### Using SDK with STS Credentials

#### Java SDK Example

```java
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

// Create credentials with session token
AwsSessionCredentials credentials = AwsSessionCredentials.create(
    "ASIA...",           // Access Key ID
    "...",               // Secret Access Key
    "..."                // Session Token
);

// Create S3 client pointing to Ozone Gateway
S3Client s3Client = S3Client.builder()
    .credentialsProvider(StaticCredentialsProvider.create(credentials))
    .endpointOverride(URI.create("http://localhost:9878"))
    .region(Region.US_EAST_1)
    .build();

// Use the client
s3Client.listBuckets();
```

#### Python SDK Example

```python
import boto3

# Create S3 client with temporary credentials
s3_client = boto3.client(
    's3',
    aws_access_key_id='ASIA...',
    aws_secret_access_key='...',
    aws_session_token='...',
    endpoint_url='http://localhost:9878',
    region_name='us-east-1'
)

# Use the client
response = s3_client.list_buckets()
```

## Security Considerations

### STS Server Requirements

Your STS server must:

- Support the AWS STS `GetCallerIdentity` API
- Return valid XML responses in AWS STS format
- Handle AWS Signature Version 4 authentication
- Provide appropriate HTTPS endpoints for production use

### Network Security

- Use HTTPS for STS server communication in production
- Configure appropriate firewall rules between S3 Gateway and STS server
- Consider network latency impact on authentication performance

### Credential Management

- Temporary credentials should have appropriate expiration times
- Monitor and log authentication failures
- Implement proper credential rotation policies
- Use least-privilege principles for STS roles

## Monitoring and Troubleshooting

### Logging

Enable debug logging for STS authentication:

```xml
<property>
  <name>log4j.logger.org.apache.hadoop.ozone.s3.sts</name>
  <value>DEBUG</value>
</property>
```

### Common Issues

1. **STS endpoint not reachable**
   - Verify network connectivity to STS server
   - Check firewall rules and DNS resolution

2. **Invalid credentials error**
   - Verify STS server is returning proper responses
   - Check credential expiration times
   - Validate session token format

3. **Performance issues**
   - Adjust cache settings for better performance
   - Monitor STS server response times
   - Consider increasing connection pool sizes

### Metrics and Monitoring

The S3 Gateway provides metrics for STS authentication:

- `sts_validation_requests_total`: Total STS validation requests
- `sts_validation_failures_total`: Failed STS validations
- `sts_cache_hits_total`: Cache hits for STS validations
- `sts_response_time`: STS server response times

## Compatibility

### AWS STS Compatibility

The implementation supports:

- AWS STS GetCallerIdentity API
- AWS Signature Version 4
- Standard AWS credential formats
- Session tokens and temporary credentials

### Limitations

- Only supports GetCallerIdentity for validation (not full STS API)
- Requires external STS server setup
- Limited to HTTP/HTTPS communication with STS server

## Migration Guide

### From Standard Authentication

1. Set up your STS server infrastructure
2. Configure STS settings in `ozone-site.xml`
3. Enable STS authentication (`ozone.s3g.sts.enabled=true`)
4. Update client applications to use temporary credentials
5. Test thoroughly before production deployment

### Rollback Plan

To disable STS authentication:

1. Set `ozone.s3g.sts.enabled=false`
2. Restart S3 Gateway services
3. Clients will fall back to standard authentication

## Best Practices

1. **Use HTTPS**: Always use HTTPS for STS server communication in production
2. **Monitor Performance**: Track STS validation latency and success rates
3. **Cache Tuning**: Adjust cache settings based on your workload patterns
4. **Credential Rotation**: Implement proper credential rotation policies
5. **Error Handling**: Implement proper error handling in client applications
6. **Testing**: Thoroughly test STS integration before production deployment

For more information about Ozone security, see:
- [Securing Ozone]({{< ref "SecureOzone" >}})
- [Securing S3]({{< ref "SecuringS3" >}})
- [Network Ports]({{< ref "NetworkPorts" >}})

