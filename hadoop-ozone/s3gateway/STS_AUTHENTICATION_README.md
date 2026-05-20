# STS Authentication for Ozone S3 Gateway

This document describes the implementation of AWS STS (Security Token Service) authentication for the Ozone S3 Gateway.

## Overview

The STS authentication feature allows the Ozone S3 Gateway to authenticate users using temporary credentials issued by an external AWS STS-compatible server, without requiring interaction with the Ozone Manager for credential validation.

## Architecture

### Components

1. **STSClient** (`org.apache.hadoop.ozone.s3.sts.STSClient`)
   - Validates credentials against external STS server
   - Implements caching for performance
   - Handles HTTP communication with STS server

2. **STSAuthenticationFilter** (`org.apache.hadoop.ozone.s3.sts.STSAuthenticationFilter`)
   - JAX-RS filter that intercepts requests
   - Extracts STS credentials from requests
   - Validates credentials using STSClient
   - Priority 60 (runs after AuthorizationFilter)

3. **STSCredentials** (`org.apache.hadoop.ozone.s3.sts.STSCredentials`)
   - Model class for STS temporary credentials
   - Includes access key, secret key, session token, and expiration

4. **STSEndpointBase** (`org.apache.hadoop.ozone.s3.sts.STSEndpointBase`)
   - Enhanced endpoint base class with STS support
   - Provides STS-aware initialization
   - Maintains compatibility with standard authentication

5. **STSInfoEndpoint** (`org.apache.hadoop.ozone.s3.sts.STSInfoEndpoint`)
   - Debugging and information endpoint
   - Provides STS status and health checks

### Authentication Flow

```
Client Request with STS Credentials
         │
         ▼
AuthorizationFilter (Priority 50)
  - Parses AWS signature
  - Extracts access key ID
         │
         ▼
STSAuthenticationFilter (Priority 60)
  - Checks if STS enabled
  - Extracts session token
  - Validates with STS server
  - Caches result
         │
         ▼
Endpoint Processing
  - Uses validated user principal
  - Proceeds with Ozone operations
```

## Configuration

### Required Configuration

```xml
<!-- Enable STS authentication -->
<property>
  <name>ozone.s3g.sts.enabled</name>
  <value>true</value>
</property>

<!-- STS server endpoint -->
<property>
  <name>ozone.s3g.sts.endpoint</name>
  <value>https://sts.amazonaws.com</value>
</property>
```

### Optional Configuration

```xml
<!-- AWS region for STS calls -->
<property>
  <name>ozone.s3g.sts.region</name>
  <value>us-east-1</value>
</property>

<!-- Connection timeout (ms) -->
<property>
  <name>ozone.s3g.sts.connection.timeout</name>
  <value>5000</value>
</property>

<!-- Read timeout (ms) -->
<property>
  <name>ozone.s3g.sts.read.timeout</name>
  <value>10000</value>
</property>

<!-- Cache TTL (ms) -->
<property>
  <name>ozone.s3g.sts.cache.ttl</name>
  <value>300000</value>
</property>

<!-- Cache size -->
<property>
  <name>ozone.s3g.sts.cache.size</name>
  <value>1000</value>
</property>
```

## Implementation Details

### STS Validation Process

1. **Credential Extraction**: The filter extracts the session token from:
   - `X-Amz-Security-Token` header
   - `X-Amz-Security-Token` query parameter

2. **STS Server Call**: Makes HTTP POST to STS endpoint with:
   - Action: `GetCallerIdentity`
   - Version: `2011-06-15`
   - Authorization headers (if available)
   - Session token header

3. **Response Parsing**: Parses XML response to extract:
   - User ARN
   - User ID
   - Account ID

4. **Caching**: Results are cached using:
   - Key: `accessKeyId:sessionTokenHash`
   - TTL: Configurable (default 5 minutes)
   - Size limit: Configurable (default 1000 entries)

### Error Handling

- **Network Errors**: Logged and returned as validation failures
- **Invalid Responses**: Parsed and appropriate errors returned
- **Timeouts**: Configurable connection and read timeouts
- **Cache Misses**: Gracefully handled with STS server calls

### Security Considerations

1. **HTTPS**: Always use HTTPS for STS server communication in production
2. **Credential Storage**: Secret keys are never stored or logged
3. **Session Tokens**: Only session tokens are used for validation
4. **Caching**: Cached results respect credential expiration times

## Testing

### Unit Tests

- `TestSTSClient`: Tests STS client functionality
- `TestSTSAuthenticationFilter`: Tests filter behavior
- Mock-based testing for HTTP interactions

### Integration Testing

Use the STS info endpoint for testing:

```bash
# Check STS status
curl -H "Authorization: AWS4-HMAC-SHA256 Credential=ASIA.../..." \
     -H "X-Amz-Security-Token: ..." \
     http://localhost:9878/sts/info

# Health check
curl http://localhost:9878/sts/health
```

### Example Client Usage

```bash
# Using AWS CLI with temporary credentials
export AWS_ACCESS_KEY_ID="ASIA..."
export AWS_SECRET_ACCESS_KEY="..."
export AWS_SESSION_TOKEN="..."

aws s3 ls --endpoint-url http://localhost:9878
```

## Performance Considerations

### Caching Strategy

- **Cache Key**: Combination of access key ID and session token hash
- **TTL**: Balances security and performance (default 5 minutes)
- **Size Management**: LRU-style eviction when cache is full
- **Expiration**: Respects credential expiration times

### Network Optimization

- **Connection Pooling**: HTTP client uses connection pooling
- **Timeouts**: Configurable to prevent hanging requests
- **Async Processing**: Consider async validation for high-throughput scenarios

### Monitoring

- Log STS validation requests and failures
- Monitor cache hit rates
- Track STS server response times
- Alert on validation failure spikes

## Troubleshooting

### Common Issues

1. **STS Server Unreachable**
   - Check network connectivity
   - Verify STS endpoint URL
   - Check firewall rules

2. **Invalid Credentials**
   - Verify STS server responses
   - Check credential expiration
   - Validate session token format

3. **Performance Issues**
   - Adjust cache settings
   - Monitor STS server latency
   - Check network bandwidth

### Debug Logging

Enable debug logging for STS components:

```xml
<property>
  <name>log4j.logger.org.apache.hadoop.ozone.s3.sts</name>
  <value>DEBUG</value>
</property>
```

### Metrics

Monitor these key metrics:
- STS validation request count
- STS validation failure rate
- Cache hit ratio
- STS server response time
- Authentication success rate

## Future Enhancements

### Potential Improvements

1. **Async Validation**: Non-blocking credential validation
2. **Multiple STS Servers**: Load balancing across STS endpoints
3. **Advanced Caching**: Distributed cache for multi-instance deployments
4. **Metrics Integration**: Prometheus/JMX metrics export
5. **Circuit Breaker**: Fail-fast pattern for STS server issues

### AWS Compatibility

- Support for additional STS APIs
- Enhanced signature validation
- Regional STS endpoint support
- Cross-account role assumption

## Migration Guide

### From Standard Authentication

1. Deploy STS server infrastructure
2. Configure STS settings in ozone-site.xml
3. Enable STS authentication
4. Update client applications
5. Monitor and validate functionality
6. Gradually migrate users

### Rollback Procedure

1. Set `ozone.s3g.sts.enabled=false`
2. Restart S3 Gateway services
3. Clients fall back to standard authentication
4. Remove STS configuration if needed

## Security Best Practices

1. **Use HTTPS**: Always use HTTPS for STS communication
2. **Network Security**: Secure network between Gateway and STS server
3. **Credential Rotation**: Implement proper credential rotation
4. **Monitoring**: Monitor authentication patterns and failures
5. **Access Control**: Limit STS server access appropriately
6. **Audit Logging**: Enable comprehensive audit logging

For more information, see the full documentation at `hadoop-hdds/docs/content/security/STSAuthentication.md`.

