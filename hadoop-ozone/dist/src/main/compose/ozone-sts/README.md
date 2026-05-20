# Ozone S3 Gateway STS Authentication Test

This Docker Compose setup provides a complete testing environment for the Ozone S3 Gateway STS (Security Token Service) authentication feature.

## Overview

This test environment includes:

- **Ozone Cluster**: Complete Ozone cluster with SCM, OM, DataNode, and S3 Gateway
- **Mock STS Server**: AWS-compatible STS server for issuing temporary credentials
- **AWS CLI Container**: Pre-configured environment for running S3 operations
- **Test Scripts**: Automated tests for the complete STS authentication flow

## Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│   AWS CLI   │───▶│   S3 Gateway     │───▶│ STS Server  │
│  Container  │    │ (STS Auth Filter)│    │ (Mock)      │
└─────────────┘    └──────────────────┘    └─────────────┘
                            │
                            ▼
                   ┌──────────────────┐
                   │  Ozone Cluster   │
                   │ (SCM/OM/DN)      │
                   └──────────────────┘
```

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Built Ozone distribution (or set `OZONE_RUNNER_IMAGE` environment variable)

### Running the Tests

```bash
# From the ozone-sts directory
./test.sh
```

This will:
1. Start the complete environment
2. Wait for all services to be ready
3. Run the STS authentication test
4. Clean up and generate a report

### Manual Testing

You can also run individual components manually:

```bash
# Start the environment
docker-compose up -d

# Check service health
curl http://localhost:8080/health  # STS server
curl http://localhost:9878         # S3 Gateway

# Run tests manually
docker-compose exec aws-cli /scripts/test-sts-auth.sh

# Clean up
docker-compose down
```

## Test Scenarios

The test script covers the following scenarios:

### 1. **Role Assumption**
- Calls STS `AssumeRole` API to get temporary credentials
- Validates the response format and credential structure
- Extracts Access Key ID, Secret Access Key, and Session Token

### 2. **Credential Validation**
- Uses `GetCallerIdentity` to verify the issued credentials
- Confirms the user identity and permissions

### 3. **S3 Operations with Temporary Credentials**
- Creates an S3 bucket using temporary credentials
- Uploads a test file
- Downloads the file and verifies content
- Lists bucket contents

### 4. **Error Handling**
- Tests invalid/expired credential rejection
- Verifies proper error responses

## STS Server Requirements

The mock STS server needs to be aware of the following details to properly authenticate requests:

### 1. **User Identity Information**
```javascript
{
  userId: 'AIDACKCEVSQ6C2EXAMPLE',
  arn: 'arn:aws:iam::123456789012:user/testuser',
  account: '123456789012',
  roles: ['arn:aws:iam::123456789012:role/OzoneS3User']
}
```

### 2. **Role Definitions**
- Role ARNs that can be assumed
- Associated permissions and policies
- Session duration limits

### 3. **Credential Management**
- Issued credential tracking
- Expiration time management
- Session token validation

### 4. **AWS API Compatibility**
- **AssumeRole**: Issues temporary credentials
- **GetCallerIdentity**: Validates credentials and returns user info
- Proper XML response formats
- Standard AWS error codes and messages

## Configuration

### S3 Gateway STS Configuration

The S3 Gateway is configured with the following STS settings:

```xml
<property>
  <name>ozone.s3g.sts.enabled</name>
  <value>true</value>
</property>

<property>
  <name>ozone.s3g.sts.endpoint</name>
  <value>http://sts-server:8080</value>
</property>

<property>
  <name>ozone.s3g.sts.region</name>
  <value>us-east-1</value>
</property>

<property>
  <name>ozone.s3g.sts.cache.ttl</name>
  <value>300000</value>
</property>

<property>
  <name>ozone.s3g.sts.cache.size</name>
  <value>1000</value>
</property>
```

### Mock STS Server Features

- **In-memory credential storage**: Tracks issued credentials
- **Automatic cleanup**: Removes expired credentials
- **Health monitoring**: Provides health check endpoint
- **Request logging**: Logs all STS requests for debugging
- **AWS-compatible responses**: Returns proper XML responses

## Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   # Check container logs
   docker-compose logs sts-server
   docker-compose logs s3g
   ```

2. **STS authentication failures**
   ```bash
   # Check STS server logs
   docker-compose logs sts-server
   
   # Verify STS server health
   curl http://localhost:8080/health
   ```

3. **S3 operations failing**
   ```bash
   # Check S3 Gateway logs
   docker-compose logs s3g
   
   # Verify credentials are valid
   aws sts get-caller-identity --endpoint-url http://localhost:8080
   ```

### Debug Mode

Enable debug logging by setting environment variables:

```bash
export OZONE_LOG_LEVEL=DEBUG
export STS_LOG_LEVEL=debug
docker-compose up
```

## Integration with Real STS Servers

To integrate with a real STS server instead of the mock:

1. **Update docker-compose.yaml**:
   ```yaml
   environment:
     OZONE-SITE.XML_ozone.s3g.sts.endpoint: "https://your-sts-server.com"
   ```

2. **Configure authentication**:
   - Set up proper IAM roles and policies
   - Configure credential providers
   - Update role ARNs in test scripts

3. **Update test scripts**:
   - Modify role ARNs to match your environment
   - Update user credentials and authentication method
   - Adjust test expectations based on your STS server responses

## Security Considerations

- The mock STS server is for **testing only**
- Use proper authentication in production environments
- Implement proper credential rotation and management
- Use HTTPS for STS communication in production
- Follow AWS security best practices for role definitions

## Files Structure

```
ozone-sts/
├── docker-compose.yaml          # Main compose configuration
├── docker-config               # Ozone configuration
├── test.sh                     # Main test runner
├── README.md                   # This file
├── sts-server/                 # Mock STS server
│   ├── package.json
│   └── server.js
├── test-scripts/               # Test scripts
│   └── test-sts-auth.sh
└── aws-config/                 # AWS CLI configuration
    ├── config
    └── credentials
```
