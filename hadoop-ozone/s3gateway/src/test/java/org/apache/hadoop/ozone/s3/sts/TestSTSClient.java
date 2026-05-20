/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.s3.sts;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.s3.S3GatewayConfigKeys;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test cases for STSClient.
 */
public class TestSTSClient {

  private Configuration conf;
  private STSClient stsClient;

  @BeforeEach
  public void setUp() {
    conf = new Configuration();
    conf.setBoolean(S3GatewayConfigKeys.OZONE_S3G_STS_ENABLED_KEY, true);
    conf.set(S3GatewayConfigKeys.OZONE_S3G_STS_ENDPOINT_KEY, "https://sts.example.com");
    conf.set(S3GatewayConfigKeys.OZONE_S3G_STS_REGION_KEY, "us-east-1");
    conf.setLong(S3GatewayConfigKeys.OZONE_S3G_STS_CACHE_TTL_KEY, 300000);
    conf.setInt(S3GatewayConfigKeys.OZONE_S3G_STS_CACHE_SIZE_KEY, 100);
  }

  @Test
  public void testIsEnabled() {
    stsClient = new STSClient(conf);
    assertTrue(stsClient.isEnabled());

    conf.setBoolean(S3GatewayConfigKeys.OZONE_S3G_STS_ENABLED_KEY, false);
    stsClient = new STSClient(conf);
    assertFalse(stsClient.isEnabled());
  }

  @Test
  public void testValidateCredentialsSuccess() throws Exception {
    stsClient = new STSClient(conf);

    String successResponse = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
        "<GetCallerIdentityResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\n" +
        "  <GetCallerIdentityResult>\n" +
        "    <Arn>arn:aws:iam::123456789012:user/testuser</Arn>\n" +
        "    <UserId>AIDACKCEVSQ6C2EXAMPLE</UserId>\n" +
        "    <Account>123456789012</Account>\n" +
        "  </GetCallerIdentityResult>\n" +
        "  <ResponseMetadata>\n" +
        "    <RequestId>01234567-89ab-cdef-0123-456789abcdef</RequestId>\n" +
        "  </ResponseMetadata>\n" +
        "</GetCallerIdentityResponse>";

    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockStatusLine.getStatusCode()).thenReturn(200);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockHttpClient.execute(any())).thenReturn(mockResponse);

    try (MockedStatic<EntityUtils> mockedEntityUtils = mockStatic(EntityUtils.class)) {
      mockedEntityUtils.when(() -> EntityUtils.toString(mockEntity)).thenReturn(successResponse);

      // Use reflection to set the mock HTTP client
      java.lang.reflect.Field httpClientField = STSClient.class.getDeclaredField("httpClient");
      httpClientField.setAccessible(true);
      httpClientField.set(stsClient, mockHttpClient);

      STSValidationResult result = stsClient.validateCredentials(
          "ASIAIOSFODNN7EXAMPLE", 
          "secretkey", 
          "sessiontoken"
      );

      assertTrue(result.isValid());
      assertNotNull(result.getCredentials());
      assertEquals("ASIAIOSFODNN7EXAMPLE", result.getCredentials().getAccessKeyId());
      assertEquals("arn:aws:iam::123456789012:user/testuser", result.getCredentials().getUserPrincipal());
      assertFalse(result.getCredentials().isExpired());
    }
  }

  @Test
  public void testValidateCredentialsFailure() throws Exception {
    stsClient = new STSClient(conf);

    CloseableHttpClient mockHttpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse mockResponse = mock(CloseableHttpResponse.class);
    StatusLine mockStatusLine = mock(StatusLine.class);
    HttpEntity mockEntity = mock(HttpEntity.class);

    when(mockStatusLine.getStatusCode()).thenReturn(403);
    when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
    when(mockResponse.getEntity()).thenReturn(mockEntity);
    when(mockHttpClient.execute(any())).thenReturn(mockResponse);

    try (MockedStatic<EntityUtils> mockedEntityUtils = mockStatic(EntityUtils.class)) {
      mockedEntityUtils.when(() -> EntityUtils.toString(mockEntity)).thenReturn("Access Denied");

      // Use reflection to set the mock HTTP client
      java.lang.reflect.Field httpClientField = STSClient.class.getDeclaredField("httpClient");
      httpClientField.setAccessible(true);
      httpClientField.set(stsClient, mockHttpClient);

      STSValidationResult result = stsClient.validateCredentials(
          "INVALIDKEY", 
          "invalidsecret", 
          "invalidsession"
      );

      assertFalse(result.isValid());
      assertNull(result.getCredentials());
      assertNotNull(result.getErrorMessage());
      assertTrue(result.getErrorMessage().contains("403"));
    }
  }

  @Test
  public void testValidateCredentialsNoEndpoint() {
    conf.unset(S3GatewayConfigKeys.OZONE_S3G_STS_ENDPOINT_KEY);
    stsClient = new STSClient(conf);

    STSValidationResult result = stsClient.validateCredentials(
        "ASIAIOSFODNN7EXAMPLE", 
        "secretkey", 
        "sessiontoken"
    );

    assertFalse(result.isValid());
    assertNull(result.getCredentials());
    assertEquals("STS endpoint not configured", result.getErrorMessage());
  }

  @Test
  public void testSTSCredentialsExpiration() {
    Instant pastExpiration = Instant.now().minusSeconds(3600); // 1 hour ago
    Instant futureExpiration = Instant.now().plusSeconds(3600); // 1 hour from now

    STSCredentials expiredCredentials = new STSCredentials(
        "ASIAIOSFODNN7EXAMPLE",
        "secretkey",
        "sessiontoken",
        pastExpiration,
        "arn:aws:iam::123456789012:user/testuser"
    );

    STSCredentials validCredentials = new STSCredentials(
        "ASIAIOSFODNN7EXAMPLE",
        "secretkey",
        "sessiontoken",
        futureExpiration,
        "arn:aws:iam::123456789012:user/testuser"
    );

    assertTrue(expiredCredentials.isExpired());
    assertFalse(validCredentials.isExpired());
  }

  @Test
  public void testSTSValidationResult() {
    STSCredentials credentials = new STSCredentials(
        "ASIAIOSFODNN7EXAMPLE",
        "secretkey",
        "sessiontoken",
        Instant.now().plusSeconds(3600),
        "arn:aws:iam::123456789012:user/testuser"
    );

    STSValidationResult successResult = STSValidationResult.success(credentials);
    assertTrue(successResult.isValid());
    assertNotNull(successResult.getCredentials());
    assertNull(successResult.getErrorMessage());

    STSValidationResult failureResult = STSValidationResult.failure("Invalid credentials");
    assertFalse(failureResult.isValid());
    assertNull(failureResult.getCredentials());
    assertEquals("Invalid credentials", failureResult.getErrorMessage());
  }
}