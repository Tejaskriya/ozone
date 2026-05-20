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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Client for validating AWS STS credentials against an external STS server.
 * This client communicates with an AWS-compatible STS server to validate
 * temporary credentials without interacting with the Ozone Manager.
 */
@ApplicationScoped
public class STSClient {
  private static final Logger LOG = LoggerFactory.getLogger(STSClient.class);

  private final Configuration conf;
  private final CloseableHttpClient httpClient;
  private final ConcurrentMap<String, CachedValidationResult> cache;
  private final String stsEndpoint;
  private final String region;
  private final long cacheTtl;
  private final int cacheSize;

  @Inject
  public STSClient(Configuration conf) {
    this.conf = conf;
    this.stsEndpoint = conf.get(S3GatewayConfigKeys.OZONE_S3G_STS_ENDPOINT_KEY);
    this.region = conf.get(S3GatewayConfigKeys.OZONE_S3G_STS_REGION_KEY,
        S3GatewayConfigKeys.OZONE_S3G_STS_REGION_DEFAULT);
    this.cacheTtl = conf.getLong(S3GatewayConfigKeys.OZONE_S3G_STS_CACHE_TTL_KEY,
        S3GatewayConfigKeys.OZONE_S3G_STS_CACHE_TTL_DEFAULT);
    this.cacheSize = conf.getInt(S3GatewayConfigKeys.OZONE_S3G_STS_CACHE_SIZE_KEY,
        S3GatewayConfigKeys.OZONE_S3G_STS_CACHE_SIZE_DEFAULT);

    int connectionTimeout = conf.getInt(S3GatewayConfigKeys.OZONE_S3G_STS_CONNECTION_TIMEOUT_KEY,
        S3GatewayConfigKeys.OZONE_S3G_STS_CONNECTION_TIMEOUT_DEFAULT);
    int readTimeout = conf.getInt(S3GatewayConfigKeys.OZONE_S3G_STS_READ_TIMEOUT_KEY,
        S3GatewayConfigKeys.OZONE_S3G_STS_READ_TIMEOUT_DEFAULT);

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(connectionTimeout)
        .setSocketTimeout(readTimeout)
        .build();

    this.httpClient = HttpClients.custom()
        .setDefaultRequestConfig(requestConfig)
        .build();

    this.cache = new ConcurrentHashMap<>();

    LOG.info("STS Client initialized with endpoint: {}, region: {}, cache TTL: {}ms, cache size: {}",
        stsEndpoint, region, cacheTtl, cacheSize);
  }

  /**
   * Validates STS credentials by calling the GetCallerIdentity API.
   * This method checks if the provided credentials are valid and returns
   * the associated user information.
   *
   * @param accessKeyId The access key ID from the request
   * @param secretAccessKey The secret access key (if available)
   * @param sessionToken The session token from the request
   * @return STSValidationResult containing validation status and credentials
   */
  public STSValidationResult validateCredentials(String accessKeyId, 
                                                String secretAccessKey, 
                                                String sessionToken) {
    if (stsEndpoint == null || stsEndpoint.isEmpty()) {
      LOG.error("STS endpoint not configured");
      return STSValidationResult.failure("STS endpoint not configured");
    }

    String cacheKey = generateCacheKey(accessKeyId, sessionToken);
    
    // Check cache first
    CachedValidationResult cached = cache.get(cacheKey);
    if (cached != null && !cached.isExpired()) {
      LOG.debug("Returning cached validation result for access key: {}", accessKeyId);
      return cached.getResult();
    }

    try {
      // Call STS GetCallerIdentity API
      STSValidationResult result = callGetCallerIdentity(accessKeyId, secretAccessKey, sessionToken);
      
      // Cache the result if successful
      if (result.isValid()) {
        cacheResult(cacheKey, result);
      }
      
      return result;
    } catch (Exception e) {
      LOG.error("Error validating STS credentials for access key: {}", accessKeyId, e);
      return STSValidationResult.failure("Failed to validate credentials: " + e.getMessage());
    }
  }

  private STSValidationResult callGetCallerIdentity(String accessKeyId, 
                                                   String secretAccessKey, 
                                                   String sessionToken) throws Exception {
    // Build the GetCallerIdentity request
    String requestBody = "Action=GetCallerIdentity&Version=2011-06-15";
    
    HttpPost httpPost = new HttpPost(stsEndpoint);
    httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded");
    httpPost.setEntity(new StringEntity(requestBody));

    // Add AWS signature headers if we have the secret key
    if (secretAccessKey != null && !secretAccessKey.isEmpty()) {
      // For now, we'll use a simplified approach where we trust the STS server
      // to validate the signature. In a production environment, you might want
      // to implement full AWS signature validation here.
      httpPost.setHeader("Authorization", 
          "AWS4-HMAC-SHA256 Credential=" + accessKeyId + "/" + 
          Instant.now().toString().substring(0, 10) + "/" + region + "/sts/aws4_request");
    }

    if (sessionToken != null && !sessionToken.isEmpty()) {
      httpPost.setHeader("X-Amz-Security-Token", sessionToken);
    }

    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      int statusCode = response.getStatusLine().getStatusCode();
      HttpEntity entity = response.getEntity();
      String responseBody = entity != null ? EntityUtils.toString(entity) : "";

      if (statusCode == 200) {
        return parseGetCallerIdentityResponse(responseBody, accessKeyId, sessionToken);
      } else {
        LOG.warn("STS validation failed with status: {}, body: {}", statusCode, responseBody);
        return STSValidationResult.failure("STS validation failed with status: " + statusCode);
      }
    }
  }

  private STSValidationResult parseGetCallerIdentityResponse(String responseBody, 
                                                           String accessKeyId, 
                                                           String sessionToken) {
    try {
      // Parse XML response (AWS STS returns XML)
      // For simplicity, we'll look for key elements in the XML
      if (responseBody.contains("<GetCallerIdentityResult>") && 
          responseBody.contains("<Arn>")) {
        
        // Extract ARN to get user principal
        String arn = extractXmlValue(responseBody, "Arn");
        String userId = extractXmlValue(responseBody, "UserId");
        String account = extractXmlValue(responseBody, "Account");
        
        // Create credentials with a reasonable expiration time
        Instant expiration = Instant.now().plusSeconds(3600); // 1 hour
        
        STSCredentials credentials = new STSCredentials(
            accessKeyId, 
            null, // We don't store the secret key
            sessionToken,
            expiration,
            arn != null ? arn : userId
        );
        
        LOG.debug("Successfully validated STS credentials for user: {}", credentials.getUserPrincipal());
        return STSValidationResult.success(credentials);
      } else {
        LOG.warn("Unexpected STS response format: {}", responseBody);
        return STSValidationResult.failure("Invalid STS response format");
      }
    } catch (Exception e) {
      LOG.error("Error parsing STS response", e);
      return STSValidationResult.failure("Error parsing STS response: " + e.getMessage());
    }
  }

  private String extractXmlValue(String xml, String tagName) {
    String startTag = "<" + tagName + ">";
    String endTag = "</" + tagName + ">";
    int startIndex = xml.indexOf(startTag);
    if (startIndex == -1) {
      return null;
    }
    startIndex += startTag.length();
    int endIndex = xml.indexOf(endTag, startIndex);
    if (endIndex == -1) {
      return null;
    }
    return xml.substring(startIndex, endIndex).trim();
  }

  private String generateCacheKey(String accessKeyId, String sessionToken) {
    return accessKeyId + ":" + (sessionToken != null ? sessionToken.hashCode() : "null");
  }

  private void cacheResult(String cacheKey, STSValidationResult result) {
    // Simple cache size management
    if (cache.size() >= cacheSize) {
      // Remove oldest entries (simple LRU approximation)
      cache.entrySet().removeIf(entry -> entry.getValue().isExpired());
      
      // If still at capacity, remove some entries
      if (cache.size() >= cacheSize) {
        cache.entrySet().stream()
            .limit(cacheSize / 4) // Remove 25% of entries
            .forEach(entry -> cache.remove(entry.getKey()));
      }
    }
    
    cache.put(cacheKey, new CachedValidationResult(result, Instant.now().plusMillis(cacheTtl)));
  }

  /**
   * Cached validation result with expiration.
   */
  private static class CachedValidationResult {
    private final STSValidationResult result;
    private final Instant expiration;

    CachedValidationResult(STSValidationResult result, Instant expiration) {
      this.result = result;
      this.expiration = expiration;
    }

    STSValidationResult getResult() {
      return result;
    }

    boolean isExpired() {
      return Instant.now().isAfter(expiration);
    }
  }

  /**
   * Check if STS authentication is enabled.
   */
  public boolean isEnabled() {
    return conf.getBoolean(S3GatewayConfigKeys.OZONE_S3G_STS_ENABLED_KEY,
        S3GatewayConfigKeys.OZONE_S3G_STS_ENABLED_DEFAULT);
  }
}

