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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.s3.util.S3Utils.wrapOS3Exception;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.inject.Inject;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.ext.Provider;
import java.io.IOException;

/**
 * Filter for STS (Security Token Service) authentication.
 * This filter validates AWS STS temporary credentials against an external STS server
 * without interacting with the Ozone Manager. It runs after the AuthorizationFilter
 * to validate the parsed signature information against the STS server.
 */
@Provider
@PreMatching
@Priority(STSAuthenticationFilter.PRIORITY)
public class STSAuthenticationFilter implements ContainerRequestFilter {
  public static final int PRIORITY = 60; // Run after AuthorizationFilter (50)

  private static final Logger LOG = LoggerFactory.getLogger(STSAuthenticationFilter.class);

  @Inject
  private STSClient stsClient;

  @Inject
  private SignatureInfo signatureInfo;

  @Override
  public void filter(ContainerRequestContext context) throws IOException {
    // Only process if STS is enabled
    if (!stsClient.isEnabled()) {
      LOG.debug("STS authentication is disabled, skipping validation");
      return;
    }

    try {
      // Extract credentials from the signature info
      String accessKeyId = signatureInfo.getAwsAccessId();
      String sessionToken = extractSessionToken(context);

      if (accessKeyId == null || accessKeyId.isEmpty()) {
        LOG.debug("No access key ID found in request");
        throw ACCESS_DENIED;
      }

      // Check if this looks like a temporary credential (has session token)
      if (sessionToken == null || sessionToken.isEmpty()) {
        LOG.debug("No session token found, skipping STS validation for access key: {}", accessKeyId);
        return;
      }

      LOG.debug("Validating STS credentials for access key: {}", accessKeyId);

      // Validate credentials with STS server
      STSValidationResult result = stsClient.validateCredentials(
          accessKeyId, 
          null, // We don't have the secret key in the filter
          sessionToken
      );

      if (!result.isValid()) {
        LOG.warn("STS credential validation failed for access key {}: {}", 
            accessKeyId, result.getErrorMessage());
        throw ACCESS_DENIED;
      }

      // Store validated credentials in request context for later use
      STSCredentials credentials = result.getCredentials();
      context.setProperty("sts.credentials", credentials);
      context.setProperty("sts.user.principal", credentials.getUserPrincipal());

      LOG.debug("STS credentials validated successfully for user: {}", credentials.getUserPrincipal());

    } catch (OS3Exception ex) {
      LOG.debug("STS authentication failed: ", ex);
      throw wrapOS3Exception(ex);
    } catch (Exception e) {
      LOG.error("Error during STS authentication: ", e);
      throw wrapOS3Exception(S3ErrorTable.newError(INTERNAL_ERROR, null, e));
    }
  }

  /**
   * Extract session token from request headers.
   * AWS STS session tokens are typically sent in the X-Amz-Security-Token header.
   */
  private String extractSessionToken(ContainerRequestContext context) {
    // Check for session token in headers
    String sessionToken = context.getHeaderString("X-Amz-Security-Token");
    if (sessionToken != null && !sessionToken.isEmpty()) {
      return sessionToken;
    }

    // Also check query parameters for session token
    if (context.getUriInfo().getQueryParameters().containsKey("X-Amz-Security-Token")) {
      return context.getUriInfo().getQueryParameters().getFirst("X-Amz-Security-Token");
    }

    return null;
  }

  @VisibleForTesting
  public void setStsClient(STSClient stsClient) {
    this.stsClient = stsClient;
  }

  @VisibleForTesting
  public void setSignatureInfo(SignatureInfo signatureInfo) {
    this.signatureInfo = signatureInfo;
  }
}

