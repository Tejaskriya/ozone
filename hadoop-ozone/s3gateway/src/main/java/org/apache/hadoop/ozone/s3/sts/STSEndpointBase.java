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

import org.apache.hadoop.ozone.s3.endpoint.EndpointBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.ws.rs.core.Context;
import javax.ws.rs.container.ContainerRequestContext;

/**
 * Enhanced EndpointBase that supports STS authentication.
 * This class extends the standard EndpointBase to work with STS credentials
 * when STS authentication is enabled, while maintaining compatibility with
 * the standard authentication flow.
 */
public abstract class STSEndpointBase extends EndpointBase {
  private static final Logger LOG = LoggerFactory.getLogger(STSEndpointBase.class);

  @Inject
  private STSClient stsClient;

  @Context
  private ContainerRequestContext requestContext;

  private STSCredentials stsCredentials;
  private String effectiveUserPrincipal;

  @PostConstruct
  @Override
  public void initialization() {
    // Check if STS credentials are available in the request context
    if (stsClient.isEnabled() && requestContext != null) {
      stsCredentials = (STSCredentials) requestContext.getProperty("sts.credentials");
      effectiveUserPrincipal = (String) requestContext.getProperty("sts.user.principal");
      
      if (stsCredentials != null) {
        LOG.debug("Using STS credentials for user: {}", effectiveUserPrincipal);
        // Initialize with STS-specific logic
        initializeWithSTS();
        return;
      }
    }

    // Fall back to standard initialization
    LOG.debug("Using standard authentication flow");
    super.initialization();
  }

  /**
   * Initialize the endpoint with STS credentials.
   * This method sets up the authentication context using validated STS credentials
   * without requiring interaction with the Ozone Manager for credential validation.
   */
  private void initializeWithSTS() {
    // Create S3Auth with STS credentials
    // Note: We use the validated user principal from STS
    org.apache.hadoop.ozone.om.protocol.S3Auth s3Auth = 
        new org.apache.hadoop.ozone.om.protocol.S3Auth(
            signatureInfo.getStringToSign(),
            signatureInfo.getSignature(),
            stsCredentials.getAccessKeyId(),
            effectiveUserPrincipal
        );

    LOG.debug("STS S3 access id: {}, user principal: {}", 
        s3Auth.getAccessID(), s3Auth.getUserPrincipal());

    // Set up the client protocol with STS authentication
    org.apache.hadoop.ozone.client.protocol.ClientProtocol clientProtocol =
        getClient().getObjectStore().getClientProxy();
    clientProtocol.setThreadLocalS3Auth(s3Auth);
    clientProtocol.setIsS3Request(true);

    // Call the abstract init method
    init();
  }

  /**
   * Get the STS credentials if available.
   */
  protected STSCredentials getStsCredentials() {
    return stsCredentials;
  }

  /**
   * Get the effective user principal (from STS or standard auth).
   */
  protected String getEffectiveUserPrincipal() {
    if (effectiveUserPrincipal != null) {
      return effectiveUserPrincipal;
    }
    // Fall back to standard auth user principal
    return signatureInfo.getAwsAccessId();
  }

  /**
   * Check if this request is using STS authentication.
   */
  protected boolean isUsingSTS() {
    return stsCredentials != null;
  }

  /**
   * Check if STS authentication is enabled.
   */
  protected boolean isStsEnabled() {
    return stsClient != null && stsClient.isEnabled();
  }
}

