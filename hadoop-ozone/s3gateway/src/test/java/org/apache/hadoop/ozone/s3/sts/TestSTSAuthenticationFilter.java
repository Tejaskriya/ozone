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

import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test cases for STSAuthenticationFilter.
 */
public class TestSTSAuthenticationFilter {

  @Mock
  private STSClient mockStsClient;

  @Mock
  private SignatureInfo mockSignatureInfo;

  @Mock
  private ContainerRequestContext mockContext;

  @Mock
  private UriInfo mockUriInfo;

  private STSAuthenticationFilter filter;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    filter = new STSAuthenticationFilter();
    filter.setStsClient(mockStsClient);
    filter.setSignatureInfo(mockSignatureInfo);
  }

  @Test
  public void testFilterStsDisabled() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(false);

    // Should not throw any exception and should not call validation
    filter.filter(mockContext);

    verify(mockStsClient, never()).validateCredentials(any(), any(), any());
  }

  @Test
  public void testFilterNoAccessKeyId() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(true);
    when(mockSignatureInfo.getAwsAccessId()).thenReturn(null);

    assertThrows(WebApplicationException.class, () -> filter.filter(mockContext));
  }

  @Test
  public void testFilterNoSessionToken() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(true);
    when(mockSignatureInfo.getAwsAccessId()).thenReturn("ASIAIOSFODNN7EXAMPLE");
    when(mockContext.getHeaderString("X-Amz-Security-Token")).thenReturn(null);
    when(mockContext.getUriInfo()).thenReturn(mockUriInfo);
    
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);

    // Should not throw exception and should not call validation (no session token)
    filter.filter(mockContext);

    verify(mockStsClient, never()).validateCredentials(any(), any(), any());
  }

  @Test
  public void testFilterValidCredentials() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(true);
    when(mockSignatureInfo.getAwsAccessId()).thenReturn("ASIAIOSFODNN7EXAMPLE");
    when(mockContext.getHeaderString("X-Amz-Security-Token")).thenReturn("sessiontoken");

    STSCredentials credentials = new STSCredentials(
        "ASIAIOSFODNN7EXAMPLE",
        "secretkey",
        "sessiontoken",
        Instant.now().plusSeconds(3600),
        "arn:aws:iam::123456789012:user/testuser"
    );
    STSValidationResult validResult = STSValidationResult.success(credentials);

    when(mockStsClient.validateCredentials(eq("ASIAIOSFODNN7EXAMPLE"), eq(null), eq("sessiontoken")))
        .thenReturn(validResult);

    filter.filter(mockContext);

    verify(mockStsClient).validateCredentials(eq("ASIAIOSFODNN7EXAMPLE"), eq(null), eq("sessiontoken"));
    verify(mockContext).setProperty(eq("sts.credentials"), eq(credentials));
    verify(mockContext).setProperty(eq("sts.user.principal"), eq("arn:aws:iam::123456789012:user/testuser"));
  }

  @Test
  public void testFilterInvalidCredentials() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(true);
    when(mockSignatureInfo.getAwsAccessId()).thenReturn("INVALIDKEY");
    when(mockContext.getHeaderString("X-Amz-Security-Token")).thenReturn("invalidsession");

    STSValidationResult invalidResult = STSValidationResult.failure("Invalid credentials");

    when(mockStsClient.validateCredentials(eq("INVALIDKEY"), eq(null), eq("invalidsession")))
        .thenReturn(invalidResult);

    assertThrows(WebApplicationException.class, () -> filter.filter(mockContext));

    verify(mockStsClient).validateCredentials(eq("INVALIDKEY"), eq(null), eq("invalidsession"));
    verify(mockContext, never()).setProperty(eq("sts.credentials"), any());
  }

  @Test
  public void testFilterSessionTokenFromQueryParam() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(true);
    when(mockSignatureInfo.getAwsAccessId()).thenReturn("ASIAIOSFODNN7EXAMPLE");
    when(mockContext.getHeaderString("X-Amz-Security-Token")).thenReturn(null);
    when(mockContext.getUriInfo()).thenReturn(mockUriInfo);

    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.add("X-Amz-Security-Token", "sessiontoken");
    when(mockUriInfo.getQueryParameters()).thenReturn(queryParams);

    STSCredentials credentials = new STSCredentials(
        "ASIAIOSFODNN7EXAMPLE",
        "secretkey",
        "sessiontoken",
        Instant.now().plusSeconds(3600),
        "arn:aws:iam::123456789012:user/testuser"
    );
    STSValidationResult validResult = STSValidationResult.success(credentials);

    when(mockStsClient.validateCredentials(eq("ASIAIOSFODNN7EXAMPLE"), eq(null), eq("sessiontoken")))
        .thenReturn(validResult);

    filter.filter(mockContext);

    verify(mockStsClient).validateCredentials(eq("ASIAIOSFODNN7EXAMPLE"), eq(null), eq("sessiontoken"));
    verify(mockContext).setProperty(eq("sts.credentials"), eq(credentials));
  }

  @Test
  public void testFilterException() throws IOException {
    when(mockStsClient.isEnabled()).thenReturn(true);
    when(mockSignatureInfo.getAwsAccessId()).thenReturn("ASIAIOSFODNN7EXAMPLE");
    when(mockContext.getHeaderString("X-Amz-Security-Token")).thenReturn("sessiontoken");

    when(mockStsClient.validateCredentials(any(), any(), any()))
        .thenThrow(new RuntimeException("STS server error"));

    assertThrows(WebApplicationException.class, () -> filter.filter(mockContext));

    verify(mockStsClient).validateCredentials(eq("ASIAIOSFODNN7EXAMPLE"), eq(null), eq("sessiontoken"));
  }
}

