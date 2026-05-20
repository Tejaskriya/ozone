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

import java.time.Instant;

/**
 * Represents AWS STS temporary credentials.
 */
public class STSCredentials {
  private final String accessKeyId;
  private final String secretAccessKey;
  private final String sessionToken;
  private final Instant expiration;
  private final String userPrincipal;

  public STSCredentials(String accessKeyId, String secretAccessKey, 
                       String sessionToken, Instant expiration, String userPrincipal) {
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expiration = expiration;
    this.userPrincipal = userPrincipal;
  }

  public String getAccessKeyId() {
    return accessKeyId;
  }

  public String getSecretAccessKey() {
    return secretAccessKey;
  }

  public String getSessionToken() {
    return sessionToken;
  }

  public Instant getExpiration() {
    return expiration;
  }

  public String getUserPrincipal() {
    return userPrincipal;
  }

  public boolean isExpired() {
    return Instant.now().isAfter(expiration);
  }

  @Override
  public String toString() {
    return "STSCredentials{" +
        "accessKeyId='" + accessKeyId + '\'' +
        ", sessionToken='" + sessionToken + '\'' +
        ", expiration=" + expiration +
        ", userPrincipal='" + userPrincipal + '\'' +
        '}';
  }
}

