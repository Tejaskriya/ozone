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

/**
 * Result of STS credential validation.
 */
public class STSValidationResult {
  private final boolean valid;
  private final STSCredentials credentials;
  private final String errorMessage;

  private STSValidationResult(boolean valid, STSCredentials credentials, String errorMessage) {
    this.valid = valid;
    this.credentials = credentials;
    this.errorMessage = errorMessage;
  }

  public static STSValidationResult success(STSCredentials credentials) {
    return new STSValidationResult(true, credentials, null);
  }

  public static STSValidationResult failure(String errorMessage) {
    return new STSValidationResult(false, null, errorMessage);
  }

  public boolean isValid() {
    return valid;
  }

  public STSCredentials getCredentials() {
    return credentials;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  @Override
  public String toString() {
    return "STSValidationResult{" +
        "valid=" + valid +
        ", credentials=" + credentials +
        ", errorMessage='" + errorMessage + '\'' +
        '}';
  }
}

