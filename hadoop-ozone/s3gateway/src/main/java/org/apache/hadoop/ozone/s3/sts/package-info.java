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

/**
 * STS (Security Token Service) authentication support for Ozone S3 Gateway.
 * 
 * This package provides AWS STS-compatible authentication for the S3 Gateway,
 * allowing it to validate temporary credentials against an external STS server
 * without requiring interaction with the Ozone Manager for credential validation.
 * 
 * Key components:
 * - STSClient: Validates credentials against external STS server
 * - STSAuthenticationFilter: JAX-RS filter for STS authentication
 * - STSCredentials: Model for STS temporary credentials
 * - STSEndpointBase: Enhanced endpoint base class with STS support
 * 
 * Configuration:
 * - ozone.s3g.sts.enabled: Enable/disable STS authentication
 * - ozone.s3g.sts.endpoint: STS server endpoint URL
 * - ozone.s3g.sts.region: AWS region for STS calls
 * - ozone.s3g.sts.cache.ttl: Cache TTL for validation results
 */
package org.apache.hadoop.ozone.s3.sts;

