/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hdds.security.x509.SecurityConfig;

/**
 * Certificate client for OzoneManager.
 */
public class OMCertificateClient extends CommonCertificateClient {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMCertificateClient.class);

  public static final String COMPONENT_NAME = "om";

  public OMCertificateClient(SecurityConfig securityConfig,
      String certSerialId, String localCrlId) {
    super(securityConfig, LOG, certSerialId, COMPONENT_NAME);
    this.setLocalCrlId(localCrlId != null ?
        Long.parseLong(localCrlId) : 0);
  }

  public OMCertificateClient(SecurityConfig securityConfig,
      String certSerialId) {
    this(securityConfig, certSerialId, null);
  }

  public OMCertificateClient(SecurityConfig securityConfig) {
    this(securityConfig, null, null);
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }
}
