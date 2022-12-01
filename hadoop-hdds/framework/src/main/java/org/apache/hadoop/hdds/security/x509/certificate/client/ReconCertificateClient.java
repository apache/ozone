/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Certificate client for Recon.
 */
public class ReconCertificateClient  extends CommonCertificateClient {
  private static final Logger LOG =
      LoggerFactory.getLogger(ReconCertificateClient.class);

  public static final String COMPONENT_NAME = "recon";

  public ReconCertificateClient(OzoneConfiguration ozoneConfig,
      String certSerialId) {
    super(ozoneConfig, LOG, certSerialId, COMPONENT_NAME);
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }
}
