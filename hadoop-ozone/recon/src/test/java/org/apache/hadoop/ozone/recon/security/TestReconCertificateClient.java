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

package org.apache.hadoop.ozone.recon.security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_METADATA_DIR_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.net.InetAddress;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CsrTestUtil;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.apache.hadoop.hdds.security.x509.keys.KeyStorage;
import org.apache.hadoop.ozone.recon.scm.ReconStorageConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

/**
 * Tests {@link ReconCertificateClient}.
 */
public class TestReconCertificateClient {

  /** A user@FQDN longer than 64 characters, as on Kubernetes, is replaced by recon@UUID in the CSR. */
  @Test
  public void testCsrCommonNameForLongHostName(@TempDir Path metaDir) throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();
    config.set(HDDS_METADATA_DIR_NAME, metaDir.toString());
    SecurityConfig securityConfig = new SecurityConfig(config);
    new KeyStorage(securityConfig, ReconCertificateClient.COMPONENT_NAME)
        .storeKeyPair(new HDDSKeyGenerator(securityConfig).generateKey());
    String reconId = UUID.randomUUID().toString();
    ReconStorageConfig storage = mock(ReconStorageConfig.class);
    when(storage.getReconId()).thenReturn(reconId);
    when(storage.getClusterID()).thenReturn(UUID.randomUUID().toString());

    InetAddress localHost = mock(InetAddress.class);
    when(localHost.getCanonicalHostName())
        .thenReturn("aaaa-ozone-000-recon-0.aaa-ozone-000-recon-headless.aaaa-data.svc.cluster.local");
    try (MockedStatic<InetAddress> inetAddress = mockStatic(InetAddress.class, CALLS_REAL_METHODS);
        ReconCertificateClient client = new ReconCertificateClient(securityConfig, null, storage, null, null)) {
      inetAddress.when(InetAddress::getLocalHost).thenReturn(localHost);
      assertEquals("recon@" + reconId, CsrTestUtil.getCsrCommonName(client));
    }
  }
}
