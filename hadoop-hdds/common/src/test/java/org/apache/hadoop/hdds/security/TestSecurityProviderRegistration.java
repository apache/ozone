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

package org.apache.hadoop.hdds.security;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.X509Certificate;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests that the client path does not register the BouncyCastle provider, and that code which names the provider
 * gets it registered. Registration is JVM-wide, so the tests run in order, in this class's own JVM.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class TestSecurityProviderRegistration {

  private static final String BC = "BC";

  @Test
  @Order(1)
  void clientPathDoesNotRegisterBouncyCastle() throws Exception {
    assertNull(Security.getProvider(BC), "BouncyCastle registered before the test");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);

    new SecurityConfig(conf);
    assertTrue(OzoneSecurityUtil.isSecurityEnabled(conf));

    // a client reading a CA certificate, as from ServiceInfoEx.getCaCertificates()
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair keys = generator.generateKeyPair();
    X509Certificate original = CertificateTestUtils.createSelfSignedCert(keys, "ca");
    X509Certificate parsed = CertificateCodec.getX509Certificate(CertificateCodec.getPEMEncodedString(original));
    assertArrayEquals(original.getEncoded(), parsed.getEncoded());
    assertEquals(original, parsed);

    assertNull(Security.getProvider(BC), "The client path must not register BouncyCastle");
  }

  @Test
  @Order(2)
  void getProviderRegistersBouncyCastle() {
    SecurityConfig securityConfig = new SecurityConfig(new OzoneConfiguration());
    assertNull(Security.getProvider(BC));

    assertEquals(BC, securityConfig.getProvider());
    assertNotNull(Security.getProvider(BC), "getProvider() must register the provider it names");
  }
}
