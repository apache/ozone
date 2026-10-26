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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import org.apache.hadoop.hdds.security.x509.CertificateTestUtils;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.junit.jupiter.api.Test;

/**
 * Tests that {@link CertificateCodec#getCertFactory()} registers BouncyCastle when nothing has registered it yet.
 * Registration is JVM-wide, so this runs in its own class (and so its own JVM).
 */
class TestCertificateCodecProviderRegistration {

  @Test
  void certFactoryRegistersBouncyCastle() throws Exception {
    assertNull(Security.getProvider("BC"), "BouncyCastle registered before the test");
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    KeyPair keys = generator.generateKeyPair();
    X509Certificate cert = CertificateTestUtils.createSelfSignedCert(keys, "ca");

    // the "PEM" cert path encoding is BouncyCastle's
    CertPath path = CertificateCodec.getCertPathFromPemEncodedString(CertificateCodec.getPEMEncodedString(cert));

    assertEquals(cert, CertificateCodec.firstCertificateFrom(path));
    assertNotNull(Security.getProvider("BC"));
  }
}
