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

package org.apache.hadoop.hdds.scm.metadata;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.CodecTestUtil;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to test {@link X509CertificateCodec}.
 */
public class TestX509CertificateCodec {
  static final Logger LOG = LoggerFactory.getLogger(
      TestX509CertificateCodec.class);

  private final Codec<X509Certificate> oldCodec
      = OldX509CertificateCodecForTesting.get();
  private final Codec<X509Certificate> newCodec
      = X509CertificateCodec.get();

  public static KeyPair genKeyPair(String algorithm, int keySize)
      throws NoSuchAlgorithmException {
    LOG.info("genKeyPair: {}, keySize={}", algorithm, keySize);
    final KeyPairGenerator keyGen = KeyPairGenerator.getInstance(algorithm);
    keyGen.initialize(keySize);
    return keyGen.genKeyPair();
  }

  @BeforeAll
  public static void initSecurityConfig() {
    SecurityConfig.initSecurityProvider(new OzoneConfiguration());
  }

  @Test
  public void testRSA() throws Exception {
    for (int n = 512; n <= 4096; n <<= 1) {
      runTestRSA(n);
    }
  }

  public void runTestRSA(int keySize) throws Exception {
    final KeyPair rsa = genKeyPair("RSA", keySize);
    final int days = ThreadLocalRandom.current().nextInt(100) + 1;
    final X509Certificate x509 = KeyStoreTestUtil.generateCertificate(
        "CN=testRSA" + keySize, rsa, days, "SHA256withRSA");
    System.out.println(CertificateCodec.getPEMEncodedString(x509));
    CodecTestUtil.runTest(newCodec, x509, null, oldCodec);
  }
}
