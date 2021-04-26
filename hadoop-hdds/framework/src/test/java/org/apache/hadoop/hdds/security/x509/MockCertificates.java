/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.security.x509;

import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v1CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.crypto.util.PrivateKeyFactory;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DefaultDigestAlgorithmIdentifierFinder;
import org.bouncycastle.operator.DefaultSignatureAlgorithmIdentifierFinder;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcRSAContentSignerBuilder;

import static org.junit.Assert.assertTrue;

/**
 * Provides mock instances of certificates for testing.
 */
public final class MockCertificates {

  public static X509Certificate generateExpiredCert(String dn,
      KeyPair pair, String algorithm) throws CertificateException,
      IllegalStateException, IOException, OperatorCreationException {
    Date from = new Date();
    // Set end date same as start date to make sure the cert is expired.
    return generateTestCert(dn, pair, algorithm, from, from);
  }

  public static X509Certificate generateNotValidYetCert(String dn,
      KeyPair pair, String algorithm) throws CertificateException,
      IllegalStateException, IOException, OperatorCreationException {
    Date from = new Date(Instant.now().toEpochMilli() + 100000L);
    Date to = new Date(from.getTime() + 200000L);
    return generateTestCert(dn, pair, algorithm, from, to);
  }

  public static X509Certificate generateTestCert(String dn,
      KeyPair pair, String algorithm, Date from, Date to)
      throws CertificateException, IllegalStateException,
      IOException, OperatorCreationException {
    BigInteger sn = new BigInteger(64, new SecureRandom());
    SubjectPublicKeyInfo subPubKeyInfo = SubjectPublicKeyInfo.getInstance(
        pair.getPublic().getEncoded());
    X500Name subjectDN = new X500Name(dn);
    X509v1CertificateBuilder builder = new X509v1CertificateBuilder(
        subjectDN, sn, from, to, subjectDN, subPubKeyInfo);

    AlgorithmIdentifier sigAlgId =
        new DefaultSignatureAlgorithmIdentifierFinder().find(algorithm);
    AlgorithmIdentifier digAlgId =
        new DefaultDigestAlgorithmIdentifierFinder().find(sigAlgId);
    ContentSigner signer =
        new BcRSAContentSignerBuilder(sigAlgId, digAlgId)
            .build(PrivateKeyFactory.createKey(pair.getPrivate().getEncoded()));
    X509CertificateHolder holder = builder.build(signer);
    return new JcaX509CertificateConverter().getCertificate(holder);
  }

  /**
   * Verify signature using public key.
   */
  public static void verifySignature(String algorithm, PublicKey publicKey,
      byte[] data, byte[] signature) throws Exception {
    Signature impl = Signature.getInstance(algorithm);
    impl.initVerify(publicKey);
    impl.update(data);
    assertTrue(impl.verify(signature));
  }

  private MockCertificates() {
    throw new AssertionError("no instances");
  }
}
