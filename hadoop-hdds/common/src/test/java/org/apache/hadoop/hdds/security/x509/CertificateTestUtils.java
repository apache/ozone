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

package org.apache.hadoop.hdds.security.x509;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;
import org.bouncycastle.asn1.oiw.OIWObjectIdentifiers;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.asn1.x509.AuthorityKeyIdentifier;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.SubjectKeyIdentifier;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509ExtensionUtils;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.DigestCalculator;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.bc.BcDigestCalculatorProvider;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

/**
 * Test utilities to create simple certificates/keys for testing.
 */
public final class CertificateTestUtils {
  private CertificateTestUtils() { }

  private static final String HASH_ALGO = "SHA256WithRSA";

  public static KeyPair aKeyPair(ConfigurationSource conf)
      throws NoSuchProviderException, NoSuchAlgorithmException {
    return new HDDSKeyGenerator(new SecurityConfig(conf)).generateKey();
  }

  public static X509Certificate createSelfSignedCert(
      KeyPair keys, String commonName
  ) throws Exception {
    return createSelfSignedCert(keys, commonName, Duration.ofDays(1));
  }

  public static X509Certificate createSelfSignedCert(
      KeyPair keys, String commonName, Duration exiresIn
  ) throws Exception {
    final Instant now = Instant.now();
    final Date notBefore = Date.from(now);
    final Date notAfter = Date.from(now.plus(exiresIn));
    final ContentSigner contentSigner =
        new JcaContentSignerBuilder(HASH_ALGO).build(keys.getPrivate());
    final X500Name x500Name = new X500Name("CN=" + commonName);

    SubjectKeyIdentifier keyId = subjectKeyIdOf(keys);
    AuthorityKeyIdentifier authorityKeyId = authorityKeyIdOf(keys);
    BasicConstraints constraints = new BasicConstraints(true);

    final X509v3CertificateBuilder certificateBuilder =
        new JcaX509v3CertificateBuilder(
            x500Name,
            BigInteger.valueOf(keys.getPublic().hashCode()),
            notBefore,
            notAfter,
            x500Name,
            keys.getPublic()
        );
    certificateBuilder
        .addExtension(Extension.subjectKeyIdentifier, false, keyId)
        .addExtension(Extension.authorityKeyIdentifier, false, authorityKeyId)
        .addExtension(Extension.basicConstraints, true, constraints);

    return new JcaX509CertificateConverter()
        .setProvider(new BouncyCastleProvider())
        .getCertificate(certificateBuilder.build(contentSigner));
  }

  private static SubjectKeyIdentifier subjectKeyIdOf(KeyPair keys)
      throws Exception {
    return extensionUtil().createSubjectKeyIdentifier(pubKeyInfo(keys));
  }

  private static AuthorityKeyIdentifier authorityKeyIdOf(KeyPair keys)
      throws Exception {
    return extensionUtil().createAuthorityKeyIdentifier(pubKeyInfo(keys));
  }

  private static SubjectPublicKeyInfo pubKeyInfo(KeyPair keys) {
    return SubjectPublicKeyInfo.getInstance(keys.getPublic().getEncoded());
  }

  private static X509ExtensionUtils extensionUtil()
      throws OperatorCreationException {
    DigestCalculator digest =
        new BcDigestCalculatorProvider()
            .get(new AlgorithmIdentifier(OIWObjectIdentifiers.idSHA1));

    return new X509ExtensionUtils(digest);
  }
}
