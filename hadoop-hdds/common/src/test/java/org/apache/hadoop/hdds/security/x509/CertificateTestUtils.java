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

package org.apache.hadoop.hdds.security.x509;

import java.math.BigInteger;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
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

/**
 * Test utilities to create simple certificates/keys for testing.
 */
public final class CertificateTestUtils {
  private static final String HASH_ALGO = "SHA256WithRSA";

  private CertificateTestUtils() { }

  /**
   * Generates a keypair using the HDDSKeyGenerator with the given config.
   *
   * @param conf the config applies to keys
   *
   * @return a newly generated keypair
   *
   * @throws NoSuchProviderException on wrong security provider in the config
   * @throws NoSuchAlgorithmException on wrong encryption algo in the config
   */
  public static KeyPair aKeyPair(ConfigurationSource conf)
      throws NoSuchProviderException, NoSuchAlgorithmException {
    return new HDDSKeyGenerator(new SecurityConfig(conf)).generateKey();
  }

  /**
   * Creates a self-signed certificate and returns it as an X509Certificate.
   * The given keys and common name are being used in the certificate.
   * The certificate will have its serial id generated based on the hashcode
   * of the public key, and will expire after 1 day.
   *
   * @param keys the keypair to use for the certificate
   * @param commonName the common name used in the certificate
   *
   * @return the X509Certificate representing a self-signed certificate
   *
   * @throws Exception in case any error occurs during the certificate creation
   */
  public static X509Certificate createSelfSignedCert(KeyPair keys,
      String commonName) throws Exception {
    return createSelfSignedCert(keys, commonName, Duration.ofDays(1));
  }

  /**
   * Creates a self-signed certificate and returns it as an X509Certificate.
   * The given keys and common name are being used in the certificate.
   * The certificate will have its serial id generated based on the hashcode
   * of the public key, and will expire after the specified duration.
   *
   * @param keys the keypair to use for the certificate
   * @param commonName the common name used in the certificate
   * @param expiresIn the lifespan of the certificate
   *
   * @return the X509Certificate representing a self-signed certificate
   *
   * @throws Exception in case any error occurs during the certificate creation
   */
  public static X509Certificate createSelfSignedCert(KeyPair keys,
      String commonName, Duration expiresIn) throws Exception {
    return createSelfSignedCert(keys, commonName, expiresIn,
        BigInteger.valueOf(keys.getPublic().hashCode()));
  }

  /**
   * Creates a self-signed certificate and returns it as an X509Certificate.
   * The given keys and common name are being used in the certificate.
   * The certificate will expire after the specified duration and its id
   * will be the specified id.
   *
   * @param keys       the keypair to use for the certificate
   * @param commonName the common name used in the certificate
   * @param expiresIn  the lifespan of the certificate
   * @param certId     the id of the generated certificate
   * @return the X509Certificate representing a self-signed certificate
   * @throws Exception in case any error occurs during the certificate creation
   */
  public static X509Certificate createSelfSignedCert(KeyPair keys,
      String commonName, Duration expiresIn, BigInteger certId)
      throws Exception {
    final Instant now = Instant.now();
    final Date notBefore = Date.from(now);
    final Date notAfter = Date.from(now.plus(expiresIn));
    final ContentSigner contentSigner =
        new JcaContentSignerBuilder(HASH_ALGO).build(keys.getPrivate());
    final X500Name x500Name = new X500Name("CN=" + commonName);

    SubjectKeyIdentifier keyId = subjectKeyIdOf(keys);
    AuthorityKeyIdentifier authorityKeyId = authorityKeyIdOf(keys);
    BasicConstraints constraints = new BasicConstraints(true);

    final X509v3CertificateBuilder certificateBuilder =
        new JcaX509v3CertificateBuilder(
            x500Name,
            certId,
            notBefore,
            notAfter,
            x500Name,
            keys.getPublic()
        );
    certificateBuilder
        .addExtension(Extension.subjectKeyIdentifier, false, keyId)
        .addExtension(Extension.authorityKeyIdentifier, false, authorityKeyId)
        .addExtension(Extension.basicConstraints, true, constraints);

    //TODO: as part of HDDS-10743 ensure that converter is instantiated only once
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
