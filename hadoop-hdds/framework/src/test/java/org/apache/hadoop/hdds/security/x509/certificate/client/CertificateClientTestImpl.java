/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.security.x509.certificate.client;

import java.io.IOException;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.ssl.KeyStoresFactory;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.authority.CAType;
import org.apache.hadoop.hdds.security.x509.certificate.authority.DefaultApprover;
import org.apache.hadoop.hdds.security.x509.certificate.authority.profile.DefaultProfile;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.hdds.security.x509.exception.CertificateException;
import org.apache.hadoop.hdds.security.x509.keys.HDDSKeyGenerator;

import org.apache.hadoop.hdds.security.x509.keys.SecurityUtil;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_DEFAULT_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_MAX_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_MAX_DURATION_DEFAULT;
import static org.apache.hadoop.hdds.security.x509.exception.CertificateException.ErrorCode.CRYPTO_SIGNATURE_VERIFICATION_ERROR;

/**
 * Test implementation for CertificateClient. To be used only for test
 * purposes.
 */

public class CertificateClientTestImpl implements CertificateClient {

  private final SecurityConfig securityConfig;
  private KeyPair keyPair;
  private X509Certificate x509Certificate;
  private KeyPair rootKeyPair;
  private X509Certificate rootCert;
  private Set<X509Certificate> rootCerts;

  private HDDSKeyGenerator keyGen;
  private DefaultApprover approver;
  private KeyStoresFactory serverKeyStoresFactory;
  private KeyStoresFactory clientKeyStoresFactory;
  private Map<String, X509Certificate> certificateMap;
  private ScheduledExecutorService executorService;
  private Set<CertificateNotification> notificationReceivers;

  public CertificateClientTestImpl(OzoneConfiguration conf)
      throws Exception {
    this(conf, false);
  }

  public CertificateClientTestImpl(OzoneConfiguration conf, boolean autoRenew)
      throws Exception {
    certificateMap = new ConcurrentHashMap<>();
    securityConfig = new SecurityConfig(conf);
    rootCerts = new HashSet<>();
    keyGen = new HDDSKeyGenerator(securityConfig);
    keyPair = keyGen.generateKey();
    rootKeyPair = keyGen.generateKey();
    LocalDateTime start = LocalDateTime.now();
    String rootCACertDuration = conf.get(HDDS_X509_MAX_DURATION,
        HDDS_X509_MAX_DURATION_DEFAULT);
    LocalDateTime end = start.plus(Duration.parse(rootCACertDuration));

    // Generate RootCA certificate
    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(start)
            .setEndDate(end)
            .setClusterID("cluster1")
            .setKey(rootKeyPair)
            .setSubject("rootCA@localhost")
            .setConfiguration(securityConfig)
            .setScmID("scm1")
            .makeCA();
    rootCert = new JcaX509CertificateConverter().getCertificate(
        builder.build());
    certificateMap.put(rootCert.getSerialNumber().toString(), rootCert);
    rootCerts.add(rootCert);

    // Generate normal certificate, signed by RootCA certificate
    approver = new DefaultApprover(new DefaultProfile(), securityConfig);

    CertificateSignRequest.Builder csrBuilder =
        new CertificateSignRequest.Builder();
    // Get host name.
    csrBuilder.setKey(keyPair)
        .setConfiguration(securityConfig)
        .setScmID("scm1")
        .setClusterID("cluster1")
        .setSubject("localhost")
        .setDigitalSignature(true)
        .setDigitalEncryption(true);

    start = LocalDateTime.now();
    String certDuration = conf.get(HDDS_X509_DEFAULT_DURATION,
        HDDS_X509_DEFAULT_DURATION_DEFAULT);
    X509CertificateHolder certificateHolder =
        approver.sign(securityConfig, rootKeyPair.getPrivate(),
            new X509CertificateHolder(rootCert.getEncoded()),
            Date.from(start.atZone(ZoneId.systemDefault()).toInstant()),
            Date.from(start.plus(Duration.parse(certDuration))
                .atZone(ZoneId.systemDefault()).toInstant()),
            csrBuilder.build(), "scm1", "cluster1",
            String.valueOf(System.nanoTime()));
    x509Certificate =
        new JcaX509CertificateConverter().getCertificate(certificateHolder);
    certificateMap.put(x509Certificate.getSerialNumber().toString(),
        x509Certificate);

    notificationReceivers = new HashSet<>();
    serverKeyStoresFactory = SecurityUtil.getServerKeyStoresFactory(
        securityConfig, this, true);
    clientKeyStoresFactory = SecurityUtil.getClientKeyStoresFactory(
        securityConfig, this, true);

    if (autoRenew) {
      Duration gracePeriod = securityConfig.getRenewalGracePeriod();
      Date expireDate = x509Certificate.getNotAfter();
      LocalDateTime gracePeriodStart = expireDate.toInstant()
          .minus(gracePeriod).atZone(ZoneId.systemDefault()).toLocalDateTime();
      LocalDateTime currentTime = LocalDateTime.now();
      Duration delay = gracePeriodStart.isBefore(currentTime) ? Duration.ZERO :
          Duration.between(currentTime, gracePeriodStart);

      executorService = Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder().setNameFormat("CertificateRenewerService")
              .setDaemon(true).build());
      this.executorService.schedule(new RenewCertTask(),
          delay.toMillis(), TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public PrivateKey getPrivateKey() {
    return keyPair.getPrivate();
  }

  @Override
  public PublicKey getPublicKey() {
    return keyPair.getPublic();
  }

  /**
   * Returns the certificate  of the specified component if it exists on the
   * local system.
   *
   * @return certificate or Null if there is no data.
   */
  @Override
  public X509Certificate getCertificate(String certSerialId)
      throws CertificateException {
    return certificateMap.get(certSerialId);
  }

  @Override
  public CertPath getCertPath() {
    return null;
  }

  @Override
  public X509Certificate getCertificate() {
    return x509Certificate;
  }

  @Override
  public List<X509Certificate> getTrustChain() {
    List<X509Certificate> list = new ArrayList<>();
    list.add(x509Certificate);
    list.add(rootCert);
    return list;
  }

  @Override
  public X509Certificate getCACertificate() {
    return rootCert;
  }

  @Override
  public byte[] signData(byte[] data) throws CertificateException {
    return new byte[0];
  }

  @Override
  public boolean verifySignature(byte[] data, byte[] signature,
      X509Certificate cert) throws CertificateException {
    try {
      Signature sign = Signature.getInstance(securityConfig.getSignatureAlgo(),
          securityConfig.getProvider());
      sign.initVerify(cert);
      sign.update(data);
      return sign.verify(signature);
    } catch (NoSuchAlgorithmException | NoSuchProviderException
             | InvalidKeyException | SignatureException e) {
      System.out.println("Error while signing the stream " + e.getMessage());
      throw new CertificateException("Error while signing the stream", e,
          CRYPTO_SIGNATURE_VERIFICATION_ERROR);
    }
  }

  @Override
  public CertificateSignRequest.Builder getCSRBuilder() {
    return new CertificateSignRequest.Builder();
  }

  @Override
  public String signAndStoreCertificate(PKCS10CertificationRequest request)
      throws CertificateException {
    return null;
  }

  @Override
  public void storeCertificate(String cert, CAType caType)
      throws CertificateException {
  }

  @Override
  public void initWithRecovery() throws IOException {
  }

  @Override
  public String getComponentName() {
    return this.getClass().getSimpleName();
  }

  @Override
  public X509Certificate getRootCACertificate() {
    return rootCert;
  }

  @Override
  public Set<X509Certificate> getAllRootCaCerts() {
    return rootCerts;
  }

  @Override
  public Set<X509Certificate> getAllCaCerts() {
    return rootCerts;
  }

  @Override
  public List<String> getCAList() {
    return null;
  }

  @Override
  public List<String> listCA() throws IOException {
    return null;
  }

  @Override
  public List<String> updateCAList() throws IOException  {
    return null;
  }

  public void renewRootCA() throws Exception {
    LocalDateTime start = LocalDateTime.now();
    Duration rootCACertDuration = securityConfig.getMaxCertificateDuration();
    LocalDateTime end = start.plus(rootCACertDuration);
    rootKeyPair = keyGen.generateKey();
    SelfSignedCertificate.Builder builder =
        SelfSignedCertificate.newBuilder()
            .setBeginDate(start)
            .setEndDate(end)
            .setClusterID("cluster1")
            .setKey(rootKeyPair)
            .setSubject("rootCA-new@localhost")
            .setConfiguration(securityConfig)
            .setScmID("scm1")
            .makeCA(BigInteger.ONE.add(BigInteger.ONE));
    rootCert = new JcaX509CertificateConverter().getCertificate(
        builder.build());
    certificateMap.put(rootCert.getSerialNumber().toString(), rootCert);
    rootCerts.add(rootCert);
  }

  public void renewKey() throws Exception {
    KeyPair newKeyPair = keyGen.generateKey();
    CertificateSignRequest.Builder csrBuilder =
        new CertificateSignRequest.Builder();
    // Get host name.
    csrBuilder.setKey(newKeyPair)
        .setConfiguration(securityConfig)
        .setScmID("scm1")
        .setClusterID("cluster1")
        .setSubject("localhost")
        .setDigitalSignature(true);

    Duration certDuration = securityConfig.getDefaultCertDuration();
    Date start = new Date();
    X509CertificateHolder certificateHolder =
        approver.sign(securityConfig, rootKeyPair.getPrivate(),
            new X509CertificateHolder(rootCert.getEncoded()), start,
            new Date(start.getTime() + certDuration.toMillis()),
            csrBuilder.build(), "scm1", "cluster1",
            String.valueOf(System.nanoTime()));
    X509Certificate newX509Certificate =
        new JcaX509CertificateConverter().getCertificate(certificateHolder);

    // Save the new private key and certificate to file
    // Save certificate and private key to keyStore
    X509Certificate oldCert = x509Certificate;
    keyPair = newKeyPair;
    x509Certificate = newX509Certificate;
    certificateMap.put(x509Certificate.getSerialNumber().toString(),
        x509Certificate);

    // notify notification receivers
    notificationReceivers.forEach(r -> r.notifyCertificateRenewed(this,
        oldCert.getSerialNumber().toString(),
        x509Certificate.getSerialNumber().toString()));
  }

  /**
   * Task to renew certificate.
   */
  public class RenewCertTask implements Runnable {
    @Override
    public void run() {
      try {
        renewRootCA();
        renewKey();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public KeyStoresFactory getServerKeyStoresFactory() {
    return serverKeyStoresFactory;
  }

  @Override
  public KeyStoresFactory getClientKeyStoresFactory() {
    return clientKeyStoresFactory;
  }

  @Override
  public void registerNotificationReceiver(CertificateNotification receiver) {
    synchronized (notificationReceivers) {
      notificationReceivers.add(receiver);
    }
  }

  @Override
  public void registerRootCARotationListener(
      Function<List<X509Certificate>, CompletableFuture<Void>> listener) {
    // we do not have tests that rely on rootCA rotation atm, leaving this
    // implementation blank for now.
  }

  @Override
  public void close() throws IOException {
    if (serverKeyStoresFactory != null) {
      serverKeyStoresFactory.destroy();
    }

    if (clientKeyStoresFactory != null) {
      clientKeyStoresFactory.destroy();
    }

    if (executorService != null) {
      executorService.shutdown();
    }
  }
}
