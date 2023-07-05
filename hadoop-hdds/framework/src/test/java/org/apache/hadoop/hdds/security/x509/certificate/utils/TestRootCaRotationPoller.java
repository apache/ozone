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
package org.apache.hadoop.hdds.security.x509.certificate.utils;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.RootCaRotationPoller;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL;

/**
 * Test for Root Ca Rotation polling mechanism on client side.
 */
public class TestRootCaRotationPoller {

  private SecurityConfig secConf;

  @Mock
  private SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL, "PT1s");
    secConf = new SecurityConfig(conf);
  }

  @Test
  public void testPollerDoesNotInvokeRootCaProcessor() throws Exception {
    X509Certificate knownCert = generateX509Cert(
        LocalDateTime.now(), Duration.ofSeconds(50));
    HashSet<X509Certificate> knownCerts = new HashSet<>();
    knownCerts.add(knownCert);
    List<String> certsFromScm = new ArrayList<>();
    certsFromScm.add(CertificateCodec.getPEMEncodedString(knownCert));
    RootCaRotationPoller poller = new RootCaRotationPoller(secConf,
        knownCerts, scmSecurityClient);

    Mockito.when(scmSecurityClient.getAllRootCaCertificates())
        .thenReturn(certsFromScm);
    AtomicBoolean atomicBoolean = new AtomicBoolean();
    atomicBoolean.set(false);
    poller.addRootCARotationProcessor(
        certificates -> CompletableFuture.supplyAsync(() -> {
          atomicBoolean.set(true);
          Assertions.assertEquals(certificates.size(), 2);
          return null;
        }));
    poller.run();
    Assertions.assertThrows(TimeoutException.class, () ->
        GenericTestUtils.waitFor(atomicBoolean::get, 50, 5000));
  }

  @Test
  public void testPollerInvokesRootCaProcessors() throws Exception {
    X509Certificate knownCert = generateX509Cert(
        LocalDateTime.now(), Duration.ofSeconds(50));
    X509Certificate newRootCa = generateX509Cert(
        LocalDateTime.now(), Duration.ofSeconds(50));
    HashSet<X509Certificate> knownCerts = new HashSet<>();
    knownCerts.add(knownCert);
    List<String> certsFromScm = new ArrayList<>();
    certsFromScm.add(CertificateCodec.getPEMEncodedString(knownCert));
    certsFromScm.add(CertificateCodec.getPEMEncodedString(newRootCa));
    RootCaRotationPoller poller = new RootCaRotationPoller(secConf,
        knownCerts, scmSecurityClient);
    poller.run();
    Mockito.when(scmSecurityClient.getAllRootCaCertificates())
        .thenReturn(certsFromScm);
    AtomicBoolean atomicBoolean = new AtomicBoolean();
    atomicBoolean.set(false);
    poller.addRootCARotationProcessor(
        certificates -> CompletableFuture.supplyAsync(() -> {
          atomicBoolean.set(true);
          Assertions.assertEquals(certificates.size(), 2);
          return null;
        }));
    GenericTestUtils.waitFor(atomicBoolean::get, 50, 5000);
  }

  private X509Certificate generateX509Cert(
      LocalDateTime startDate, Duration certLifetime) throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    LocalDateTime start = startDate == null ? LocalDateTime.now() : startDate;
    LocalDateTime end = start.plus(certLifetime);
    return new JcaX509CertificateConverter().getCertificate(
        SelfSignedCertificate.newBuilder().setBeginDate(start)
            .setEndDate(end).setClusterID("cluster").setKey(keyPair)
            .setSubject("localhost").setConfiguration(secConf).setScmID("test")
            .build());
  }
}
