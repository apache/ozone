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
package org.apache.hadoop.hdds.security.x509.certificate.client;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocolPB.SCMSecurityProtocolClientSideTranslatorPB;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL;

/**
 * Test for Root Ca Rotation polling mechanism on client side.
 */
public class TestRootCaRotationPoller {

  private SecurityConfig secConf;
  private GenericTestUtils.LogCapturer logCapturer;

  @Mock
  private SCMSecurityProtocolClientSideTranslatorPB scmSecurityClient;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.openMocks(this);
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL, "PT1s");
    secConf = new SecurityConfig(conf);
    logCapturer = GenericTestUtils.LogCapturer.captureLogs(
        org.slf4j.LoggerFactory.getLogger(RootCaRotationPoller.class));
  }

  @Test
  public void testPollerDoesNotInvokeRootCaProcessor() throws Exception {
    //Given the root ca poller that knows a set of root ca certificates
    X509Certificate knownCert = generateX509Cert(
        LocalDateTime.now(), Duration.ofSeconds(50));
    HashSet<X509Certificate> knownCerts = new HashSet<>();
    knownCerts.add(knownCert);
    List<String> certsFromScm = new ArrayList<>();
    certsFromScm.add(CertificateCodec.getPEMEncodedString(knownCert));
    RootCaRotationPoller poller = new RootCaRotationPoller(secConf,
        knownCerts, scmSecurityClient, "");
    //When the scm returns the same set of root ca certificates, and they poll
    //for them
    Mockito.when(scmSecurityClient.getAllRootCaCertificates())
        .thenReturn(certsFromScm);
    CompletableFuture<Void> processingResult = new CompletableFuture<>();
    AtomicBoolean isProcessed = new AtomicBoolean(false);
    poller.addRootCARotationProcessor(
        certificates -> {
          isProcessed.set(true);
          processingResult.complete(null);
          return processingResult;
        }
    );
    poller.pollRootCas();
    //Then the certificates are not processed. Note that we can't invoke
    // processingResult.join before as it never gets completed
    Assertions.assertThrows(TimeoutException.class, () ->
        GenericTestUtils.waitFor(isProcessed::get, 50, 5000));
  }

  @Test
  public void testPollerInvokesRootCaProcessors() throws Exception {
    //Given the root ca poller knowing a root ca certificate, and an unknown
    //root ca certificate
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
        knownCerts, scmSecurityClient, "");
    //when the scm returns the unknown certificate to the poller
    Mockito.when(scmSecurityClient.getAllRootCaCertificates())
        .thenReturn(certsFromScm);
    CompletableFuture<Void> processingResult = new CompletableFuture<>();
    AtomicBoolean isProcessed = new AtomicBoolean(false);
    poller.addRootCARotationProcessor(
        certificates -> {
          isProcessed.set(true);
          processingResult.complete(null);
          return processingResult;
        }
    );
    poller.pollRootCas();
    processingResult.join();
    //The root ca processors are invoked
    Assertions.assertTrue(isProcessed.get());
  }

  @Test
  public void testPollerRetriesAfterFailure() throws Exception {
    //Given a the root ca poller knowing about a root ca certificate and the
    // SCM providing a new one
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
        knownCerts, scmSecurityClient, "");
    Mockito.when(scmSecurityClient.getAllRootCaCertificates())
        .thenReturn(certsFromScm);
    CompletableFuture<Void> processingResult = new CompletableFuture<>();
    //When encountering an error for the first run:
    AtomicInteger runNumber = new AtomicInteger(1);
    poller.addRootCARotationProcessor(
        certificates -> {
          if (runNumber.getAndIncrement() < 2) {
            poller.setCertificateRenewalError();
          }
          Assertions.assertEquals(certificates.size(), 2);
          processingResult.complete(null);
          return processingResult;
        }
    );
    //Then the first run encounters an error
    poller.pollRootCas();
    processingResult.join();
    Assertions.assertTrue(logCapturer.getOutput().contains(
        "There was a caught exception when trying to sign the certificate"));
    //And then the second clean run is successful.
    poller.pollRootCas();
    Assertions.assertTrue(logCapturer.getOutput().contains(
        "Certificate processing was successful."));
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
