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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.security;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.server.SCMSecurityProtocolServer;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_TIME_OF_DAY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.INFO;

/**
 * Test for root CA rotation manager.
 */
public class TestRootCARotationManager {

  private OzoneConfiguration ozoneConfig;
  private SecurityConfig securityConfig;
  private RootCARotationManager rootCARotationManager;
  private StorageContainerManager scm;
  private SCMCertificateClient scmCertClient;
  private SCMServiceManager scmServiceManager;
  private SCMHAManager scmhaManager;
  private SCMContext scmContext;
  private SequenceIdGenerator sequenceIdGenerator;
  private SCMStorageConfig scmStorageConfig;
  private SCMSecurityProtocolServer scmSecurityProtocolServer;
  private RootCARotationHandlerImpl handler;
  private File testDir;
  private String cID = UUID.randomUUID().toString();
  private String scmID = UUID.randomUUID().toString();
  private BigInteger certID = new BigInteger("1");

  @BeforeEach
  public void init() throws IOException, TimeoutException,
      CertificateException {
    ozoneConfig = new OzoneConfiguration();
    testDir = GenericTestUtils.getTestDir(
        TestRootCARotationManager.class.getSimpleName() + UUID.randomUUID());
    ozoneConfig
        .set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    ozoneConfig
        .setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
    ozoneConfig.setBoolean(HDDS_X509_CA_ROTATION_ENABLED, true);
    scm = Mockito.mock(StorageContainerManager.class);
    securityConfig = new SecurityConfig(ozoneConfig);
    scmCertClient = new SCMCertificateClient(securityConfig, null, scmID, cID,
        certID.toString(), "localhost");
    scmServiceManager = new SCMServiceManager();
    scmContext = Mockito.mock(SCMContext.class);
    scmhaManager = Mockito.mock(SCMHAManager.class);
    sequenceIdGenerator = Mockito.mock(SequenceIdGenerator.class);
    scmStorageConfig = new SCMStorageConfig(ozoneConfig);
    scmStorageConfig.setScmId(scmID);
    scmStorageConfig.setClusterId(cID);
    scmSecurityProtocolServer = Mockito.mock(SCMSecurityProtocolServer.class);
    handler = Mockito.mock(RootCARotationHandlerImpl.class);
    when(scmContext.isLeader()).thenReturn(true);
    when(scm.getConfiguration()).thenReturn(ozoneConfig);
    when(scm.getScmCertificateClient()).thenReturn(scmCertClient);
    when(scm.getScmContext()).thenReturn(scmContext);
    when(scm.getSCMServiceManager()).thenReturn(scmServiceManager);
    when(scm.getScmHAManager()).thenReturn(scmhaManager);
    when(scmhaManager.getRatisServer())
        .thenReturn(Mockito.mock(SCMRatisServerImpl.class));
    when(scm.getSequenceIdGen()).thenReturn(sequenceIdGenerator);
    when(sequenceIdGenerator.getNextId(Mockito.anyString())).thenReturn(2L);
    when(scm.getScmStorageConfig()).thenReturn(scmStorageConfig);
    when(scm.getSecurityProtocolServer()).thenReturn(scmSecurityProtocolServer);
    Mockito.doNothing().when(scmSecurityProtocolServer)
        .setRootCertificateServer(Mockito.anyObject());
    Mockito.doNothing().when(handler).rotationPrepare(Mockito.anyString());
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rootCARotationManager != null) {
      rootCARotationManager.stop();
    }

    FileUtil.fullyDelete(testDir);
  }

  @Test
  public void testProperties() {
    // invalid check interval
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P28");
    try {
      rootCARotationManager = new RootCARotationManager(scm);
      fail("Should fail");
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof DateTimeParseException);
    }

    // check interval should be less than grace period
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P28D");
    try {
      rootCARotationManager = new RootCARotationManager(scm);
      fail("Should fail");
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof IllegalArgumentException);
      Assertions.assertTrue(e.getMessage().contains("should be smaller than"));
    }

    // invalid time of day format
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P1D");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY, "01:00");
    try {
      rootCARotationManager = new RootCARotationManager(scm);
      fail("Should fail");
    } catch (Exception e) {
      Assertions.assertTrue(e instanceof IllegalArgumentException);
      Assertions.assertTrue(
          e.getMessage().contains("should follow the hh:mm:ss format"));
    }

    // valid properties
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P1D");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY, "01:00:00");

    try {
      rootCARotationManager = new RootCARotationManager(scm);
    } catch (Exception e) {
      fail("Should succeed");
    }

    // invalid property value is ignored when auto rotation is disabled.
    ozoneConfig.setBoolean(HDDS_X509_CA_ROTATION_ENABLED, false);
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P28D");
    try {
      rootCARotationManager = new RootCARotationManager(scm);
    } catch (Exception e) {
      fail("Should succeed");
    }
  }

  @Test
  public void testRotationOnSchedule() throws Exception {
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "PT2S");
    ozoneConfig.set(HDDS_X509_RENEW_GRACE_DURATION, "PT15S");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT, "PT2S");
    Date date = Calendar.getInstance().getTime();
    date.setSeconds(date.getSeconds() + 10);
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY,
        String.format("%02d", date.getHours()) + ":" +
            String.format("%02d", date.getMinutes()) + ":" +
            String.format("%02d", date.getSeconds()));

    X509Certificate cert = generateX509Cert(ozoneConfig,
        LocalDateTime.now(), Duration.ofSeconds(35));
    scmCertClient.setCACertificate(cert);

    rootCARotationManager = new RootCARotationManager(scm);
    rootCARotationManager.setRootCARotationHandler(handler);
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(RootCARotationManager.LOG);
    GenericTestUtils.setLogLevel(RootCARotationManager.LOG, INFO);
    rootCARotationManager.start();
    rootCARotationManager.notifyStatusChanged();

    String msg = "Root certificate " +
        cert.getSerialNumber().toString() + " rotation is started.";
    GenericTestUtils.waitFor(
        () -> !logs.getOutput().contains("Start the rotation immediately") &&
            logs.getOutput().contains(msg),
        100, 10000);
    assertEquals(1, StringUtils.countMatches(logs.getOutput(), msg));
  }

  @Test
  public void testRotationImmediately() throws Exception {
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "PT2S");
    ozoneConfig.set(HDDS_X509_RENEW_GRACE_DURATION, "PT15S");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT, "PT2S");
    Date date = Calendar.getInstance().getTime();
    date.setMinutes(date.getMinutes() + 5);
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY,
        String.format("%02d", date.getHours()) + ":" +
            String.format("%02d", date.getMinutes()) + ":" +
            String.format("%02d", date.getSeconds()));

    X509Certificate cert = generateX509Cert(ozoneConfig,
        LocalDateTime.now(), Duration.ofSeconds(35));
    scmCertClient.setCACertificate(cert);

    rootCARotationManager = new RootCARotationManager(scm);
    rootCARotationManager.setRootCARotationHandler(handler);
    GenericTestUtils.LogCapturer logs =
        GenericTestUtils.LogCapturer.captureLogs(RootCARotationManager.LOG);
    GenericTestUtils.setLogLevel(RootCARotationManager.LOG, INFO);
    rootCARotationManager.start();
    rootCARotationManager.notifyStatusChanged();

    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("Start the rotation immediately") &&
        logs.getOutput().contains("Root certificate " +
            cert.getSerialNumber().toString() + " rotation is started."),
        100, 10000);
  }

  private X509Certificate generateX509Cert(
      OzoneConfiguration conf, LocalDateTime startDate,
      Duration certLifetime) throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    LocalDateTime start = startDate == null ? LocalDateTime.now() : startDate;
    LocalDateTime end = start.plus(certLifetime);
    return new JcaX509CertificateConverter().getCertificate(
        SelfSignedCertificate.newBuilder()
            .setBeginDate(start)
            .setEndDate(end)
            .setScmID(scmID)
            .setClusterID(cID)
            .setSubject("localhost")
            .setConfiguration(new SecurityConfig(conf))
            .setKey(keyPair)
            .makeCA(certID)
            .build());
  }
}
