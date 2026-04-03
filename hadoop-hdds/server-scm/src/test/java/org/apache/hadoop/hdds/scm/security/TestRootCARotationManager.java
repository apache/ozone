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

package org.apache.hadoop.hdds.scm.security;

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ACK_TIMEOUT;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_CHECK_INTERNAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_CA_ROTATION_TIME_OF_DAY;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_EXPIRED_CERTIFICATE_CHECK_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_RENEW_GRACE_DURATION;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.slf4j.event.Level.INFO;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.ha.SCMHAManager;
import org.apache.hadoop.hdds.scm.ha.SCMRatisServerImpl;
import org.apache.hadoop.hdds.scm.ha.SCMServiceManager;
import org.apache.hadoop.hdds.scm.ha.SequenceIdGenerator;
import org.apache.hadoop.hdds.scm.ha.StatefulServiceStateManager;
import org.apache.hadoop.hdds.scm.server.SCMSecurityProtocolServer;
import org.apache.hadoop.hdds.scm.server.SCMStorageConfig;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.CertInfo;
import org.apache.hadoop.hdds.security.x509.certificate.client.SCMCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec;
import org.apache.hadoop.hdds.security.x509.certificate.utils.SelfSignedCertificate;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test for root CA rotation manager.
 */
public class TestRootCARotationManager {

  private OzoneConfiguration ozoneConfig;
  private SecurityConfig securityConfig;
  private RootCARotationManager rootCARotationManager;
  private StorageContainerManager scm;
  private SCMCertificateClient scmCertClient;
  private SCMContext scmContext;
  private RootCARotationHandlerImpl handler;
  private StatefulServiceStateManager statefulServiceStateManager;
  @TempDir
  private File testDir;
  private String cID = UUID.randomUUID().toString();
  private String scmID = UUID.randomUUID().toString();
  private BigInteger certID = new BigInteger("1");

  @BeforeEach
  public void init() throws IOException, TimeoutException,
      CertificateException {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig
        .set(HddsConfigKeys.OZONE_METADATA_DIRS, testDir.getAbsolutePath());
    ozoneConfig
        .setBoolean(HDDS_X509_GRACE_DURATION_TOKEN_CHECKS_ENABLED, false);
    ozoneConfig.setBoolean(HDDS_X509_CA_ROTATION_ENABLED, true);
    scm = mock(StorageContainerManager.class);
    securityConfig = new SecurityConfig(ozoneConfig);
    scmCertClient = new SCMCertificateClient(securityConfig, null, scmID, cID,
        certID.toString(), "localhost");
    SCMServiceManager scmServiceManager = new SCMServiceManager();
    scmContext = mock(SCMContext.class);
    SCMHAManager scmhaManager = mock(SCMHAManager.class);
    SequenceIdGenerator sequenceIdGenerator = mock(SequenceIdGenerator.class);
    SCMStorageConfig scmStorageConfig = new SCMStorageConfig(ozoneConfig);
    scmStorageConfig.setScmId(scmID);
    scmStorageConfig.setClusterId(cID);
    SCMSecurityProtocolServer scmSecurityProtocolServer = mock(SCMSecurityProtocolServer.class);
    handler = mock(RootCARotationHandlerImpl.class);
    statefulServiceStateManager = mock(StatefulServiceStateManager.class);
    when(scmContext.isLeader()).thenReturn(true);
    when(scm.getConfiguration()).thenReturn(ozoneConfig);
    when(scm.getScmCertificateClient()).thenReturn(scmCertClient);
    when(scm.getScmContext()).thenReturn(scmContext);
    when(scm.getSCMServiceManager()).thenReturn(scmServiceManager);
    when(scm.getScmHAManager()).thenReturn(scmhaManager);
    when(scmhaManager.getRatisServer()).thenReturn(mock(SCMRatisServerImpl.class));
    when(scm.getSequenceIdGen()).thenReturn(sequenceIdGenerator);
    when(sequenceIdGenerator.getNextId(anyString())).thenReturn(2L);
    when(scm.getScmStorageConfig()).thenReturn(scmStorageConfig);
    when(scm.getSecurityProtocolServer()).thenReturn(scmSecurityProtocolServer);
    doNothing().when(scmSecurityProtocolServer).setRootCertificateServer(any());
    doNothing().when(handler).rotationPrepare(anyString());
    when(scm.getStatefulServiceStateManager()).thenReturn(statefulServiceStateManager);
    when(statefulServiceStateManager.readConfiguration(anyString())).thenReturn(null);
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (rootCARotationManager != null) {
      rootCARotationManager.stop();
    }
  }

  @Test
  void testProperties() throws Exception {
    // invalid check interval
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P28");
    assertThrows(DateTimeParseException.class, () -> rootCARotationManager = new RootCARotationManager(scm));

    // check interval should be less than grace period
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P28D");
    IllegalArgumentException ex =
        assertThrows(IllegalArgumentException.class, () -> rootCARotationManager = new RootCARotationManager(scm));
    assertThat(ex.getMessage()).contains("should be smaller than");

    // invalid time of day format
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P1D");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY, "01:00");
    ex = assertThrows(IllegalArgumentException.class, () -> rootCARotationManager = new RootCARotationManager(scm));
    assertThat(ex.getMessage()).contains("should follow the hh:mm:ss format");

    // valid properties
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P1D");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY, "01:00:00");

    rootCARotationManager = new RootCARotationManager(scm);

    // invalid property value is ignored when auto rotation is disabled.
    ozoneConfig.setBoolean(HDDS_X509_CA_ROTATION_ENABLED, false);
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "P28D");

    rootCARotationManager = new RootCARotationManager(scm);
  }

  @Test
  public void testRotationOnSchedule() throws Exception {
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "PT2S");
    ozoneConfig.set(HDDS_X509_RENEW_GRACE_DURATION, "PT15S");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT, "PT2S");
    ozoneConfig.set(HDDS_X509_EXPIRED_CERTIFICATE_CHECK_INTERVAL, "PT15S");
    Date date = Calendar.getInstance().getTime();
    date.setSeconds(date.getSeconds() + 10);
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY,
        String.format("%02d", date.getHours()) + ":" +
            String.format("%02d", date.getMinutes()) + ":" +
            String.format("%02d", date.getSeconds()));

    X509Certificate cert = generateX509Cert(ozoneConfig,
        ZonedDateTime.now(), Duration.ofSeconds(35));
    scmCertClient.setCACertificate(cert);

    rootCARotationManager = new RootCARotationManager(scm);
    rootCARotationManager.setRootCARotationHandler(handler);
    LogCapturer logs = LogCapturer.captureLogs(RootCARotationManager.class);
    GenericTestUtils.setLogLevel(RootCARotationManager.class, INFO);
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
        ZonedDateTime.now(), Duration.ofSeconds(35));
    scmCertClient.setCACertificate(cert);

    rootCARotationManager = new RootCARotationManager(scm);
    rootCARotationManager.setRootCARotationHandler(handler);
    LogCapturer logs = LogCapturer.captureLogs(RootCARotationManager.class);
    GenericTestUtils.setLogLevel(RootCARotationManager.class, INFO);
    rootCARotationManager.start();
    rootCARotationManager.notifyStatusChanged();

    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("Start the rotation immediately") &&
        logs.getOutput().contains("Root certificate " +
            cert.getSerialNumber().toString() + " rotation is started."),
        100, 10000);
  }

  @Test
  public void testPostProcessingCheck() throws Exception {
    ozoneConfig.set(HDDS_X509_CA_ROTATION_CHECK_INTERNAL, "PT2S");
    ozoneConfig.set(HDDS_X509_RENEW_GRACE_DURATION, "PT20S");
    ozoneConfig.set(HDDS_X509_CA_ROTATION_ACK_TIMEOUT, "PT5S");
    ozoneConfig.set(HDDS_X509_ROOTCA_CERTIFICATE_POLLING_INTERVAL, "PT10S");
    Date date = Calendar.getInstance().getTime();
    date.setMinutes(date.getMinutes() + 5);
    ozoneConfig.set(HDDS_X509_CA_ROTATION_TIME_OF_DAY,
        String.format("%02d", date.getHours()) + ":" +
            String.format("%02d", date.getMinutes()) + ":" +
            String.format("%02d", date.getSeconds()));

    X509Certificate cert = generateX509Cert(ozoneConfig,
        ZonedDateTime.now(), Duration.ofSeconds(90));
    scmCertClient.setCACertificate(cert);
    CertificateCodec certCodec = new CertificateCodec(securityConfig,
        "scm/sub-ca");
    certCodec.writeCertificate(cert);
    rootCARotationManager = new RootCARotationManager(scm);
    rootCARotationManager.setRootCARotationHandler(handler);
    LogCapturer logs = LogCapturer.captureLogs(RootCARotationManager.class);
    GenericTestUtils.setLogLevel(RootCARotationManager.class, INFO);
    rootCARotationManager.start();
    rootCARotationManager.notifyStatusChanged();

    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("No RootCARotationManager " +
                "configuration found in stateful storage"),
        100, 10000);

    when(statefulServiceStateManager.readConfiguration(anyString()))
        .thenReturn(new CertInfo.Builder().setX509Certificate(cert)
            .setTimestamp(cert.getNotBefore().getTime())
            .build().getProtobuf().toByteString());

    logs.clearOutput();
    when(scmContext.isLeader()).thenReturn(false);
    rootCARotationManager.notifyStatusChanged();
    when(scmContext.isLeader()).thenReturn(true);
    rootCARotationManager.notifyStatusChanged();
    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("isPostProcessing is true for"),
        100, 20000);

    logs.clearOutput();
    when(scmContext.isLeader()).thenReturn(false);
    rootCARotationManager.notifyStatusChanged();
    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("disable monitor task"),
        100, 20000);

    logs.clearOutput();
    when(scmContext.isLeader()).thenReturn(true);
    rootCARotationManager.notifyStatusChanged();
    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("isPostProcessing is true for"),
        100, 20000);

    doNothing().when(statefulServiceStateManager).deleteConfiguration(anyString());
    GenericTestUtils.waitFor(
        () -> logs.getOutput().contains("isPostProcessing is false") &&
            logs.getOutput().contains("Stateful configuration is deleted"),
        100, 20000);
    verify(statefulServiceStateManager, times(1)).deleteConfiguration(anyString());
  }

  private X509Certificate generateX509Cert(
      OzoneConfiguration conf, ZonedDateTime startDate,
      Duration certLifetime) throws Exception {
    KeyPair keyPair = KeyStoreTestUtil.generateKeyPair("RSA");
    ZonedDateTime start = startDate == null ? ZonedDateTime.now() : startDate;
    ZonedDateTime end = start.plus(certLifetime);
    return
        SelfSignedCertificate.newBuilder()
            .setBeginDate(start)
            .setEndDate(end)
            .setScmID(scmID)
            .setClusterID(cID)
            .setSubject("localhost")
            .setConfiguration(new SecurityConfig(conf))
            .setKey(keyPair)
            .makeCA(certID)
            .build();
  }
}
