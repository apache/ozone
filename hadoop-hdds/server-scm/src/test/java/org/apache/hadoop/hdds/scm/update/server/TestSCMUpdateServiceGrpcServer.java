/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.hadoop.hdds.scm.update.server;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.update.client.CRLClientUpdateHandler;
import org.apache.hadoop.hdds.scm.update.client.ClientCRLStore;
import org.apache.hadoop.hdds.scm.update.client.SCMUpdateClientConfiguration;
import org.apache.hadoop.hdds.scm.update.client.SCMUpdateServiceGrpcClient;
import org.apache.hadoop.hdds.scm.update.client.UpdateServiceConfig;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.tag.Unhealthy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.event.Level;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

/**
 * Tests for SCM update Service.
 */
@Timeout(300)
public class TestSCMUpdateServiceGrpcServer {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSCMUpdateServiceGrpcServer.class);

  private MockCRLStore mockCRLStore;

  @BeforeEach
  public void setUp(@TempDir Path tempDir) throws Exception {
    mockCRLStore = new MockCRLStore(tempDir, LOG);
    GenericTestUtils.setLogLevel(CRLClientUpdateHandler.getLog(), Level.DEBUG);
  }

  @AfterEach
  public void destroyDbStore() throws Exception {
    if (mockCRLStore != null) {
      mockCRLStore.close();
      mockCRLStore = null;
    }
  }

  private UpdateServiceConfig getUpdateServiceConfig(OzoneConfiguration conf) {
    return conf.getObject(UpdateServiceConfig.class);
  }

  @Test
  public void testStartStop() {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMUpdateServiceGrpcServer server = new SCMUpdateServiceGrpcServer(
        getUpdateServiceConfig(conf), mockCRLStore);
    ClientCRLStore clientCRLStore = new ClientCRLStore();
    SCMUpdateServiceGrpcClient client =
        new SCMUpdateServiceGrpcClient("localhost", conf, clientCRLStore);

    try {
      server.start();
      client.start();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      // client need to handle the case when the server is stopped first.
      client.stop(true);
      server.stop();
    }
  }


  @Unhealthy("HDDS-5319")
  @Test
  public void testClientUpdateWithRevoke() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMUpdateServiceGrpcServer server = new SCMUpdateServiceGrpcServer(
        getUpdateServiceConfig(conf), mockCRLStore);
    ClientCRLStore clientCRLStore = new ClientCRLStore();
    SCMUpdateServiceGrpcClient client =
        new SCMUpdateServiceGrpcClient("localhost", conf, clientCRLStore);
    server.start();
    client.start();

    try {
      // issue 10 certs
      List<BigInteger> certIds = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        BigInteger certId = mockCRLStore.issueCert();
        certIds.add(certId);
      }

      // revoke 4 certs and broadcast
      for (int i = 0; i < 4; i++) {
        revokeCertNow((certIds.get(i)));
      }
      server.notifyCrlUpdate();

      GenericTestUtils.waitFor(() -> client.getUpdateCount() == 4, 100, 2000);
      Assertions.assertEquals(4, client.getUpdateCount());
      Assertions.assertEquals(0, client.getErrorCount());

      revokeCertNow(certIds.get(5));
      server.notifyCrlUpdate();
      GenericTestUtils.waitFor(() -> client.getUpdateCount() > 4, 100, 2000);
      Assertions.assertEquals(5, client.getUpdateCount());
      Assertions.assertEquals(0, client.getErrorCount());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      client.stop(true);
      server.stop();
    }
  }

  @Unhealthy("HDDS-5319")
  @Test
  public void testClientUpdateWithDelayedRevoke() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMUpdateServiceGrpcServer server = new SCMUpdateServiceGrpcServer(
        getUpdateServiceConfig(conf), mockCRLStore);

    ClientCRLStore clientCRLStore = new ClientCRLStore();

    // check pending crl every 5 seconds
    SCMUpdateClientConfiguration updateClientConfiguration =
        conf.getObject(SCMUpdateClientConfiguration.class);
    updateClientConfiguration.setClientCrlCheckInterval(Duration.ofSeconds(2));
    conf.setFromObject(updateClientConfiguration);

    SCMUpdateServiceGrpcClient client =
        new SCMUpdateServiceGrpcClient("localhost", conf, clientCRLStore);
    server.start();
    client.start();

    try {
      // issue 10 certs
      List<BigInteger> certIds = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        BigInteger certId = mockCRLStore.issueCert();
        certIds.add(certId);
      }

      // revoke cert 0
      revokeCertNow((certIds.get(0)));
      server.notifyCrlUpdate();

      GenericTestUtils.waitFor(() -> client.getUpdateCount() == 1,
          100, 2000);
      Assertions.assertEquals(1, client.getUpdateCount());
      Assertions.assertEquals(0, client.getErrorCount());

      // revoke cert 5 with 10 seconds delay
      revokeCert(certIds.get(5), Instant.now().plus(Duration.ofSeconds(5)));
      server.notifyCrlUpdate();
      GenericTestUtils.waitFor(() -> client.getUpdateCount() > 1,
          100, 2000);
      Assertions.assertTrue(2 <= client.getUpdateCount());
      Assertions.assertEquals(0, client.getErrorCount());
      Assertions.assertTrue(1 >= client.getClientCRLStore()
          .getPendingCrlIds().size());

      GenericTestUtils.waitFor(() -> client.getPendingCrlRemoveCount() == 1,
          100, 20_000);
      Assertions.assertTrue(client.getClientCRLStore()
          .getPendingCrlIds().isEmpty());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      client.stop(true);
      server.stop();
    }
  }

  private Long revokeCert(BigInteger certId, Instant revokeTime)
      throws IOException, TimeoutException {
    Optional<Long> crlId =
        mockCRLStore.revokeCert(Arrays.asList(certId), revokeTime);
    return crlId.get();
  }

  private Long revokeCertNow(BigInteger certId)
      throws IOException, TimeoutException {
    Optional<Long> crlId =
        mockCRLStore.revokeCert(Arrays.asList(certId), Instant.now());
    return crlId.get();
  }

  @Unhealthy("HDDS-5319")
  @Test
  public void testClientUpdateWithRestart() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    SCMUpdateServiceGrpcServer server = new SCMUpdateServiceGrpcServer(
        getUpdateServiceConfig(conf), mockCRLStore);
    ClientCRLStore clientCRLStore = new ClientCRLStore();
    SCMUpdateServiceGrpcClient client =
        new SCMUpdateServiceGrpcClient("localhost", conf, clientCRLStore);
    server.start();
    client.start();

    try {
      // issue 10 certs
      List<BigInteger> certIds = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        BigInteger certId = mockCRLStore.issueCert();
        certIds.add(certId);
      }

      // revoke 4 certs and broadcast
      for (int i = 0; i < 4; i++) {
        revokeCertNow((certIds.get(i)));
      }
      server.notifyCrlUpdate();
      GenericTestUtils.waitFor(() -> client.getUpdateCount() == 4,
          100, 2000);
      Assertions.assertEquals(4, client.getUpdateCount());


      // server restart
      // client onError->
      // 1. reconnect
      // 2. new subscribe resumes from previous state
      LOG.info("Test server restart begin.");
      // server shutdown can lead to duplicate message received on client when
      // client retry connect to the server. The client will handle that.
      server.stop();
      server.start();
      GenericTestUtils.waitFor(() -> client.getErrorCount() == 1,
          100, 2000);
      Assertions.assertEquals(4, client.getUpdateCount());
      Assertions.assertEquals(1, client.getErrorCount());
      Assertions.assertEquals(4, clientCRLStore.getLatestCrlId());
      LOG.info("Test server restart end.");

      revokeCertNow(certIds.get(5));
      server.notifyCrlUpdate();
      GenericTestUtils.waitFor(() -> client.getUpdateCount() > 4,
          100, 5000);
      Assertions.assertEquals(5, client.getUpdateCount());
      Assertions.assertEquals(1, client.getErrorCount());
      Assertions.assertEquals(5, clientCRLStore.getLatestCrlId());

      // client restart
      // server onError->
      // 1. remove stale client
      // 2. new subscribe resumes from previous state.
      LOG.info("Test client restart begin.");
      // a full client channel shutdown and create
      client.stop(true);
      client.createChannel();
      client.start();
      Assertions.assertEquals(5, clientCRLStore.getLatestCrlId());
      GenericTestUtils.waitFor(() -> client.getUpdateCount() > 5,
          100, 2000);
      revokeCertNow(certIds.get(6));
      // mostly noop
      server.notifyCrlUpdate();
      LOG.info("Test client restart end.");

      GenericTestUtils.waitFor(() -> client.getUpdateCount() > 6,
          100, 2000);
      Assertions.assertTrue(client.getUpdateCount() >= 6);
      Assertions.assertEquals(2, client.getErrorCount());
      Assertions.assertEquals(6, clientCRLStore.getLatestCrlId());
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      client.stop(true);
      server.stop();
    }
  }
}
