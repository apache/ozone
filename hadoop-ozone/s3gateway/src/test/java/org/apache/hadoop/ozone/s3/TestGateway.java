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

package org.apache.hadoop.ozone.s3;

import static org.apache.hadoop.hdds.cli.GenericCli.EXECUTION_ERROR_EXIT_CODE;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STANDARD_STORAGE_CLASS_USE_CLIENT_DEFAULT_KEY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_STANDARD_IA_STORAGE_POLICY;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_REDUCED_REDUNDANCY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.S3_STORAGE_CLASS_STANDARD_IA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.ozone.s3.util.S3StorageClassResolver;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Tests S3 Gateway startup behavior.
 */
public class TestGateway {

  private final Logger rootLogger = LogManager.getLogManager().getLogger("");
  private Handler[] originalHandlers;

  @BeforeEach
  void saveRootLoggerHandlers() {
    OzoneConfigurationHolder.resetConfiguration();
    originalHandlers = rootLogger.getHandlers();
    removeRootLoggerHandlers();
  }

  @AfterEach
  void restoreRootLoggerHandlers() {
    OzoneConfigurationHolder.resetConfiguration();
    removeRootLoggerHandlers();
    for (Handler handler : originalHandlers) {
      rootLogger.addHandler(handler);
    }
  }

  @Test
  void redirectsJulLogsToSlf4j() {
    Logger julLogger = Logger.getLogger(TestGateway.class.getName());
    LogCapturer logCapturer = LogCapturer.captureLogs(TestGateway.class);
    rootLogger.addHandler(new ConsoleHandler());

    try {
      Gateway.redirectJulToSlf4j();

      assertThat(rootLogger.getHandlers())
          .singleElement()
          .isInstanceOf(SLF4JBridgeHandler.class);

      String message = "JUL warning redirected to the S3 Gateway log";
      julLogger.warning(message);

      assertThat(logCapturer.getOutput()).contains(message);
    } finally {
      logCapturer.stopCapturing();
    }
  }

  @Test
  void rejectsInvalidStoragePoliciesBeforeStartingServers() {
    for (String key : Arrays.asList(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY,
        OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY,
        OZONE_S3G_STORAGE_CLASS_STANDARD_IA_STORAGE_POLICY)) {
      for (String value : Arrays.asList("COLD", "LUKEWARM")) {
        Gateway gateway = new Gateway() {
          @Override
          public void start() {
            fail("Invalid storage policy must prevent server startup");
          }
        };
        gateway.getOzoneConf().set(key, value);
        StringWriter errors = new StringWriter();
        gateway.getCmd().setErr(new PrintWriter(errors));

        assertEquals(EXECUTION_ERROR_EXIT_CODE, gateway.execute(new String[0]));
        assertThat(errors.toString()).contains(key, value, "Expected HOT, WARM or empty.");
        assertNull(OzoneConfigurationHolder.configuration());
        assertNull(OzoneConfigurationHolder.storageClassResolver());
      }
    }
  }

  @Test
  void keepsResolverForTheInitialConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_S3G_STANDARD_STORAGE_CLASS_USE_CLIENT_DEFAULT_KEY, true);
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "HOT");
    conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, "WARM");
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_IA_STORAGE_POLICY, "HOT");
    OzoneConfigurationHolder.setConfiguration(conf);
    S3StorageClassResolver resolver = OzoneConfigurationHolder.storageClassResolver();

    conf.setBoolean(OZONE_S3G_STANDARD_STORAGE_CLASS_USE_CLIENT_DEFAULT_KEY, false);
    OzoneConfiguration ignored = new OzoneConfiguration();
    ignored.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "COLD");
    OzoneConfigurationHolder.setConfiguration(ignored);

    assertSame(conf, OzoneConfigurationHolder.configuration());
    assertSame(resolver, OzoneConfigurationHolder.storageClassResolver());
    ReplicationConfig clientConfig = new ECReplicationConfig("rs-6-3-512k");
    assertEquals(clientConfig, resolver.resolveReplicationConfig(S3_STORAGE_CLASS_STANDARD, null, clientConfig, null));
    assertEquals(OzoneStoragePolicy.HOT, resolver.toSpec(S3_STORAGE_CLASS_STANDARD).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.WARM, resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.HOT, resolver.toSpec(S3_STORAGE_CLASS_STANDARD_IA).getStoragePolicy());
  }

  @Test
  void resetsResolverWithConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_S3G_STANDARD_STORAGE_CLASS_USE_CLIENT_DEFAULT_KEY, true);
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "HOT");
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_IA_STORAGE_POLICY, "HOT");
    OzoneConfigurationHolder.setConfiguration(conf);
    S3StorageClassResolver previous = OzoneConfigurationHolder.storageClassResolver();

    OzoneConfigurationHolder.resetConfiguration();
    assertNull(OzoneConfigurationHolder.configuration());
    assertNull(OzoneConfigurationHolder.storageClassResolver());

    OzoneConfiguration next = new OzoneConfiguration();
    next.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "WARM");
    next.set(OZONE_S3G_STORAGE_CLASS_STANDARD_IA_STORAGE_POLICY, "WARM");
    OzoneConfigurationHolder.setConfiguration(next);
    S3StorageClassResolver resolver = OzoneConfigurationHolder.storageClassResolver();
    assertSame(next, OzoneConfigurationHolder.configuration());
    assertNotSame(previous, resolver);
    ReplicationConfig clientConfig = new ECReplicationConfig("rs-6-3-512k");
    assertEquals(clientConfig, previous.resolveReplicationConfig(S3_STORAGE_CLASS_STANDARD, null, clientConfig, null));
    assertEquals(RatisReplicationConfig.getInstance(ReplicationFactor.THREE),
        resolver.resolveReplicationConfig(S3_STORAGE_CLASS_STANDARD, null, clientConfig, null));
    assertEquals(OzoneStoragePolicy.WARM, resolver.toSpec(S3_STORAGE_CLASS_STANDARD).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.HOT, previous.toSpec(S3_STORAGE_CLASS_STANDARD_IA).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.WARM, resolver.toSpec(S3_STORAGE_CLASS_STANDARD_IA).getStoragePolicy());
  }

  @Test
  void canInitializeAfterCorrectingInvalidConfiguration() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OZONE_S3G_STORAGE_CLASS_STANDARD_STORAGE_POLICY, "HOT");
    conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, "COLD");
    assertThrows(IllegalArgumentException.class, () -> OzoneConfigurationHolder.setConfiguration(conf));
    assertNull(OzoneConfigurationHolder.configuration());
    assertNull(OzoneConfigurationHolder.storageClassResolver());

    conf.set(OZONE_S3G_STORAGE_CLASS_REDUCED_REDUNDANCY_STORAGE_POLICY, "WARM");
    OzoneConfigurationHolder.setConfiguration(conf);
    S3StorageClassResolver resolver = OzoneConfigurationHolder.storageClassResolver();
    assertSame(conf, OzoneConfigurationHolder.configuration());
    assertEquals(OzoneStoragePolicy.HOT, resolver.toSpec(S3_STORAGE_CLASS_STANDARD).getStoragePolicy());
    assertEquals(OzoneStoragePolicy.WARM, resolver.toSpec(S3_STORAGE_CLASS_REDUCED_REDUNDANCY).getStoragePolicy());
  }

  private void removeRootLoggerHandlers() {
    for (Handler handler : rootLogger.getHandlers()) {
      rootLogger.removeHandler(handler);
    }
  }
}
