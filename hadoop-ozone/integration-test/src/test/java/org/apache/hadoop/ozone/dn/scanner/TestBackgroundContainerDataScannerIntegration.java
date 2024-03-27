/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.hadoop.ozone.dn.scanner;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.ozoneimpl.BackgroundContainerDataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Integration tests for the background container data scanner. This scanner
 * checks all data and metadata in the container.
 */
class TestBackgroundContainerDataScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  private final LogCapturer logCapturer =
      LogCapturer.log4j2(ContainerLogger.LOG_NAME);

  @BeforeAll
  static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED, true);
    // Make sure the background metadata scanner does not detect failures
    // before the data scanner under test does.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_METADATA_ENABLED,
        false);
    // Make the background data scanner run frequently to reduce test time.
    ozoneConfig.setTimeDuration(
        ContainerScannerConfiguration.DATA_SCAN_INTERVAL_KEY,
        SCAN_INTERVAL.getSeconds(), TimeUnit.SECONDS);
    buildCluster(ozoneConfig);
  }

  /**
   * {@link BackgroundContainerDataScanner} should detect corrupted blocks
   * in a closed container without client interaction.
   */
  @ParameterizedTest
  // Background container data scanner should be able to detect all errors.
  @EnumSource
  void testCorruptionDetected(ContainerCorruptions corruption)
      throws Exception {
    pauseScanner();

    long containerID = writeDataThenCloseContainer();
    // Container corruption has not yet been introduced.
    Container<?> container = getDnContainer(containerID);
    assertEquals(State.CLOSED, container.getContainerState());

    corruption.applyTo(container);

    resumeScanner();

    // Wait for the scanner to detect corruption.
    GenericTestUtils.waitFor(
        () -> container.getContainerState() == State.UNHEALTHY,
        500, 15_000);

    // Wait for SCM to get a report of the unhealthy replica.
    waitForScmToSeeUnhealthyReplica(containerID);
    corruption.assertLogged(logCapturer);
  }
}
