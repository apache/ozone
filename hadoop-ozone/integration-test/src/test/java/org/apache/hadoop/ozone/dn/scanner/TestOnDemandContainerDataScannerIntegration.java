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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.container.common.utils.ContainerLogger;
import org.apache.hadoop.ozone.container.ozoneimpl.OnDemandContainerDataScanner;
import org.apache.hadoop.ozone.container.ozoneimpl.ContainerScannerConfiguration;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Integration tests for the on demand container data scanner. This scanner
 * is triggered when there is an error while a client interacts with a
 * container.
 */
@RunWith(Parameterized.class)
public class TestOnDemandContainerDataScannerIntegration
    extends TestContainerScannerIntegrationAbstract {

  private final ContainerCorruptions corruption;
  private final GenericTestUtils.LogCapturer logCapturer;

  /**
   The on-demand container scanner is triggered by errors on the block read
   path. Since this may not touch all parts of the container, the scanner is
   limited in what errors it can detect:
   - The container file is not on the read path, so any errors in this file
   will not trigger an on-demand scan.
   - With container schema v3 (one RocksDB per volume), RocksDB is not in
   the container metadata directory, therefore nothing in this directory is on
   the read path.
   - Block checksums are verified on the client side. If there is a checksum
   error during read, the datanode will not learn about it.
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> supportedCorruptionTypes() {
    return ContainerCorruptions.getAllParamsExcept(
        ContainerCorruptions.MISSING_METADATA_DIR,
        ContainerCorruptions.MISSING_CONTAINER_FILE,
        ContainerCorruptions.CORRUPT_CONTAINER_FILE,
        ContainerCorruptions.TRUNCATED_CONTAINER_FILE,
        ContainerCorruptions.CORRUPT_BLOCK,
        ContainerCorruptions.TRUNCATED_BLOCK);
  }

  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration ozoneConfig = new OzoneConfiguration();
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_ENABLED,
        true);
    // Disable both background container scanners to make sure only the
    // on-demand scanner is detecting failures.
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_DATA_ENABLED,
        false);
    ozoneConfig.setBoolean(
        ContainerScannerConfiguration.HDDS_CONTAINER_SCRUB_DEV_METADATA_ENABLED,
        false);
    buildCluster(ozoneConfig);
  }

  public TestOnDemandContainerDataScannerIntegration(
      ContainerCorruptions corruption) {
    this.corruption = corruption;
    logCapturer = GenericTestUtils.LogCapturer.captureLogs(
        LoggerFactory.getLogger(ContainerLogger.LOG_NAME));
  }

  /**
   * {@link OnDemandContainerDataScanner} should detect corrupted blocks
   * in a closed container when a client reads from it.
   */
  @Test
  public void testCorruptionDetected() throws Exception {
    String keyName = "testKey";
    long containerID = writeDataThenCloseContainer(keyName);
    // Container corruption has not yet been introduced.
    Assert.assertEquals(ContainerProtos.ContainerDataProto.State.CLOSED,
        getDnContainer(containerID).getContainerState());
    // Corrupt the container.
    corruption.applyTo(getDnContainer(containerID));
    // This method will check that reading from the corrupted key returns an
    // error to the client.
    readFromCorruptedKey(keyName);
    // Reading from the corrupted key should have triggered an on-demand scan
    // of the container, which will detect the corruption.
    GenericTestUtils.waitFor(() ->
            getDnContainer(containerID).getContainerState() ==
                ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        500, 5000);

    // Wait for SCM to get a report of the unhealthy replica.
    waitForScmToSeeUnhealthyReplica(containerID);
    corruption.assertLogged(logCapturer);
  }
}
