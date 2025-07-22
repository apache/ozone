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

package org.apache.hadoop.hdds.scm.container.metrics;

import static org.apache.ozone.test.MetricsAsserts.getLongCounter;
import static org.apache.ozone.test.MetricsAsserts.getMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

/**
 * Class used to test {@link SCMContainerManagerMetrics}.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestSCMContainerManagerMetrics implements NonHATests.TestCase {

  private StorageContainerManager scm;
  private OzoneClient client;

  @BeforeEach
  public void setup() throws Exception {
    client = cluster().newClient();
    scm = cluster().getStorageContainerManager();
  }

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testContainerOpsMetrics() throws Exception {
    MetricsRecordBuilder metrics;
    ContainerManager containerManager = scm.getContainerManager();
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulCreateContainers = getLongCounter(
        "NumSuccessfulCreateContainers", metrics);
    long numFailureCreateContainers = getLongCounter(
        "NumFailureCreateContainers", metrics);

    ContainerInfo containerInfo = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), OzoneConsts.OZONE);

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    assertEquals(getLongCounter("NumSuccessfulCreateContainers",
        metrics), ++numSuccessfulCreateContainers);

    assertThrows(IOException.class, () ->
        containerManager.allocateContainer(
            new ECReplicationConfig(8, 5), OzoneConsts.OZONE));
    // allocateContainer should fail, so it should have the old metric value.
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    assertEquals(getLongCounter("NumSuccessfulCreateContainers",
        metrics), numSuccessfulCreateContainers);
    assertEquals(getLongCounter("NumFailureCreateContainers",
        metrics), numFailureCreateContainers + 1);

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulDeleteContainers = getLongCounter(
        "NumSuccessfulDeleteContainers", metrics);
    long numFailureDeleteContainers = getLongCounter(
        "NumFailureDeleteContainers", metrics);

    containerManager.deleteContainer(
        ContainerID.valueOf(containerInfo.getContainerID()));

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
        metrics), ++numSuccessfulDeleteContainers);

    assertThrows(ContainerNotFoundException.class, () ->
        containerManager.deleteContainer(
            ContainerID.valueOf(RandomUtils.secure().randomLong(10000, 20000))));
    // deleteContainer should fail, so it should have the old metric value.
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
        metrics), numSuccessfulDeleteContainers);
    assertEquals(getLongCounter("NumFailureDeleteContainers",
        metrics), numFailureDeleteContainers + 1);

    long currentValue = getLongCounter("NumListContainerOps", metrics);
    containerManager.getContainers(
        ContainerID.valueOf(containerInfo.getContainerID()), 1);
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    assertEquals(currentValue + 1,
        getLongCounter("NumListContainerOps", metrics));

  }

  @Test
  public void testReportProcessingMetrics() throws Exception {
    MetricsRecordBuilder metrics =
        getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    assertThat(getLongCounter("NumContainerReportsProcessedSuccessful", metrics))
        .isPositive();

    final long previous = getLongCounter("NumICRReportsProcessedSuccessful", metrics);
    OzoneTestUtils.closeAllContainers(scm.getEventQueue(), scm);

    // Create key should create container on DN.
    TestDataUtil.createKeys(cluster(), 1);

    GenericTestUtils.waitFor(() -> {
      final MetricsRecordBuilder scmMetrics =
          getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      return getLongCounter("NumICRReportsProcessedSuccessful", scmMetrics) > previous;
    }, 100, 30_000);
  }
}
