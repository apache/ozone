/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.container.metrics;

import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_SCM_SAFEMODE_PIPELINE_CREATION;
import static org.apache.hadoop.test.MetricsAsserts.getLongCounter;
import static org.apache.hadoop.test.MetricsAsserts.getMetrics;

/**
 * Class used to test {@link SCMContainerManagerMetrics}.
 */
@Timeout(300)
public class TestSCMContainerManagerMetrics {

  private MiniOzoneCluster cluster;
  private StorageContainerManager scm;
  private OzoneClient client;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HDDS_CONTAINER_REPORT_INTERVAL, "3000s");
    conf.setBoolean(HDDS_SCM_SAFEMODE_PIPELINE_CREATION, false);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    scm = cluster.getStorageContainerManager();
  }


  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    cluster.shutdown();
  }

  @Test
  public void testContainerOpsMetrics() throws Exception {
    MetricsRecordBuilder metrics;
    ContainerManager containerManager = scm.getContainerManager();
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulCreateContainers = getLongCounter(
        "NumSuccessfulCreateContainers", metrics);

    ContainerInfo containerInfo = containerManager.allocateContainer(
        RatisReplicationConfig.getInstance(
            HddsProtos.ReplicationFactor.ONE), OzoneConsts.OZONE);

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assertions.assertEquals(getLongCounter("NumSuccessfulCreateContainers",
        metrics), ++numSuccessfulCreateContainers);

    Assertions.assertThrows(IOException.class, () ->
        containerManager.allocateContainer(
            RatisReplicationConfig.getInstance(
                HddsProtos.ReplicationFactor.THREE), OzoneConsts.OZONE));
    // allocateContainer should fail, so it should have the old metric value.
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assertions.assertEquals(getLongCounter("NumSuccessfulCreateContainers",
        metrics), numSuccessfulCreateContainers);
    Assertions.assertEquals(getLongCounter("NumFailureCreateContainers",
        metrics), 1);

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    long numSuccessfulDeleteContainers = getLongCounter(
        "NumSuccessfulDeleteContainers", metrics);

    containerManager.deleteContainer(
        ContainerID.valueOf(containerInfo.getContainerID()));

    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assertions.assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
        metrics), numSuccessfulDeleteContainers + 1);

    Assertions.assertThrows(ContainerNotFoundException.class, () ->
        containerManager.deleteContainer(
            ContainerID.valueOf(RandomUtils.nextLong(10000, 20000))));
    // deleteContainer should fail, so it should have the old metric value.
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assertions.assertEquals(getLongCounter("NumSuccessfulDeleteContainers",
        metrics), numSuccessfulCreateContainers);
    Assertions.assertEquals(getLongCounter("NumFailureDeleteContainers",
        metrics), 1);

    long currentValue = getLongCounter("NumListContainerOps", metrics);
    containerManager.getContainers(
        ContainerID.valueOf(containerInfo.getContainerID()), 1);
    metrics = getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assertions.assertEquals(currentValue + 1,
        getLongCounter("NumListContainerOps", metrics));

  }

  @Test
  public void testReportProcessingMetrics() throws Exception {
    String volumeName = "vol1";
    String bucketName = "bucket1";
    String key = "key1";

    MetricsRecordBuilder metrics =
        getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
    Assertions.assertEquals(1L,
        getLongCounter("NumContainerReportsProcessedSuccessful", metrics));

    // Create key should create container on DN.
    client.getObjectStore().getClientProxy()
        .createVolume(volumeName);
    client.getObjectStore().getClientProxy()
        .createBucket(volumeName, bucketName);
    OzoneOutputStream ozoneOutputStream = client
        .getObjectStore().getClientProxy().createKey(volumeName, bucketName,
            key, 0, ReplicationType.RATIS, ReplicationFactor.ONE,
            new HashMap<>());

    String data = "file data";
    ozoneOutputStream.write(data.getBytes(UTF_8), 0, data.length());
    ozoneOutputStream.close();


    GenericTestUtils.waitFor(() -> {
      final MetricsRecordBuilder scmMetrics =
          getMetrics(SCMContainerManagerMetrics.class.getSimpleName());
      return getLongCounter("NumICRReportsProcessedSuccessful",
          scmMetrics) == 1;
    }, 1000, 500000);
  }
}
