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

package org.apache.hadoop.ozone.client.rpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.pipeline.WritableECContainerProvider.WritableECContainerProviderConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.io.ECKeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.HddsDispatcher;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.keyvalue.helpers.KeyValueContainerLocationUtil;
import org.apache.hadoop.ozone.container.metadata.ContainerCreateInfo;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ozone.test.GenericTestUtils.LogCapturer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.event.Level;

/**
 * An EC key stream that holds a block group allocated before the container was deleted keeps writing to that container
 * after every replica of it was deleted on all five datanodes. The datanodes must refuse the stripe with
 * CLOSED_CONTAINER_IO instead of creating an empty replica of the deleted container again, they must keep the entry of
 * the container in the witnessed container DB as (DELETED, -1), and the key must complete on another block group.
 */
public class TestECDelayedStripeAfterContainerDelete {
  private static final int CHUNK_SIZE = 1024 * 1024;
  private static final int STRIPE_SIZE = 3 * CHUNK_SIZE;
  private static final ECReplicationConfig EC_CONFIG =
      new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS, CHUNK_SIZE);

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;

  @BeforeAll
  public static void init() throws Exception {
    final OzoneConfiguration conf = new OzoneConfiguration();
    // One open pipeline, so that consecutive block group allocations share the container.
    final WritableECContainerProviderConfig ecConfig = conf.getObject(WritableECContainerProviderConfig.class);
    ecConfig.setMinimumPipelines(1);
    conf.setFromObject(ecConfig);
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(5).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    final String name = UUID.randomUUID().toString();
    client.getObjectStore().createVolume(name);
    client.getObjectStore().getVolume(name).createBucket(name);
    bucket = client.getObjectStore().getVolume(name).getBucket(name);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static OzoneContainer container(HddsDatanodeService dn) {
    return dn.getDatanodeStateMachine().getContainer();
  }

  /** The entry of the container in the witnessed container DB of the datanode, as text. */
  private static String witnessedEntry(HddsDatanodeService dn, long containerId) throws IOException {
    final ContainerCreateInfo info =
        container(dn).getWitnessedContainerMetadataStore().getContainerCreateInfoTable().get(
            ContainerID.valueOf(containerId));
    return info == null ? "none" : "(" + info.getState() + ", " + info.getReplicaIndex() + ")";
  }

  /** The number of volumes of the datanode that have a directory for the container. */
  private static long containerDirCount(HddsDatanodeService dn, long containerId) {
    return container(dn).getVolumeSet().getVolumesList().stream()
        .map(volume -> (HddsVolume) volume)
        .map(volume -> new File(KeyValueContainerLocationUtil.getBaseContainerLocation(
            volume.getHddsRootDir().toString(), volume.getClusterID(), containerId)))
        .filter(File::exists)
        .count();
  }

  /** True if the log shows that the datanode refused to create the container with CLOSED_CONTAINER_IO. */
  private static boolean refused(String log, HddsDatanodeService dn, long containerId) {
    final String thread = "[" + dn.getDatanodeDetails().threadNamePrefix();
    return Arrays.stream(log.split("\n")).anyMatch(line -> line.startsWith(thread)
        && line.contains("ContainerID " + containerId + " creation failed") && line.contains("CLOSED_CONTAINER_IO"));
  }

  @Test
  public void testDelayedStripeToDeletedContainerIsRefused() throws Exception {
    final byte[] stripe = RandomUtils.secure().randomBytes(STRIPE_SIZE);
    final List<HddsDatanodeService> datanodes = cluster.getHddsDatanodes();
    // Keep SCM from repairing the container that the test deletes.
    final ReplicationManager replicationManager = cluster.getStorageContainerManager().getReplicationManager();
    replicationManager.stop();
    GenericTestUtils.waitFor(() -> !replicationManager.isRunning(), 100, 10000);

    // K2 is opened first, so that its block group is allocated before any replica of the container exists.
    final OzoneOutputStream k2 = bucket.createKey("k2", STRIPE_SIZE, EC_CONFIG, new HashMap<>());
    try {
      final long containerId =
          ((ECKeyOutputStream) k2.getOutputStream()).getStreamEntries().get(0).getBlockID().getContainerID();
      // K1 creates the replicas of the container on all five datanodes.
      try (OzoneOutputStream k1 = bucket.createKey("k1", STRIPE_SIZE, EC_CONFIG, new HashMap<>())) {
        k1.write(stripe);
      }
      assertEquals(containerId, bucket.getKey("k1").getOzoneKeyLocations().get(0).getContainerID(),
          "K1 and K2 must share the container");
      for (HddsDatanodeService dn : datanodes) {
        assertNotNull(container(dn).getContainerSet().getContainer(containerId), "replica before delete");
        assertEquals(1, containerDirCount(dn, containerId), "directory before delete");
        assertNotEquals("none", witnessedEntry(dn, containerId), "entry before delete");
      }

      // Force-delete the replica on every datanode. Each datanode must keep the entry as (DELETED, -1).
      for (HddsDatanodeService dn : datanodes) {
        container(dn).getController().deleteContainer(containerId, true);
      }
      for (HddsDatanodeService dn : datanodes) {
        final String where = " on " + dn.getDatanodeDetails();
        assertNull(container(dn).getContainerSet().getContainer(containerId), "replica after delete" + where);
        assertEquals(0, containerDirCount(dn, containerId), "directory after delete" + where);
        assertEquals("(DELETED, -1)", witnessedEntry(dn, containerId), "entry after delete" + where);
      }

      // K2 now writes a full stripe to its stale block group. No datanode may create the container again.
      final String log;
      GenericTestUtils.setLogLevel(HddsDispatcher.class, Level.DEBUG);
      try (LogCapturer logs = LogCapturer.captureLogs(
          Logger.getLogger(HddsDispatcher.class), new PatternLayout("[%t] %m%n"))) {
        k2.write(stripe);
        k2.close();
        log = logs.getOutput();
      } finally {
        GenericTestUtils.setLogLevel(HddsDispatcher.class, Level.INFO);
      }
      final List<Long> k2Containers = bucket.getKey("k2").getOzoneKeyLocations().stream()
          .map(OzoneKeyLocation::getContainerID).collect(Collectors.toList());
      final byte[] k2Data;
      try (OzoneInputStream in = bucket.readKey("k2")) {
        k2Data = in.readAllBytes();
      }

      final List<Executable> checks = new ArrayList<>();
      for (HddsDatanodeService dn : datanodes) {
        final String where = " on " + dn.getDatanodeDetails();
        checks.add(() -> assertTrue(refused(log, dn, containerId), "stripe refused" + where));
        checks.add(() -> assertNull(container(dn).getContainerSet().getContainer(containerId), "replica" + where));
        checks.add(() -> assertEquals(0, containerDirCount(dn, containerId), "directory" + where));
        checks.add(() -> assertEquals("(DELETED, -1)", witnessedEntry(dn, containerId), "entry" + where));
      }
      checks.add(() -> assertThat(k2Containers).as("containers of K2 block locations")
          .isNotEmpty().doesNotContain(containerId));
      checks.add(() -> assertArrayEquals(stripe, k2Data, "K2 data"));
      assertAll(checks);
    } finally {
      IOUtils.closeQuietly(k2);
    }
  }
}
