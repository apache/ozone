/**
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
package org.apache.hadoop.hdds.scm.container;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.TestUtils;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.Pipeline;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos;
import org.apache.hadoop.hdds.protocol.proto
    .StorageContainerDatanodeProtocolProtos.ContainerReportsProto;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.container.common.SCMTestUtils;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Tests for Container Mapping.
 */
public class TestContainerMapping {
  private static ContainerMapping mapping;
  private static MockNodeManager nodeManager;
  private static File testDir;
  private static XceiverClientManager xceiverClientManager;
  private static String containerOwner = "OZONE";
  private static Random random;

  private static final long TIMEOUT = 10000;

  @Rule
  public ExpectedException thrown = ExpectedException.none();
  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = SCMTestUtils.getConf();

    testDir = GenericTestUtils
        .getTestDir(TestContainerMapping.class.getSimpleName());
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS,
        testDir.getAbsolutePath());
    conf.setTimeDuration(
        ScmConfigKeys.OZONE_SCM_CONTAINER_CREATION_LEASE_TIMEOUT,
        TIMEOUT,
        TimeUnit.MILLISECONDS);
    boolean folderExisted = testDir.exists() || testDir.mkdirs();
    if (!folderExisted) {
      throw new IOException("Unable to create test directory path");
    }
    nodeManager = new MockNodeManager(true, 10);
    mapping = new ContainerMapping(conf, nodeManager, 128);
    xceiverClientManager = new XceiverClientManager(conf);
    random = new Random();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    if(mapping != null) {
      mapping.close();
    }
    FileUtil.fullyDelete(testDir);
  }

  @Before
  public void clearChillMode() {
    nodeManager.setChillmode(false);
  }

  @Test
  public void testallocateContainer() throws Exception {
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    Assert.assertNotNull(containerInfo);
  }

  @Test
  public void testallocateContainerDistributesAllocation() throws Exception {
    /* This is a lame test, we should really be testing something like
    z-score or make sure that we don't have 3sigma kind of events. Too lazy
    to write all that code. This test very lamely tests if we have more than
    5 separate nodes  from the list of 10 datanodes that got allocated a
    container.
     */
    Set<UUID> pipelineList = new TreeSet<>();
    for (int x = 0; x < 30; x++) {
      ContainerInfo containerInfo = mapping.allocateContainer(
          xceiverClientManager.getType(),
          xceiverClientManager.getFactor(),
          containerOwner);

      Assert.assertNotNull(containerInfo);
      Assert.assertNotNull(containerInfo.getPipeline());
      pipelineList.add(containerInfo.getPipeline().getLeader()
          .getUuid());
    }
    Assert.assertTrue(pipelineList.size() > 5);
  }

  @Test
  public void testGetContainer() throws IOException {
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    Pipeline pipeline  = containerInfo.getPipeline();
    Assert.assertNotNull(pipeline);
    Pipeline newPipeline = mapping.getContainer(
        containerInfo.getContainerID()).getPipeline();
    Assert.assertEquals(pipeline.getLeader().getUuid(),
        newPipeline.getLeader().getUuid());
  }

  @Test
  public void testgetNoneExistentContainer() throws IOException {
    thrown.expectMessage("Specified key does not exist.");
    mapping.getContainer(random.nextLong());
  }

  @Test
  public void testChillModeAllocateContainerFails() throws IOException {
    nodeManager.setChillmode(true);
    thrown.expectMessage("Unable to create container while in chill mode");
    mapping.allocateContainer(xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), containerOwner);
  }

  @Test
  public void testContainerCreationLeaseTimeout() throws IOException,
      InterruptedException {
    nodeManager.setChillmode(false);
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    mapping.updateContainerState(containerInfo.getContainerID(),
        HddsProtos.LifeCycleEvent.CREATE);
    Thread.sleep(TIMEOUT + 1000);

    NavigableSet<ContainerID> deleteContainers = mapping.getStateManager()
        .getMatchingContainerIDs(
            "OZONE",
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.DELETING);
    Assert.assertTrue(deleteContainers.contains(containerInfo.containerID()));

    thrown.expect(IOException.class);
    thrown.expectMessage("Lease Exception");
    mapping.updateContainerState(containerInfo.getContainerID(),
        HddsProtos.LifeCycleEvent.CREATED);
  }

  @Test
  public void testFullContainerReport() throws IOException {
    ContainerInfo info = createContainer();
    DatanodeDetails datanodeDetails = TestUtils.getDatanodeDetails();
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo> reports =
        new ArrayList<>();
    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    ciBuilder.setFinalhash("e16cc9d6024365750ed8dbd194ea46d2")
        .setSize(5368709120L)
        .setUsed(2000000000L)
        .setKeyCount(100000000L)
        .setReadCount(100000000L)
        .setWriteCount(100000000L)
        .setReadBytes(2000000000L)
        .setWriteBytes(2000000000L)
        .setContainerID(info.getContainerID());

    reports.add(ciBuilder.build());

    ContainerReportsProto.Builder crBuilder = ContainerReportsProto
        .newBuilder();
    crBuilder.addAllReports(reports);

    mapping.processContainerReports(datanodeDetails, crBuilder.build());

    ContainerInfo updatedContainer =
        mapping.getContainer(info.getContainerID());
    Assert.assertEquals(100000000L,
        updatedContainer.getNumberOfKeys());
    Assert.assertEquals(2000000000L, updatedContainer.getUsedBytes());
  }

  @Test
  public void testContainerCloseWithContainerReport() throws IOException {
    ContainerInfo info = createContainer();
    DatanodeDetails datanodeDetails = TestUtils.getDatanodeDetails();
    List<StorageContainerDatanodeProtocolProtos.ContainerInfo> reports =
        new ArrayList<>();

    StorageContainerDatanodeProtocolProtos.ContainerInfo.Builder ciBuilder =
        StorageContainerDatanodeProtocolProtos.ContainerInfo.newBuilder();
    ciBuilder.setFinalhash("7c45eb4d7ed5e0d2e89aaab7759de02e")
        .setSize(5368709120L)
        .setUsed(5368705120L)
        .setKeyCount(500000000L)
        .setReadCount(500000000L)
        .setWriteCount(500000000L)
        .setReadBytes(5368705120L)
        .setWriteBytes(5368705120L)
        .setContainerID(info.getContainerID());

    reports.add(ciBuilder.build());

    ContainerReportsProto.Builder crBuilder =
        ContainerReportsProto.newBuilder();
    crBuilder.addAllReports(reports);

    mapping.processContainerReports(datanodeDetails, crBuilder.build());

    ContainerInfo updatedContainer =
        mapping.getContainer(info.getContainerID());
    Assert.assertEquals(500000000L,
        updatedContainer.getNumberOfKeys());
    Assert.assertEquals(5368705120L, updatedContainer.getUsedBytes());
    NavigableSet<ContainerID> pendingCloseContainers = mapping.getStateManager()
        .getMatchingContainerIDs(
            containerOwner,
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.CLOSING);
    Assert.assertTrue(
         pendingCloseContainers.contains(updatedContainer.containerID()));
  }

  @Test
  public void testCloseContainer() throws IOException {
    ContainerInfo info = createContainer();
    mapping.updateContainerState(info.getContainerID(),
        HddsProtos.LifeCycleEvent.FINALIZE);
    NavigableSet<ContainerID> pendingCloseContainers = mapping.getStateManager()
        .getMatchingContainerIDs(
            containerOwner,
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.CLOSING);
    Assert.assertTrue(pendingCloseContainers.contains(info.containerID()));
    mapping.updateContainerState(info.getContainerID(),
        HddsProtos.LifeCycleEvent.CLOSE);
    NavigableSet<ContainerID> closeContainers = mapping.getStateManager()
        .getMatchingContainerIDs(
            containerOwner,
            xceiverClientManager.getType(),
            xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.CLOSED);
    Assert.assertTrue(closeContainers.contains(info.containerID()));
  }

  /**
   * Creates a container with the given name in ContainerMapping.
   * @throws IOException
   */
  private ContainerInfo createContainer()
      throws IOException {
    nodeManager.setChillmode(false);
    ContainerInfo containerInfo = mapping.allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(),
        containerOwner);
    mapping.updateContainerState(containerInfo.getContainerID(),
        HddsProtos.LifeCycleEvent.CREATE);
    mapping.updateContainerState(containerInfo.getContainerID(),
        HddsProtos.LifeCycleEvent.CREATED);
    return containerInfo;
  }

}
