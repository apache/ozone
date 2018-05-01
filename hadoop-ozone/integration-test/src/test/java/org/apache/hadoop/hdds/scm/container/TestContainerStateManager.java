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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.container;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerInfo;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.NavigableSet;
import java.util.Random;

/**
 * Tests for ContainerStateManager.
 */
public class TestContainerStateManager {

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster;
  private XceiverClientManager xceiverClientManager;
  private StorageContainerManager scm;
  private Mapping scmContainerMapping;
  private ContainerStateManager containerStateManager;
  private String containerOwner = "OZONE";


  @Before
  public void setup() throws Exception {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
    cluster.waitForClusterToBeReady();
    xceiverClientManager = new XceiverClientManager(conf);
    scm = cluster.getStorageContainerManager();
    scmContainerMapping = scm.getScmContainerManager();
    containerStateManager = scmContainerMapping.getStateManager();
  }

  @After
  public void cleanUp() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testAllocateContainer() throws IOException {
    // Allocate a container and verify the container info
    String container1 = "container" + RandomStringUtils.randomNumeric(5);
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    ContainerInfo info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED);
    Assert.assertEquals(container1, info.getContainerName());
    Assert.assertEquals(OzoneConsts.GB * 3, info.getAllocatedBytes());
    Assert.assertEquals(containerOwner, info.getOwner());
    Assert.assertEquals(xceiverClientManager.getType(),
        info.getPipeline().getType());
    Assert.assertEquals(xceiverClientManager.getFactor(),
        info.getPipeline().getFactor());
    Assert.assertEquals(HddsProtos.LifeCycleState.ALLOCATED, info.getState());

    // Check there are two containers in ALLOCATED state after allocation
    String container2 = "container" + RandomStringUtils.randomNumeric(5);
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container2, containerOwner);
    int numContainers = containerStateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(2, numContainers);
  }

  @Test
  public void testContainerStateManagerRestart() throws IOException {
    // Allocate 5 containers in ALLOCATED state and 5 in CREATING state
    String cname = "container" + RandomStringUtils.randomNumeric(5);
    for (int i = 0; i < 10; i++) {
      scm.getClientProtocolServer().allocateContainer(
          xceiverClientManager.getType(),
          xceiverClientManager.getFactor(), cname + i, containerOwner);
      if (i >= 5) {
        scm.getScmContainerManager()
            .updateContainerState(cname + i, HddsProtos.LifeCycleEvent.CREATE);
      }
    }

    // New instance of ContainerStateManager should load all the containers in
    // container store.
    ContainerStateManager stateManager =
        new ContainerStateManager(conf, scmContainerMapping
        );
    int containers = stateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(5, containers);
    containers = stateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CREATING).size();
    Assert.assertEquals(5, containers);
  }

  @Test
  public void testGetMatchingContainer() throws IOException {
    String container1 = "container-01234";
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    scmContainerMapping.updateContainerState(container1,
        HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container1,
        HddsProtos.LifeCycleEvent.CREATED);

    String container2 = "container-56789";
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container2, containerOwner);

    ContainerInfo info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.OPEN);
    Assert.assertEquals(container1, info.getContainerName());

    info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED);
    Assert.assertEquals(container2, info.getContainerName());

    scmContainerMapping.updateContainerState(container2,
        HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container2,
        HddsProtos.LifeCycleEvent.CREATED);

    // space has already been allocated in container1, now container 2 should
    // be chosen.
    info = containerStateManager
        .getMatchingContainer(OzoneConsts.GB * 3, containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.OPEN);
    Assert.assertEquals(container2, info.getContainerName());
  }

  @Test
  public void testUpdateContainerState() throws IOException {
    NavigableSet<ContainerID> containerList = containerStateManager
        .getMatchingContainerIDs(containerOwner,
            xceiverClientManager.getType(), xceiverClientManager.getFactor(),
            HddsProtos.LifeCycleState.ALLOCATED);
    int containers = containerList == null ? 0 : containerList.size();
    Assert.assertEquals(0, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // OPEN -> CLOSING -> CLOSED -> DELETING -> DELETED
    String container1 = "container" + RandomStringUtils.randomNumeric(5);
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.ALLOCATED).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping.updateContainerState(container1,
        HddsProtos.LifeCycleEvent.CREATE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CREATING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping.updateContainerState(container1,
        HddsProtos.LifeCycleEvent.CREATED);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.OPEN).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, HddsProtos.LifeCycleEvent.FINALIZE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, HddsProtos.LifeCycleEvent.CLOSE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, HddsProtos.LifeCycleEvent.DELETE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    scmContainerMapping
        .updateContainerState(container1, HddsProtos.LifeCycleEvent.CLEANUP);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETED).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // DELETING
    String container2 = "container" + RandomStringUtils.randomNumeric(5);
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container2, containerOwner);
    scmContainerMapping.updateContainerState(container2,
        HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping
        .updateContainerState(container2, HddsProtos.LifeCycleEvent.TIMEOUT);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.DELETING).size();
    Assert.assertEquals(1, containers);

    // Allocate container1 and update its state from ALLOCATED -> CREATING ->
    // OPEN -> CLOSING -> CLOSED
    String container3 = "container" + RandomStringUtils.randomNumeric(5);
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container3, containerOwner);
    scmContainerMapping.updateContainerState(container3,
        HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container3,
        HddsProtos.LifeCycleEvent.CREATED);
    scmContainerMapping.updateContainerState(container3,
        HddsProtos.LifeCycleEvent.FINALIZE);
    scmContainerMapping
        .updateContainerState(container3, HddsProtos.LifeCycleEvent.CLOSE);
    containers = containerStateManager.getMatchingContainerIDs(containerOwner,
        xceiverClientManager.getType(), xceiverClientManager.getFactor(),
        HddsProtos.LifeCycleState.CLOSED).size();
    Assert.assertEquals(1, containers);
  }

  @Test
  public void testUpdatingAllocatedBytes() throws Exception {
    String container1 = "container" + RandomStringUtils.randomNumeric(5);
    scm.getClientProtocolServer().allocateContainer(
        xceiverClientManager.getType(),
        xceiverClientManager.getFactor(), container1, containerOwner);
    scmContainerMapping.updateContainerState(container1,
        HddsProtos.LifeCycleEvent.CREATE);
    scmContainerMapping.updateContainerState(container1,
        HddsProtos.LifeCycleEvent.CREATED);

    Random ran = new Random();
    long allocatedSize = 0;
    for (int i = 0; i<5; i++) {
      long size = Math.abs(ran.nextLong() % OzoneConsts.GB);
      allocatedSize += size;
      // trigger allocating bytes by calling getMatchingContainer
      ContainerInfo info = containerStateManager
          .getMatchingContainer(size, containerOwner,
              xceiverClientManager.getType(), xceiverClientManager.getFactor(),
              HddsProtos.LifeCycleState.OPEN);
      Assert.assertEquals(container1, info.getContainerName());

      ContainerMapping containerMapping =
          (ContainerMapping)scmContainerMapping;
      // manually trigger a flush, this will persist the allocated bytes value
      // to disk
      containerMapping.flushContainerInfo();

      Charset utf8 = Charset.forName("UTF-8");
      // the persisted value should always be equal to allocated size.
      byte[] containerBytes =
          containerMapping.getContainerStore().get(container1.getBytes(utf8));
      HddsProtos.SCMContainerInfo infoProto =
          HddsProtos.SCMContainerInfo.PARSER.parseFrom(containerBytes);
      ContainerInfo currentInfo = ContainerInfo.fromProtobuf(infoProto);
      Assert.assertEquals(allocatedSize, currentInfo.getAllocatedBytes());
    }
  }
}
