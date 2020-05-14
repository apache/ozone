/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.container;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.ratis.RatisHelper;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.events.SCMEvents;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineNotFoundException;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.io.BlockOutputStreamEntry;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.common.impl.ContainerData;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.transport.server.XceiverServerSpi;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.XceiverServerRatis;

import org.apache.hadoop.test.GenericTestUtils;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.impl.RaftServerImpl;
import org.apache.ratis.server.impl.RaftServerProxy;
import org.apache.ratis.statemachine.StateMachine;
import org.junit.Assert;

/**
 * Helpers for container tests.
 */
public final class TestHelper {

  /**
   * Never constructed.
   */
  private TestHelper() {
  }

  public static boolean isContainerClosed(MiniOzoneCluster cluster,
      long containerID, DatanodeDetails datanode) {
    ContainerData containerData;
    for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes()) {
      if (datanode.equals(datanodeService.getDatanodeDetails())) {
        Container container =
            datanodeService.getDatanodeStateMachine().getContainer()
                .getContainerSet().getContainer(containerID);
        if (container != null) {
          containerData = container.getContainerData();
          return containerData.isClosed();
        }
      }
    }
    return false;
  }

  public static boolean isContainerPresent(MiniOzoneCluster cluster,
      long containerID, DatanodeDetails datanode) {
    for (HddsDatanodeService datanodeService : cluster.getHddsDatanodes()) {
      if (datanode.equals(datanodeService.getDatanodeDetails())) {
        Container container =
            datanodeService.getDatanodeStateMachine().getContainer()
                .getContainerSet().getContainer(containerID);
        if (container != null) {
          return true;
        }
      }
    }
    return false;
  }

  public static OzoneOutputStream createKey(String keyName,
      ReplicationType type, long size, ObjectStore objectStore,
      String volumeName, String bucketName) throws Exception {
    int replication =
        type == ReplicationType.STAND_ALONE ? 1 : 3;
    return objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, size, type, replication, new HashMap<>());
  }

  public static OzoneOutputStream createKey(String keyName,
      ReplicationType type,
      int replication, long size,
      ObjectStore objectStore, String volumeName, String bucketName)
      throws Exception {
    return objectStore.getVolume(volumeName).getBucket(bucketName)
        .createKey(keyName, size, type, replication, new HashMap<>());
  }

  public static void validateData(String keyName, byte[] data,
      ObjectStore objectStore, String volumeName, String bucketName)
      throws Exception {
    byte[] readData = new byte[data.length];
    OzoneInputStream is =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .readKey(keyName);
    is.read(readData);
    MessageDigest sha1 = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha1.update(data);
    MessageDigest sha2 = MessageDigest.getInstance(OzoneConsts.FILE_HASH);
    sha2.update(readData);
    Assert.assertTrue(Arrays.equals(sha1.digest(), sha2.digest()));
    is.close();
  }

  public static void waitForContainerClose(OzoneOutputStream outputStream,
      MiniOzoneCluster cluster) throws Exception {
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) outputStream.getOutputStream();
    List<BlockOutputStreamEntry> streamEntryList =
        keyOutputStream.getStreamEntries();
    List<Long> containerIdList = new ArrayList<>();
    for (BlockOutputStreamEntry entry : streamEntryList) {
      long id = entry.getBlockID().getContainerID();
      if (!containerIdList.contains(id)) {
        containerIdList.add(id);
      }
    }
    Assert.assertTrue(!containerIdList.isEmpty());
    waitForContainerClose(cluster, containerIdList.toArray(new Long[0]));
  }

  public static void waitForPipelineClose(OzoneOutputStream outputStream,
      MiniOzoneCluster cluster, boolean waitForContainerCreation)
      throws Exception {
    KeyOutputStream keyOutputStream =
        (KeyOutputStream) outputStream.getOutputStream();
    List<BlockOutputStreamEntry> streamEntryList =
        keyOutputStream.getStreamEntries();
    List<Long> containerIdList = new ArrayList<>();
    for (BlockOutputStreamEntry entry : streamEntryList) {
      long id = entry.getBlockID().getContainerID();
      if (!containerIdList.contains(id)) {
        containerIdList.add(id);
      }
    }
    Assert.assertTrue(!containerIdList.isEmpty());
    waitForPipelineClose(cluster, waitForContainerCreation,
        containerIdList.toArray(new Long[0]));
  }

  public static void waitForPipelineClose(MiniOzoneCluster cluster,
      boolean waitForContainerCreation, Long... containerIdList)
      throws TimeoutException, InterruptedException, IOException {
    List<Pipeline> pipelineList = new ArrayList<>();
    for (long containerID : containerIdList) {
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueof(containerID));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      if (!pipelineList.contains(pipeline)) {
        pipelineList.add(pipeline);
      }
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      if (waitForContainerCreation) {
        for (DatanodeDetails details : datanodes) {
          // Client will issue write chunk and it will create the container on
          // datanodes.
          // wait for the container to be created
          GenericTestUtils
              .waitFor(() -> isContainerPresent(cluster, containerID, details),
                  500, 100 * 1000);
          Assert.assertTrue(isContainerPresent(cluster, containerID, details));

          // make sure the container gets created first
          Assert.assertFalse(isContainerClosed(cluster, containerID, details));
        }
      }
    }
    waitForPipelineClose(pipelineList, cluster);
  }

  public static void waitForPipelineClose(List<Pipeline> pipelineList,
      MiniOzoneCluster cluster)
      throws TimeoutException, InterruptedException, IOException {
    for (Pipeline pipeline1 : pipelineList) {
      // issue pipeline destroy command
      cluster.getStorageContainerManager().getPipelineManager()
          .finalizeAndDestroyPipeline(pipeline1, false);
    }

    // wait for the pipeline to get destroyed in the datanodes
    for (Pipeline pipeline : pipelineList) {
      for (DatanodeDetails dn : pipeline.getNodes()) {
        XceiverServerSpi server =
            cluster.getHddsDatanodes().get(cluster.getHddsDatanodeIndex(dn))
                .getDatanodeStateMachine().getContainer().getWriteChannel();
        Assert.assertTrue(server instanceof XceiverServerRatis);
        XceiverServerRatis raftServer = (XceiverServerRatis) server;
        GenericTestUtils.waitFor(
            () -> (!raftServer.getPipelineIds().contains(pipeline.getId())),
            500, 100 * 1000);
      }
    }
  }

  public static void waitForContainerClose(MiniOzoneCluster cluster,
      Long... containerIdList)
      throws ContainerNotFoundException, PipelineNotFoundException,
      TimeoutException, InterruptedException {
    List<Pipeline> pipelineList = new ArrayList<>();
    for (long containerID : containerIdList) {
      ContainerInfo container =
          cluster.getStorageContainerManager().getContainerManager()
              .getContainer(ContainerID.valueof(containerID));
      Pipeline pipeline =
          cluster.getStorageContainerManager().getPipelineManager()
              .getPipeline(container.getPipelineID());
      pipelineList.add(pipeline);
      List<DatanodeDetails> datanodes = pipeline.getNodes();

      for (DatanodeDetails details : datanodes) {
        // Client will issue write chunk and it will create the container on
        // datanodes.
        // wait for the container to be created
        GenericTestUtils
            .waitFor(() -> isContainerPresent(cluster, containerID, details),
                500, 100 * 1000);
        Assert.assertTrue(isContainerPresent(cluster, containerID, details));

        // make sure the container gets created first
        Assert.assertFalse(isContainerClosed(cluster, containerID, details));
        // send the order to close the container
        cluster.getStorageContainerManager().getEventQueue()
            .fireEvent(SCMEvents.CLOSE_CONTAINER,
                ContainerID.valueof(containerID));
      }
    }
    int index = 0;
    for (long containerID : containerIdList) {
      Pipeline pipeline = pipelineList.get(index);
      List<DatanodeDetails> datanodes = pipeline.getNodes();
      // Below condition avoids the case where container has been allocated
      // but not yet been used by the client. In such a case container is never
      // created.
      for (DatanodeDetails datanodeDetails : datanodes) {
        GenericTestUtils.waitFor(
            () -> isContainerClosed(cluster, containerID, datanodeDetails), 500,
            15 * 1000);
        //double check if it's really closed
        // (waitFor also throws an exception)
        Assert.assertTrue(
            isContainerClosed(cluster, containerID, datanodeDetails));
      }
      index++;
    }
  }

  public static StateMachine getStateMachine(MiniOzoneCluster cluster)
      throws Exception {
    return getStateMachine(cluster.getHddsDatanodes().get(0), null);
  }

  private static RaftServerImpl getRaftServerImpl(HddsDatanodeService dn,
      Pipeline pipeline) throws Exception {
    XceiverServerSpi server = dn.getDatanodeStateMachine().
        getContainer().getWriteChannel();
    RaftServerProxy proxy =
        (RaftServerProxy) (((XceiverServerRatis) server).getServer());
    RaftGroupId groupId =
        pipeline == null ? proxy.getGroupIds().iterator().next() :
            RatisHelper.newRaftGroup(pipeline).getGroupId();
    return proxy.getImpl(groupId);
  }

  public static StateMachine getStateMachine(HddsDatanodeService dn,
      Pipeline pipeline) throws Exception {
    return getRaftServerImpl(dn, pipeline).getStateMachine();
  }

}
