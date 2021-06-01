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
package org.apache.hadoop.ozone.dn.volume;

import org.apache.hadoop.hdds.DFSConfigKeysLegacy;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.ozone.HddsDatanodeService;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.Storage;
import org.apache.hadoop.ozone.container.common.interfaces.Container;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeConfiguration;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.ozoneimpl.OzoneContainer;
import org.apache.hadoop.ozone.dn.DatanodeTestUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Rule;
import org.junit.rules.Timeout;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.STAND_ALONE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CONTAINER_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_REPLICATION;

/**
 * This class tests datanode can detect failed volumes.
 */
public class TestDatanodeHddsVolumeFailureDetection {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = Timeout.seconds(300);
  private MiniOzoneCluster cluster;
  private OzoneConfiguration ozoneConfig;
  private OzoneClient ozClient = null;
  private ScmClient scmClient;
  private ObjectStore store = null;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private List<HddsDatanodeService> datanodes;

  @Before
  public void init() throws Exception {
    ozoneConfig = new OzoneConfiguration();
    ozoneConfig.set(OZONE_SCM_CONTAINER_SIZE, "1GB");
    ozoneConfig.setInt(OZONE_REPLICATION, ReplicationFactor.ONE.getValue());
    // keep the cache size = 1, so we could trigger io exception on
    // reading on-disk db instance
    ozoneConfig.setInt(OZONE_CONTAINER_CACHE_SIZE, 1);
    // shorten the gap between successive checks to ease tests
    ozoneConfig.setTimeDuration(
        DFSConfigKeysLegacy.DFS_DATANODE_DISK_CHECK_MIN_GAP_KEY, 5,
        TimeUnit.SECONDS);
    // set tolerated = 1
    DatanodeConfiguration dnConf =
        ozoneConfig.getObject(DatanodeConfiguration.class);
    dnConf.setFailedVolumesTolerated(1);
    ozoneConfig.setFromObject(dnConf);
    cluster = MiniOzoneCluster.newBuilder(ozoneConfig)
        .setNumDatanodes(1)
        .setNumDataVolumes(1)
        .build();
    cluster.waitForClusterToBeReady();

    ozClient = OzoneClientFactory.getRpcClient(ozoneConfig);
    store = ozClient.getObjectStore();
    scmClient = new ContainerOperationClient(ozoneConfig);

    String volumeName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);

    String bucketName = UUID.randomUUID().toString();
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);

    datanodes = cluster.getHddsDatanodes();
  }

  @After
  public void shutdown() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testHddsVolumeFailureOnChunkFileCorrupt() throws Exception {
    // write a file
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());

    // corrupt chunk file by rename file->dir
    HddsDatanodeService dn = datanodes.get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    MutableVolumeSet volSet = oc.getVolumeSet();
    HddsVolume vol0 = volSet.getVolumesList().get(0);
    File clusterDir = DatanodeTestUtils.getHddsVolumeClusterDir(vol0);
    File currentDir = new File(clusterDir, Storage.STORAGE_DIR_CURRENT);
    File containerTopDir = new File(currentDir, Storage.CONTAINER_DIR + "0");
    File containerDir = new File(containerTopDir, "1");
    File chunksDir = new File(containerDir, OzoneConsts.STORAGE_DIR_CHUNKS);
    File[] chunkFiles = chunksDir.listFiles();
    Assert.assertNotNull(chunkFiles);
    for (File chunkFile : chunkFiles) {
      DatanodeTestUtils.injectDataFileFailure(chunkFile);
    }

    // simulate bad volume by removing write permission on root dir
    // refer to HddsVolume.check()
    DatanodeTestUtils.simulateBadVolume(vol0);

    // read written file to trigger checkVolumeAsync
    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[value.getBytes(UTF_8).length];

    try {
      is.read(fileContent);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    } finally {
      is.close();
    }

    // should trigger checkVolumeAsync and
    // a failed volume should be detected
    DatanodeTestUtils.waitForCheckVolume(volSet, 1L);
    DatanodeTestUtils.waitForHandleFailedVolume(volSet, 1);

    // restore for cleanup
    DatanodeTestUtils.restoreBadVolume(vol0);
    for (File chunkFile : chunkFiles) {
      DatanodeTestUtils.restoreDataFileFromFailure(chunkFile);
    }
  }

  @Test
  public void testHddsVolumeFailureOnContainerFileCorrupt() throws Exception {
    // create a container
    ContainerWithPipeline container = scmClient.createContainer(HddsProtos
        .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
        .ONE, OzoneConsts.OZONE);

    // corrupt container file by removing write permission on
    // container metadata dir, since container update operation
    // use a create temp & rename way, so we can't just rename
    // container file to simulate corruption
    HddsDatanodeService dn = datanodes.get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    MutableVolumeSet volSet = oc.getVolumeSet();
    HddsVolume vol0 = volSet.getVolumesList().get(0);
    Container c1 = oc.getContainerSet().getContainer(container.
        getContainerInfo().getContainerID());
    File metadataDir = new File(c1.getContainerFile().getParent());
    DatanodeTestUtils.injectContainerMetaDirFailure(metadataDir);

    // simulate bad volume by removing write permission on root dir
    // refer to HddsVolume.check()
    DatanodeTestUtils.simulateBadVolume(vol0);

    // close container to trigger checkVolumeAsync
    try {
      c1.close();
      Assert.fail();
    } catch(Exception e) {
      Assert.assertTrue(e instanceof IOException);
    }

    // should trigger CheckVolumeAsync and
    // a failed volume should be detected
    DatanodeTestUtils.waitForCheckVolume(volSet, 1L);
    DatanodeTestUtils.waitForHandleFailedVolume(volSet, 1);

    // restore for cleanup
    DatanodeTestUtils.restoreBadVolume(vol0);
    DatanodeTestUtils.restoreContainerMetaDirFromFailure(metadataDir);
  }

  @Test
  public void testHddsVolumeFailureOnDbFileCorrupt() throws Exception {
    // write a file, will create container1
    String keyName = UUID.randomUUID().toString();
    String value = "sample value";
    OzoneOutputStream out = bucket.createKey(keyName,
        value.getBytes(UTF_8).length, STAND_ALONE,
        ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();
    OzoneKey key = bucket.getKey(keyName);
    Assert.assertEquals(keyName, key.getName());

    // close container1
    HddsDatanodeService dn = datanodes.get(0);
    OzoneContainer oc = dn.getDatanodeStateMachine().getContainer();
    Container c1 = oc.getContainerSet().getContainer(1);
    c1.close();

    // create container2, and container1 is kicked out of cache
    ContainerWithPipeline c2 = scmClient.createContainer(HddsProtos
        .ReplicationType.STAND_ALONE, HddsProtos.ReplicationFactor
        .ONE, OzoneConsts.OZONE);
    Assert.assertTrue(c2.getContainerInfo().getState()
        .equals(HddsProtos.LifeCycleState.OPEN));

    // corrupt db by rename dir->file
    File metadataDir = new File(c1.getContainerFile().getParent());
    File dbDir = new File(metadataDir, "1" + OzoneConsts.DN_CONTAINER_DB);
    DatanodeTestUtils.injectDataDirFailure(dbDir);

    // simulate bad volume by removing write permission on root dir
    // refer to HddsVolume.check()
    MutableVolumeSet volSet = oc.getVolumeSet();
    HddsVolume vol0 = volSet.getVolumesList().get(0);
    DatanodeTestUtils.simulateBadVolume(vol0);

    // read written file to trigger checkVolumeAsync
    OzoneInputStream is = bucket.readKey(keyName);
    byte[] fileContent = new byte[value.getBytes(UTF_8).length];

    try {
      is.read(fileContent);
      Assert.fail();
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IOException);
    } finally {
      is.close();
    }

    // should trigger CheckVolumeAsync and
    // a failed volume should be detected
    DatanodeTestUtils.waitForCheckVolume(volSet, 1L);
    DatanodeTestUtils.waitForHandleFailedVolume(volSet, 1);

    // restore all
    DatanodeTestUtils.restoreBadVolume(vol0);
    DatanodeTestUtils.restoreDataDirFromFailure(dbDir);
  }
}
