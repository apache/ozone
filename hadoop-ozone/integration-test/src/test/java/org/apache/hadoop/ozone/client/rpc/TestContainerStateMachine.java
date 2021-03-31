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

package org.apache.hadoop.ozone.client.rpc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.TestHelper;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.ContainerStateMachine;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.RatisServerConfiguration;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.test.GenericTestUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * Tests the containerStateMachine failure handling.
 */
public class TestContainerStateMachine {

  /**
    * Set a timeout for each test.
    */
  @Rule
  public Timeout timeout = Timeout.seconds(300);

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private String path;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void setup() throws Exception {
    path = GenericTestUtils
        .getTempPath(TestContainerStateMachine.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();

    conf.setInt(ScmConfigKeys.OZONE_DATANODE_PIPELINE_LIMIT, 1);
    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    //  conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_SCM_WATCHER_TIMEOUT, 1000, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    OzoneManager.setTestSecureOmFlag(true);
    conf.setLong(OzoneConfigKeys.DFS_RATIS_SNAPSHOT_THRESHOLD_KEY, 1);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setStreamBufferFlushDelay(false);
    conf.setFromObject(clientConfig);

    //  conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1)
            .setHbInterval(200)
            .setCertificateClient(new CertificateClientTestImpl(conf))
            .build();
    cluster.setWaitForClusterToBeReadyTimeout(300000);
    cluster.waitForClusterToBeReady();
    cluster.getOzoneManager().startSecretManager();
    //the easiest way to create an open container is creating a key
    client = OzoneClientFactory.getRpcClient(conf);
    objectStore = client.getObjectStore();
    volumeName = "testcontainerstatemachinefailures";
    bucketName = volumeName;
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
  }

  /**
   * Shutdown MiniDFSCluster.
   */
  @After
  public void shutdown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerStateMachineFailures() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    // First write and flush creates a container in the datanode
    key.write("ratis".getBytes(UTF_8));
    key.flush();
    key.write("ratis".getBytes(UTF_8));

    //get the name of a valid container
    KeyOutputStream groupOutputStream =
        (KeyOutputStream) key.getOutputStream();

    List<OmKeyLocationInfo> locationInfoList =
        groupOutputStream.getLocationInfoList();
    Assert.assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);

    // delete the container dir
    FileUtil.fullyDelete(new File(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID()).getContainerData()
            .getContainerPath()));

    key.close();
    // Make sure the container is marked unhealthy
    Assert.assertEquals(
        ContainerProtos.ContainerDataProto.State.UNHEALTHY,
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerState());
  }

  @Test
  public void testRatisSnapshotRetention() throws Exception {

    ContainerStateMachine stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(cluster);
    SimpleStateMachineStorage storage =
        (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    Assert.assertNull(storage.findLatestSnapshot());

    // Write 10 keys. Num snapshots should be equal to config value.
    for (int i = 1; i <= 10; i++) {
      OzoneOutputStream key =
          objectStore.getVolume(volumeName).getBucket(bucketName)
              .createKey(("ratis" + i), 1024, ReplicationType.RATIS,
                  ReplicationFactor.ONE, new HashMap<>());
      // First write and flush creates a container in the datanode
      key.write(("ratis" + i).getBytes(UTF_8));
      key.flush();
      key.write(("ratis" + i).getBytes(UTF_8));
      key.close();
    }

    RatisServerConfiguration ratisServerConfiguration =
        conf.getObject(RatisServerConfiguration.class);

    stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(cluster);
    storage = (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    Path parentPath = storage.findLatestSnapshot().getFile().getPath();
    int numSnapshots = parentPath.getParent().toFile().listFiles().length;
    Assert.assertTrue(Math.abs(ratisServerConfiguration
        .getNumSnapshotsRetained() - numSnapshots) <= 1);

    // Write 10 more keys. Num Snapshots should remain the same.
    for (int i = 11; i <= 20; i++) {
      OzoneOutputStream key =
          objectStore.getVolume(volumeName).getBucket(bucketName)
              .createKey(("ratis" + i), 1024, ReplicationType.RATIS,
                  ReplicationFactor.ONE, new HashMap<>());
      // First write and flush creates a container in the datanode
      key.write(("ratis" + i).getBytes(UTF_8));
      key.flush();
      key.write(("ratis" + i).getBytes(UTF_8));
      key.close();
    }
    stateMachine =
        (ContainerStateMachine) TestHelper.getStateMachine(cluster);
    storage = (SimpleStateMachineStorage) stateMachine.getStateMachineStorage();
    parentPath = storage.findLatestSnapshot().getFile().getPath();
    numSnapshots = parentPath.getParent().toFile().listFiles().length;
    Assert.assertTrue(Math.abs(ratisServerConfiguration
        .getNumSnapshotsRetained() - numSnapshots) <= 1);
  }

}
