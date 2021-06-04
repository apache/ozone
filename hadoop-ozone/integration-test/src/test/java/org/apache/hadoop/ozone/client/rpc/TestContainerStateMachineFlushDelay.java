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

import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.CertificateClientTestImpl;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.*;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.*;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.HDDS_SCM_WATCHER_TIMEOUT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;

/**
 * Tests the containerStateMachine failure handling by set flush delay.
 */
public class TestContainerStateMachineFlushDelay {

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
  private int chunkSize;
  private int flushSize;
  private int maxFlushSize;
  private int blockSize;
  private String keyString;

  /**
   * Create a MiniDFSCluster for testing.
   *
   * @throws IOException
   */
  @Before
  public void setup() throws Exception {
    chunkSize = 100;
    flushSize = 2 * chunkSize;
    maxFlushSize = 2 * flushSize;
    blockSize = 2 * maxFlushSize;
    keyString = UUID.randomUUID().toString();
    path = GenericTestUtils
        .getTempPath(TestContainerStateMachineFlushDelay.class.getSimpleName());
    File baseDir = new File(path);
    baseDir.mkdirs();

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
    //  conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());
    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1)
            .setBlockSize(blockSize)
            .setChunkSize(chunkSize)
            .setStreamBufferFlushSize(flushSize)
            .setStreamBufferMaxSize(maxFlushSize)
            .setStreamBufferSizeUnit(StorageUnit.BYTES)
            .setHbInterval(200)
            .setCertificateClient(new CertificateClientTestImpl(conf))
            .build();
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
    // Now ozone.client.stream.buffer.flush.delay is currently enabled
    // by default. Here we  written data(length 110) greater than chunk
    // Size(length 100), make sure flush will sync data.
    byte[] data =
        ContainerTestHelper.getFixedLengthString(keyString, 110)
        .getBytes(UTF_8);
    // First write and flush creates a container in the datanode
    key.write(data);
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
    Assert.assertTrue(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerState()
            == ContainerProtos.ContainerDataProto.State.UNHEALTHY);
  }

}
