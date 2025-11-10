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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_BLOCK_TOKEN_ENABLED;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_COMMAND_STATUS_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClientTestImpl;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.SecretKeyTestClient;
import org.apache.hadoop.ozone.client.io.KeyOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests the containerStateMachine failure handling by set flush delay.
 */
public class TestContainerStateMachineFlushDelay {
  private static final int CHUNK_SIZE = 100;
  private static final int FLUSH_SIZE = 2 * CHUNK_SIZE;
  private static final int MAX_FLUSH_SIZE = 2 * FLUSH_SIZE;
  private static final int BLOCK_SIZE = 2 * MAX_FLUSH_SIZE;

  private MiniOzoneCluster cluster;
  private OzoneConfiguration conf = new OzoneConfiguration();
  private OzoneClient client;
  private ObjectStore objectStore;
  private String volumeName;
  private String bucketName;
  private String keyString;

  @BeforeEach
  public void setup() throws Exception {
    keyString = UUID.randomUUID().toString();

    conf.setBoolean(HDDS_BLOCK_TOKEN_ENABLED, true);
    //  conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_COMMAND_STATUS_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_SCM_STALENODE_INTERVAL, 3, TimeUnit.SECONDS);
    conf.setQuietMode(false);
    OzoneManager.setTestSecureOmFlag(true);
    conf.setLong(OzoneConfigKeys.HDDS_RATIS_SNAPSHOT_THRESHOLD_KEY, 1);
    //  conf.set(HADOOP_SECURITY_AUTHENTICATION, KERBEROS.toString());
    conf.set(OzoneConfigKeys.OZONE_SCM_CLOSE_CONTAINER_WAIT_DURATION, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "2s");
    conf.set(ScmConfigKeys.OZONE_SCM_PIPELINE_DESTROY_TIMEOUT, "5s");

    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setBlockSize(BLOCK_SIZE)
        .setChunkSize(CHUNK_SIZE)
        .setStreamBufferFlushSize(FLUSH_SIZE)
        .setStreamBufferMaxSize(MAX_FLUSH_SIZE)
        .applyTo(conf);

    cluster =
        MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1)
            .setCertificateClient(new CertificateClientTestImpl(conf))
            .setSecretKeyClient(new SecretKeyTestClient())
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

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testContainerStateMachineFailures() throws Exception {
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey("ratis", 1024,
                ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                    ReplicationFactor.ONE), new HashMap<>());
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
    assertEquals(1, locationInfoList.size());
    OmKeyLocationInfo omKeyLocationInfo = locationInfoList.get(0);

    // delete the container dir
    FileUtil.fullyDelete(new File(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID()).getContainerData()
            .getContainerPath()));

    key.close();
    // Make sure the container is marked unhealthy
    assertSame(
        cluster.getHddsDatanodes().get(0).getDatanodeStateMachine()
            .getContainer().getContainerSet()
            .getContainer(omKeyLocationInfo.getContainerID())
            .getContainerState(),
        ContainerProtos.ContainerDataProto.State.UNHEALTHY);
  }

}
