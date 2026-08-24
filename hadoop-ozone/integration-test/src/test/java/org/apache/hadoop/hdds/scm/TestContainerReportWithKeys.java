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

package org.apache.hadoop.hdds.scm;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.HashMap;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplica;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests container report with DN container state info.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestContainerReportWithKeys implements NonHATests.TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(
      TestContainerReportWithKeys.class);
  private OzoneClient client;
  private StorageContainerManager scm;

  @BeforeAll
  void init() throws Exception {
    client = cluster().newClient();
    scm = cluster().getStorageContainerManager();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void testContainerReportKeyWrite() throws Exception {
    final String volumeName = "volume" + RandomStringUtils.secure().nextNumeric(5);
    final String bucketName = "bucket" + RandomStringUtils.secure().nextNumeric(5);
    final String keyName = "key" + RandomStringUtils.secure().nextNumeric(5);
    final int keySize = 100;

    ObjectStore objectStore = client.getObjectStore();
    objectStore.createVolume(volumeName);
    objectStore.getVolume(volumeName).createBucket(bucketName);
    OzoneOutputStream key =
        objectStore.getVolume(volumeName).getBucket(bucketName)
            .createKey(keyName, keySize, ReplicationType.RATIS,
                ReplicationFactor.ONE, new HashMap<>());
    String dataString = RandomStringUtils.secure().nextAlphabetic(keySize);
    key.write(dataString.getBytes(UTF_8));
    key.close();

    OmKeyArgs keyArgs = new OmKeyArgs.Builder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setReplicationConfig(
            StandaloneReplicationConfig
                .getInstance(HddsProtos.ReplicationFactor.ONE))
        .setDataSize(keySize)
        .build();


    OmKeyLocationInfo keyInfo =
        cluster().getOzoneManager().lookupKey(keyArgs).getKeyLocationVersions()
            .get(0).getBlocksLatestVersionOnly().get(0);


    ContainerInfo cinfo = scm.getContainerInfo(keyInfo.getContainerID());
    Set<ContainerReplica> replicas =
        scm.getContainerManager().getContainerReplicas(
            ContainerID.valueOf(keyInfo.getContainerID()));
    assertEquals(1, replicas.size());
    replicas.stream().forEach(rp ->
        assertNotNull(rp.getDatanodeDetails().getParent()));

    LOG.info("SCM Container Info keyCount: {} usedBytes: {}",
        cinfo.getNumberOfKeys(), cinfo.getUsedBytes());
  }

}
