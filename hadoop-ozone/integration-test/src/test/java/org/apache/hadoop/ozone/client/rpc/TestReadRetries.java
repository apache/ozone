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

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.apache.hadoop.ozone.om.request.OMRequestTestUtils.configureFSOptimizedPaths;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.Test;

class TestReadRetries {

  /**
   * Test read retries from multiple nodes in the pipeline.
   */
  @Test
  void testPutKeyAndGetKeyThreeNodes() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    configureFSOptimizedPaths(conf, true, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    try (MiniOzoneCluster cluster = newCluster(conf)) {
      cluster.waitForClusterToBeReady();
      cluster.waitForPipelineTobeReady(THREE, 180000);

      try (OzoneClient client = cluster.newClient()) {
        ObjectStore store = client.getObjectStore();

        String volumeName = UUID.randomUUID().toString();
        store.createVolume(volumeName);
        OzoneVolume volume = store.getVolume(volumeName);

        String bucketName = UUID.randomUUID().toString();
        volume.createBucket(bucketName);
        OzoneBucket bucket = volume.getBucket(bucketName);

        String keyName = "a/b/c/" + UUID.randomUUID();
        byte[] content = RandomUtils.secure().randomBytes(128);
        TestDataUtil.createKey(bucket, keyName,
            RatisReplicationConfig.getInstance(THREE), content);

        // First, confirm the key info from the client matches the info in OM.
        OmKeyArgs keyArgs = new OmKeyArgs.Builder()
            .setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setKeyName(keyName)
            .build();
        OmKeyLocationInfo keyInfo = cluster.getOzoneManager().lookupKey(keyArgs)
            .getKeyLocationVersions().get(0)
            .getBlocksLatestVersionOnly().get(0);
        long containerID = keyInfo.getContainerID();

        OzoneKeyDetails keyDetails = bucket.getKey(keyName);
        assertEquals(keyName, keyDetails.getName());

        List<OzoneKeyLocation> keyLocations = keyDetails.getOzoneKeyLocations();
        assertEquals(1, keyLocations.size());
        assertEquals(containerID, keyLocations.get(0).getContainerID());
        assertEquals(keyInfo.getLocalID(), keyLocations.get(0).getLocalID());

        // Make sure that the data size matched.
        assertEquals(content.length, keyLocations.get(0).getLength());

        StorageContainerManager scm = cluster.getStorageContainerManager();
        ContainerInfo container = scm.getContainerManager()
            .getContainer(ContainerID.valueOf(containerID));
        Pipeline pipeline = scm.getPipelineManager()
            .getPipeline(container.getPipelineID());
        List<DatanodeDetails> datanodes = pipeline.getNodes();
        assertEquals(3, datanodes.size());

        // shutdown the datanode
        cluster.shutdownHddsDatanode(datanodes.get(0));
        // try to read, this should be successful
        assertKeyContent(bucket, keyName, content);

        // shutdown the second datanode
        cluster.shutdownHddsDatanode(datanodes.get(1));
        // we still should be able to read
        assertKeyContent(bucket, keyName, content);

        // shutdown the 3rd datanode
        cluster.shutdownHddsDatanode(datanodes.get(2));
        // no longer can read it
        assertThrows(IOException.class,
            () -> assertKeyContent(bucket, keyName, content));

        // read intermediate directory
        verifyIntermediateDir(bucket, "a/b/c");
      }
    }
  }

  private static MiniOzoneCluster newCluster(OzoneConfiguration conf)
      throws IOException {
    return MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
  }

  private static void verifyIntermediateDir(OzoneBucket bucket, String dir)
      throws IOException {
    OzoneFileStatus fileStatus = bucket.getFileStatus(dir);
    assertTrue(fileStatus.isDirectory());
    assertEquals(dir, fileStatus.getTrimmedName());
  }
}
