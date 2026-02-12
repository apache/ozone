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

package org.apache.hadoop.ozone.fsck;

import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_DB_DIRS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test cases for ContainerMapper.
 */
public class TestContainerMapper {
  @TempDir
  private static Path dbPath;
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static String volName = UUID.randomUUID().toString();
  private static String bucketName = UUID.randomUUID().toString();
  private static OzoneConfiguration conf;
  private static List<String> keyList = new ArrayList<>();

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.set(OZONE_OM_DB_DIRS, dbPath.toString());
    conf.set(ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE, "100MB");
    conf.setStorageSize(OZONE_DATANODE_RATIS_VOLUME_FREE_SPACE_MIN,
        0, StorageUnit.MB);
    // By default, 2 pipelines are created. Setting the value to 6, will ensure
    // each pipleine can have 3 containers open.
    conf.setInt(ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 6);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    ObjectStore store = ozClient.getObjectStore();
    store.createVolume(volName);
    OzoneVolume volume = store.getVolume(volName);
    // TODO: HDDS-5463
    //  Recon's container ID to key mapping does not yet support FSO buckets.
    volume.createBucket(bucketName, BucketArgs.newBuilder()
            .setBucketLayout(BucketLayout.OBJECT_STORE)
            .build());
    OzoneBucket bucket = volume.getBucket(bucketName);
    byte[] data = generateData(10 * 1024 * 1024, (byte)98);

    for (int i = 0; i < 20; i++) {
      String key = UUID.randomUUID().toString();
      keyList.add(key);
      OzoneOutputStream out = bucket.createKey(key, data.length,
          ReplicationType.STAND_ALONE, ReplicationFactor.ONE,
          new HashMap<String, String>());
      out.write(data, 0, data.length);
      out.close();
    }
    cluster.stop();
  }

  @Test
  public void testContainerMapper() throws Exception {
    ContainerMapper containerMapper = new ContainerMapper();
    Map<Long, List<Map<Long, BlockIdDetails>>> dataMap =
        containerMapper.parseOmDB(conf);
    // As we have created 20 keys with 10 MB size, and each
    // container max size is 100 MB, it should create 3 containers because
    // containers are closing before reaching the threshold
    assertEquals(3, dataMap.size());
  }

  private static byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }

  @AfterAll
  public static void shutdown() throws IOException {
    IOUtils.closeQuietly(ozClient);
    cluster.shutdown();
  }
}
