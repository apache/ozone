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

package org.apache.hadoop.ozone.om.service;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.client.DeletedBlock;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.block.BlockManager;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.common.DeletedBlockGroup;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * DeletionService test to Pass Usage from OM to SCM.
 */
public class TestDeletionService {

  private static final String VOLUME_NAME = "vol1";
  private static final String BUCKET_NAME = "bucket1";
  private static final String KEY_NAME = "testkey";
  private static final int KEY_SIZE = 5 * 1024; // 5 KB

  @Test
  public void testDeletedKeyBytesPropagatedToSCM() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100, TimeUnit.MILLISECONDS);
    MiniOzoneCluster cluster = null;

    try {
      // Step 1: Start MiniOzoneCluster
      cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(1).build();
      cluster.waitForClusterToBeReady();

      // Step 2: Create volume, bucket, and write a key
      OzoneClient client = cluster.newClient();
      client.getObjectStore().createVolume(VOLUME_NAME);
      client.getObjectStore().getVolume(VOLUME_NAME).createBucket(BUCKET_NAME);
      OzoneBucket bucket = client.getObjectStore().getVolume(VOLUME_NAME).getBucket(BUCKET_NAME);
      byte[] data = new byte[KEY_SIZE];
      try (OzoneOutputStream out = bucket.createKey(KEY_NAME, KEY_SIZE,
          ReplicationType.RATIS, ReplicationFactor.ONE, new HashMap<>())) {
        out.write(data);
      }

      // Step 3: Spy on BlockManager and inject it into SCM
      StorageContainerManager scm = cluster.getStorageContainerManager();
      BlockManager realManager = scm.getScmBlockManager();
      BlockManager spyManager = spy(realManager);

      Field field = scm.getClass().getDeclaredField("scmBlockManager");
      field.setAccessible(true);
      field.set(scm, spyManager);

      // Step 4: Delete the key (which triggers deleteBlocks call)
      bucket.deleteKey(KEY_NAME);

      // Step 5: Verify deleteBlocks call and capture argument
      ArgumentCaptor<List<DeletedBlockGroup>> captor =
          ArgumentCaptor.forClass(List.class);
      verify(spyManager, timeout(50000)).deleteBlocks(captor.capture());

      // Step 6: Calculate and assert used bytes
      long totalUsedBytes = captor.getValue().stream()
          .flatMap(group -> group.getAllBlocks().stream())
          .mapToLong(DeletedBlock::getUsedBytes)
          .sum();
      assertEquals(KEY_SIZE, totalUsedBytes);
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
