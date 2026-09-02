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

package org.apache.hadoop.ozone.om;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_BLOCK_DELETING_SERVICE_INTERVAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.container.ContainerTestHelper;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.service.KeyDeletingService;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test OM's {@link KeyDeletingService}.
 */
public class TestKeyPurging {

  private MiniOzoneCluster cluster;
  private ObjectStore store;
  private OzoneManager om;

  private static final int NUM_KEYS = 10;
  private static final int KEY_SIZE = 100;
  private OzoneClient client;

  @BeforeEach
  public void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(OZONE_BLOCK_DELETING_SERVICE_INTERVAL, 100,
        TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200,
        TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);

    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(3)
        .build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    store = client.getObjectStore();
    om = cluster.getOzoneManager();
  }

  @AfterEach
  public void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testKeysPurgingByKeyDeletingService() throws Exception {
    // Create Volume and Bucket
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    // Create some keys and write data into them
    String keyBase = UUID.randomUUID().toString();
    String keyString = UUID.randomUUID().toString();
    byte[] data = ContainerTestHelper.getFixedLengthString(
        keyString, KEY_SIZE).getBytes(UTF_8);
    List<String> keys = new ArrayList<>(NUM_KEYS);
    for (int i = 1; i <= NUM_KEYS; i++) {
      String keyName = keyBase + "-" + i;
      keys.add(keyName);
      DataTestUtil.createKey(bucket, keyName, data);
    }

    // Delete created keys
    for (String key : keys) {
      bucket.deleteKey(key);
    }

    // Verify that KeyDeletingService picks up deleted keys and purges them
    // from DB.
    KeyManager keyManager = om.getKeyManager();
    KeyDeletingService keyDeletingService =
        (KeyDeletingService) keyManager.getDeletingService();

    GenericTestUtils.waitFor(
        () -> keyDeletingService.getDeletedKeyCount().get() >= NUM_KEYS,
        1000, 10000);

    assertThat(keyDeletingService.getRunCount().get()).isGreaterThan(1);

    GenericTestUtils.waitFor(
        () -> {
          try {
            return keyManager.getPendingDeletionKeys((kv) -> true, Integer.MAX_VALUE)
                .getPurgedKeys().isEmpty();
          } catch (IOException e) {
            return false;
          }
        }, 1000, 10000);
  }

  /**
   * A server side copy shares the source key's blocks, so deleting one of the
   * two keys must purge only its metadata and leave the data readable through
   * the other. The blocks are only released once the last sharer is gone.
   */
  @Test
  public void testCopiedKeyKeepsBlocksUntilLastSharerIsDeleted() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE).build());
    OzoneBucket bucket = volume.getBucket(bucketName);

    String sourceKey = "source";
    String copiedKey = "copy";
    byte[] data = ContainerTestHelper.getFixedLengthString(
        UUID.randomUUID().toString(), KEY_SIZE).getBytes(UTF_8);
    DataTestUtil.createKey(bucket, sourceKey, data);

    bucket.copyKey(sourceKey, copiedKey, Collections.emptyMap());

    // The copy reads back the source's bytes without any data having moved.
    assertArrayEquals(data, readKey(bucket, copiedKey));

    OMMetadataManager metadataManager = om.getMetadataManager();
    Table<String, OmKeyInfo> keyTable =
        metadataManager.getKeyTable(BucketLayout.OBJECT_STORE);
    OmKeyInfo sourceInfo = keyTable.get(
        metadataManager.getOzoneKey(volumeName, bucketName, sourceKey));
    OmKeyInfo copiedInfo = keyTable.get(
        metadataManager.getOzoneKey(volumeName, bucketName, copiedKey));

    assertEquals(blockIds(sourceInfo), blockIds(copiedInfo));
    assertNotEquals(sourceInfo.getObjectID(), copiedInfo.getObjectID());
    assertEquals(sourceInfo.getObjectID(), sourceInfo.getSharedBlockGroupId());
    assertEquals(sourceInfo.getSharedBlockGroupId(), copiedInfo.getSharedBlockGroupId());
    assertEquals(2L, metadataManager.getSharedBlockGroupTable()
        .get(sourceInfo.getSharedBlockGroupId()));

    KeyManager keyManager = om.getKeyManager();
    long sharedBlockGroupId = sourceInfo.getSharedBlockGroupId();
    bucket.deleteKey(sourceKey);

    // Reclaiming the source drops the sharer count to one, which removes the
    // row: the copy is the sole owner again. Waiting on the count rather than
    // on an empty deletedTable, which is also empty before the key arrives.
    GenericTestUtils.waitFor(
        () -> {
          try {
            return metadataManager.getSharedBlockGroupTable().get(sharedBlockGroupId) == null;
          } catch (IOException e) {
            return false;
          }
        }, 500, 60000);

    // The blocks were withheld from SCM, so the copy still reads.
    assertArrayEquals(data, readKey(bucket, copiedKey));

    bucket.deleteKey(copiedKey);
    GenericTestUtils.waitFor(
        () -> {
          try {
            return keyManager.getPendingDeletionKeys((kv) -> true, Integer.MAX_VALUE)
                .getPurgedKeys().isEmpty();
          } catch (IOException e) {
            return false;
          }
        }, 500, 60000);
  }

  private static byte[] readKey(OzoneBucket bucket, String keyName) throws IOException {
    try (InputStream in = bucket.readKey(keyName)) {
      return org.apache.commons.io.IOUtils.toByteArray(in);
    }
  }

  private static List<String> blockIds(OmKeyInfo keyInfo) {
    return keyInfo.getKeyLocationVersions().stream()
        .flatMap(group -> group.getLocationLists().stream().flatMap(List::stream))
        .map(location -> location.getContainerID() + "/" + location.getLocalID())
        .collect(Collectors.toList());
  }
}
