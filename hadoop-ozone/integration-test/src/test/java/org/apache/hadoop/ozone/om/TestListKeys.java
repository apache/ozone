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

import static com.google.common.collect.Lists.newLinkedList;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.ozone.test.OzoneTestBase.uniqueObjectName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.params.provider.Arguments.of;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.ozone.test.ClusterForTests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test covers listKeys(keyPrefix, startKey, shallow) combinations
 * in a legacy/OBS bucket layout type.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestListKeys implements ClusterForTests.TestCase {

  private OzoneBucket legacyOzoneBucket;
  private OzoneBucket obsOzoneBucket;
  private OzoneClient client;
  private boolean originalFileSystemPathEnabled;
  private long originalMaxListSize;

  @BeforeAll
  void init() throws Exception {
    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    originalFileSystemPathEnabled = omConfig.isFileSystemPathEnabled();
    omConfig.setFileSystemPathEnabled(true);
    originalMaxListSize = omConfig.getMaxListSize();
    omConfig.setMaxListSize(2);

    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 3);
    conf.setInt(OZONE_CLIENT_LIST_CACHE_SIZE, 3);

    client = OzoneClientFactory.getRpcClient(conf);

    // create a volume and a LEGACY bucket
    legacyOzoneBucket = DataTestUtil
        .createVolumeAndBucket(client, BucketLayout.LEGACY);

    // create a volume and a OBJECT_STORE bucket
    obsOzoneBucket = DataTestUtil
        .createVolumeAndBucket(client, BucketLayout.OBJECT_STORE);

    initFSNameSpace();
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    omConfig.setFileSystemPathEnabled(originalFileSystemPathEnabled);
    omConfig.setMaxListSize(originalMaxListSize);
  }

  private void initFSNameSpace() throws Exception {
    buildNameSpaceTree(legacyOzoneBucket);
    buildNameSpaceTree(obsOzoneBucket);
  }

  /**
   * Verify listKeys at different levels.
   *
   *                  buck-1
   *                    |
   *                    a1
   *                    |
   *      --------------------------------------------------------
   *     |              |                       |                |
   *     b1             b2                      b3               b4
   *    -------         ---------              -----------
   *   |      |        |    |   |             |    |     |
   *  c1     c2       d1   d2  d3             e1   e2   e3
   *  |      |        |    |   |              |    |    |
   * c1.tx  c2.tx  d11.tx  | d31.tx           |    |    e31.tx
   *                      ---------           |   e21.tx
   *                     |        |           |
   *                    d21.tx   d22.tx      e11.tx
   *
   * Above is the key namespace tree structure.
   */
  private static void buildNameSpaceTree(OzoneBucket ozoneBucket)
      throws Exception {
    LinkedList<String> keys = new LinkedList<>();
    keys.add("a1/b1/c1111.tx");
    keys.add("a1/b1/c1222.tx");
    keys.add("a1/b1/c1333.tx");
    keys.add("a1/b1/c1444.tx");
    keys.add("a1/b1/c1555.tx");
    keys.add("a1/b1/c1/c1.tx");
    keys.add("a1/b1/c12/c2.tx");
    keys.add("a1/b1/c12/c3.tx");

    keys.add("a1/b2/d1/d11.tx");
    keys.add("a1/b2/d2/d21.tx");
    keys.add("a1/b2/d2/d22.tx");
    keys.add("a1/b2/d3/d31.tx");

    keys.add("a1/b3/e1/e11.tx");
    keys.add("a1/b3/e2/e21.tx");
    keys.add("a1/b3/e3/e31.tx");

    createAndAssertKeys(ozoneBucket, keys);

    ozoneBucket.createDirectory("a1/b4/");
  }

  private static Stream<Arguments> shallowListDataWithTrailingSlash() {
    return Stream.of(

        // Case-1: StartKey is less than prefixKey, return all immediate node.
        of("a1/b2/", "a1", newLinkedList(Arrays.asList(
            "a1/b2/",
            "a1/b2/d1/",
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-2: StartKey is empty, return all immediate node.
        of("a1/b2/", "", newLinkedList(Arrays.asList(
            "a1/b2/",
            "a1/b2/d1/",
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-3: StartKey is same as prefixKey, return all immediate nodes.
        of("a1/b2/", "a1/b2", newLinkedList(Arrays.asList(
            "a1/b2/",
            "a1/b2/d1/",
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-4: StartKey is greater than prefixKey
        of("a1/b2/", "a1/b2/d2/d21.tx", newLinkedList(Arrays.asList(
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-5: StartKey reaches last element, return emptyList
        of("a1/b2/", "a1/b2/d3/d31.tx", newLinkedList(
            Collections.emptyList()
        )),

        // Case-6: Mix result
        of("a1/b1/", "a1/b1/c12", newLinkedList(Arrays.asList(
            "a1/b1/c12/",
            "a1/b1/c1222.tx",
            "a1/b1/c1333.tx",
            "a1/b1/c1444.tx",
            "a1/b1/c1555.tx"
        ))),

        // Case-7: StartKey is empty, return key that is same as keyPrefix.
        of("a1/b4/", "", newLinkedList(Arrays.asList(
            "a1/b4/"
        )))
    );
  }

  private static Stream<Arguments> shallowListObsDataWithTrailingSlash() {
    return Stream.of(

        // Case-1: StartKey is less than prefixKey, return all immediate node.
        of("a1/b2/", "a1", newLinkedList(Arrays.asList(
            "a1/b2/d1/",
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-2: StartKey is empty, return all immediate node.
        of("a1/b2/", "", newLinkedList(Arrays.asList(
            "a1/b2/d1/",
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-3: StartKey is same as prefixKey, return all immediate nodes.
        of("a1/b2/", "a1/b2", newLinkedList(Arrays.asList(
            "a1/b2/d1/",
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-4: StartKey is greater than prefixKey
        of("a1/b2/", "a1/b2/d2/d21.tx", newLinkedList(Arrays.asList(
            "a1/b2/d2/",
            "a1/b2/d3/"
        ))),

        // Case-5: StartKey reaches last element, return emptyList
        of("a1/b2/", "a1/b2/d3/d31.tx", newLinkedList(
            Collections.emptyList()
        )),

        // Case-6: Mix result
        of("a1/b1/", "a1/b1/c12", newLinkedList(Arrays.asList(
            "a1/b1/c12/",
            "a1/b1/c1222.tx",
            "a1/b1/c1333.tx",
            "a1/b1/c1444.tx",
            "a1/b1/c1555.tx"
        ))),

        // Case-7: StartKey is empty, return key that is same as keyPrefix.
        of("a1/b4/", "", newLinkedList(Arrays.asList(
            "a1/b4/"
        )))
    );
  }

  private static Stream<Arguments> shallowListDataWithoutTrailingSlash() {
    return Stream.of(

        // Case-1: StartKey is less than prefixKey, return all immediate node.
        of("a1/b2", "a1", newLinkedList(Arrays.asList(
            "a1/b2/"
        ))),

        // Case-1a: StartKey is an existing key on a branch before prefixKey.
        of("a1/b2", "a1/b1/c1/c1.tx", newLinkedList(Arrays.asList(
            "a1/b2/"
        ))),

        // Case-2: StartKey is empty, return all immediate node.
        of("a1/b2", "", newLinkedList(Arrays.asList(
            "a1/b2/"
        ))),

        // Case-3: StartKey is same as prefixKey.
        of("a1/b2", "a1/b2", newLinkedList(Arrays.asList(
            "a1/b2/"
        ))),

        // Case-4: StartKey is greater than prefixKey, return immediate
        // nodes which after startKey.
        of("a1/b2", "a1/b2/d2/d21.tx", newLinkedList(Arrays.asList(
            "a1/b2/"
        ))),

        // Case-5: StartKey reaches last element, return emptyList
        of("a1/b2", "a1/b2/d3/d31.tx", newLinkedList(
            Collections.emptyList()
        )),

        // Case-6: StartKey is invalid (less than last element)
        of("a1/b1/c1", "a1/b1/c1/c0invalid", newLinkedList(Arrays.asList(
            "a1/b1/c1/",
            "a1/b1/c1111.tx",
            "a1/b1/c12/",
            "a1/b1/c1222.tx",
            "a1/b1/c1333.tx",
            "a1/b1/c1444.tx",
            "a1/b1/c1555.tx"
        ))),

        // Case-7: StartKey reaches last element
        of("a1/b1/c1", "a1/b1/c1/c2.tx", newLinkedList(Arrays.asList(
            "a1/b1/c1111.tx",
            "a1/b1/c12/",
            "a1/b1/c1222.tx",
            "a1/b1/c1333.tx",
            "a1/b1/c1444.tx",
            "a1/b1/c1555.tx"
        ))),

        // Case-8: StartKey is invalid (greater than last element)
        of("a1/b1/c1", "a1/b1/c1/c2invalid", newLinkedList(Arrays.asList(
            "a1/b1/c1111.tx",
            "a1/b1/c12/",
            "a1/b1/c1222.tx",
            "a1/b1/c1333.tx",
            "a1/b1/c1444.tx",
            "a1/b1/c1555.tx"
        ))),

        // Case-9:
        of("a1/b1/c12", "", newLinkedList(Arrays.asList(
            "a1/b1/c12/",
            "a1/b1/c1222.tx"
        ))),

        // Case-10:
        of("a1/b4", "", newLinkedList(Arrays.asList(
            "a1/b4/"
        )))

    );
  }

  @ParameterizedTest
  @MethodSource("shallowListDataWithTrailingSlash")
  public void testShallowListKeysWithPrefixTrailingSlash(String keyPrefix,
      String startKey, List<String> expectedKeys) throws Exception {
    checkKeyShallowList(keyPrefix, startKey, expectedKeys, legacyOzoneBucket);
  }

  @ParameterizedTest
  @MethodSource("shallowListObsDataWithTrailingSlash")
  public void testShallowListObsKeysWithPrefixTrailingSlash(String keyPrefix,
      String startKey, List<String> expectedKeys) throws Exception {
    checkKeyShallowList(keyPrefix, startKey, expectedKeys, obsOzoneBucket);
  }

  @ParameterizedTest
  @MethodSource("shallowListDataWithoutTrailingSlash")
  public void testShallowListKeysWithoutPrefixTrailingSlash(String keyPrefix,
      String startKey, List<String> expectedKeys) throws Exception {
    checkKeyShallowList(keyPrefix, startKey, expectedKeys, legacyOzoneBucket);
    checkKeyShallowList(keyPrefix, startKey, expectedKeys, obsOzoneBucket);
  }

  @Test
  public void testListKeysPaginationWithCacheAndTombstones() throws Exception {
    final String volumeName = uniqueObjectName("vol-pagination");
    final String bucketName = uniqueObjectName("bucket-pagination");
    final BucketLayout layout = BucketLayout.OBJECT_STORE;
    final OMMetadataManager metadataManager = metadataManager();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, metadataManager, layout);

    final RatisReplicationConfig replication = RatisReplicationConfig.getInstance(ONE);
    // Flush odd p1 keys, then tombstoned p2 keys, then cache-only even p1 keys.
    for (int keyIndex : new int[] {1, 3, 5}) {
      OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, "p1/k" + keyIndex, 0L, replication,
          metadataManager);
    }
    for (int i = 0; i < 50; i++) {
      OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, String.format("p2/k%03d", i), 0L, replication,
          metadataManager);
    }
    metadataManager.getStore().flushDB();

    final Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(layout);
    for (int i = 0; i < 50; i++) {
      keyTable.delete(metadataManager.getOzoneKey(volumeName, bucketName, String.format("p2/k%03d", i)));
    }

    for (int keyIndex : new int[] {2, 4}) {
      OMRequestTestUtils.addKeyToTableCache(volumeName, bucketName, "p1/k" + keyIndex, replication, metadataManager);
    }

    assertEquals(Arrays.asList("p1/k1", "p1/k2", "p1/k3", "p1/k4", "p1/k5"),
        listAllKeysPaginated(metadataManager, volumeName, bucketName, "p1/", 2));
  }

  @Test
  public void testListKeysWithStartKeyPastPrefixUpperBound() throws Exception {
    final String volumeName = uniqueObjectName("vol-oob");
    final String bucketName = uniqueObjectName("bucket-oob");
    final OMMetadataManager metadataManager = metadataManager();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName, metadataManager, BucketLayout.OBJECT_STORE);

    final RatisReplicationConfig replication = RatisReplicationConfig.getInstance(ONE);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, "a/1", 0L, replication, metadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, "a/2", 0L, replication, metadataManager);
    OMRequestTestUtils.addKeyToTable(false, volumeName, bucketName, "b/1", 0L, replication, metadataManager);
    metadataManager.getStore().flushDB();

    ListKeysResult result = metadataManager.listKeys(volumeName, bucketName, "b/1", "a/", 10);

    assertEquals(Collections.emptyList(), listKeyNames(result),
        "Expected empty result when startKey is past prefix upper bound");
    assertFalse(result.isTruncated());
  }

  @Test
  public void testListKeysPaginationBucketBoundaryIsolation() throws Exception {
    final String volumeName = uniqueObjectName("vol-boundary");
    final String bucket1 = "bucket1";
    final String bucket10 = "bucket10";
    final BucketLayout layout = BucketLayout.OBJECT_STORE;
    final OMMetadataManager metadataManager = metadataManager();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket1, metadataManager, layout);
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket10, metadataManager, layout);

    final RatisReplicationConfig replication = RatisReplicationConfig.getInstance(ONE);
    for (int i = 1; i <= 5; i++) {
      OMRequestTestUtils.addKeyToTable(false, volumeName, bucket1, "k" + i, 0L, replication, metadataManager);
      OMRequestTestUtils.addKeyToTable(false, volumeName, bucket10, "other" + i, 0L, replication, metadataManager);
    }
    metadataManager.getStore().flushDB();

    final Table<String, OmKeyInfo> keyTable = metadataManager.getKeyTable(layout);
    for (int i = 1; i <= 5; i++) {
      keyTable.delete(metadataManager.getOzoneKey(volumeName, bucket10, "other" + i));
    }

    assertEquals(Arrays.asList("k1", "k2", "k3", "k4", "k5"),
        listAllKeysPaginated(metadataManager, volumeName, bucket1, "", 2));
  }

  private void checkKeyShallowList(String keyPrefix, String startKey,
      List<String> keys, OzoneBucket bucket)
      throws Exception {

    Iterator<? extends OzoneKey> ozoneKeyIterator =
        bucket.listKeys(keyPrefix, startKey, true);
    ReplicationConfig expectedReplication =
        Optional.ofNullable(bucket.getReplicationConfig())
            .orElse(cluster().getOzoneManager().getDefaultReplicationConfig());

    List <String> keyLists = new ArrayList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      assertEquals(expectedReplication, ozoneKey.getReplicationConfig());
      keyLists.add(ozoneKey.getName());
    }
    LinkedList outputKeysList = new LinkedList(keyLists);
    System.out.println("BEGIN:::keyPrefix---> " + keyPrefix + ":::---> " +
        startKey);
    for (String key : keys) {
      System.out.println(" " + key);
    }
    System.out.println("END:::keyPrefix---> " + keyPrefix + ":::---> " +
        startKey);
    assertEquals(keys, outputKeysList);
  }

  private static void createAndAssertKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {
    for (String key : keys) {
      byte[] input = DataTestUtil.createStringKey(ozoneBucket, key, 10);
      // Read the key with given key name.
      readkey(ozoneBucket, key, 10, input);
    }
  }

  private static void readkey(OzoneBucket ozoneBucket, String key, int length, byte[] input)
      throws Exception {
    byte[] read = new byte[length];
    try (InputStream in = ozoneBucket.readKey(key)) {
      IOUtils.readFully(in, read);
    }

    assertEquals(new String(input, StandardCharsets.UTF_8), new String(read, StandardCharsets.UTF_8));
  }

  private OMMetadataManager metadataManager() {
    return cluster().getOzoneManager().getMetadataManager();
  }

  private static List<String> listKeyNames(ListKeysResult result) {
    final List<String> names = new ArrayList<>();
    for (OmKeyInfo keyInfo : result.getKeys()) {
      names.add(keyInfo.getKeyName());
    }
    return names;
  }

  private static List<String> listAllKeysPaginated(OMMetadataManager metadataManager, String volumeName,
      String bucketName, String keyPrefix, int maxKeys) throws IOException {
    final List<String> collected = new ArrayList<>();
    String startKey = null;
    boolean truncated;
    do {
      final ListKeysResult result = metadataManager.listKeys(volumeName, bucketName, startKey, keyPrefix, maxKeys);
      final List<String> page = listKeyNames(result);
      collected.addAll(page);
      if (!page.isEmpty()) {
        startKey = page.get(page.size() - 1);
      }
      truncated = result.isTruncated();
    } while (truncated);
    return collected;
  }
}
