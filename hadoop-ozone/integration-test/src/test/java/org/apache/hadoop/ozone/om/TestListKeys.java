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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.Lists.newLinkedList;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_SERVER_LIST_MAX_SIZE;
import static org.junit.jupiter.params.provider.Arguments.of;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test covers listKeys(keyPrefix, startKey, shallow) combinations
 * in a legacy/OBS bucket layout type.
 */
@Timeout(1200)
public class TestListKeys {

  private static MiniOzoneCluster cluster = null;

  private static OzoneConfiguration conf;

  private static OzoneBucket legacyOzoneBucket;

  private static OzoneBucket obsOzoneBucket;
  private static OzoneClient client;

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS, true);
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 3);
    conf.setInt(OZONE_CLIENT_LIST_CACHE_SIZE, 3);
    conf.setInt(OZONE_OM_SERVER_LIST_MAX_SIZE, 2);
    cluster = MiniOzoneCluster.newBuilder(conf).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    // create a volume and a LEGACY bucket
    legacyOzoneBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.LEGACY);

    // create a volume and a OBJECT_STORE bucket
    obsOzoneBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.OBJECT_STORE);

    initFSNameSpace();
  }

  @AfterAll
  public static void teardownClass() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void initFSNameSpace() throws Exception {
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

    createKeys(ozoneBucket, keys);

    ozoneBucket.createDirectory("a1/b4/");
  }

  private static Stream<Arguments> shallowListDataWithTrailingSlash() {
    return Stream.of(

        // Case-1: StartKey is less than prefixKey, return emptyList.
        of("a1/b2/", "a1", newLinkedList(Collections.emptyList())),

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

        // Case-1: StartKey is less than prefixKey, return emptyList.
        of("a1/b2/", "a1", newLinkedList(Collections.emptyList())),

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

        // Case-1: StartKey is less than prefixKey, return emptyList.
        of("a1/b2", "a1", newLinkedList(Collections.emptyList())),

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

  private void checkKeyShallowList(String keyPrefix, String startKey,
      List<String> keys, OzoneBucket bucket)
      throws Exception {

    Iterator<? extends OzoneKey> ozoneKeyIterator =
        bucket.listKeys(keyPrefix, startKey, true);
    ReplicationConfig expectedReplication =
        Optional.ofNullable(bucket.getReplicationConfig())
            .orElse(cluster.getOzoneManager().getDefaultReplicationConfig());

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

  private static void createKeys(OzoneBucket ozoneBucket, List<String> keys)
      throws Exception {
    int length = 10;
    byte[] input = new byte[length];
    Arrays.fill(input, (byte) 96);
    for (String key : keys) {
      createKey(ozoneBucket, key, 10, input);
    }
  }

  private static void createKey(OzoneBucket ozoneBucket, String key, int length,
      byte[] input) throws Exception {

    OzoneOutputStream ozoneOutputStream =
        ozoneBucket.createKey(key, length);

    ozoneOutputStream.write(input);
    ozoneOutputStream.write(input, 0, 10);
    ozoneOutputStream.close();

    // Read the key with given key name.
    OzoneInputStream ozoneInputStream = ozoneBucket.readKey(key);
    byte[] read = new byte[length];
    ozoneInputStream.read(read, 0, length);
    ozoneInputStream.close();

    assertEquals(new String(input, StandardCharsets.UTF_8), new String(read, StandardCharsets.UTF_8));
  }
}
