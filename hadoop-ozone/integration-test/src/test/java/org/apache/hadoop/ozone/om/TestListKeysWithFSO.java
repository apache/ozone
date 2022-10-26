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

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;

/**
 * Test covers listKeys(keyPrefix, startKey) combinations
 * in a FSO bucket layout type.
 */
public class TestListKeysWithFSO {

  private static MiniOzoneCluster cluster = null;
  private static OzoneConfiguration conf;
  private static String clusterId;
  private static String scmId;
  private static String omId;

  private static OzoneBucket legacyOzoneBucket;
  private static OzoneBucket fsoOzoneBucket;
  private static OzoneBucket legacyOzoneBucket2;
  private static OzoneBucket fsoOzoneBucket2;

  @Rule
  public Timeout timeout = new Timeout(1200000);

  /**
   * Create a MiniDFSCluster for testing.
   * <p>
   *
   * @throws IOException
   */
  @BeforeClass
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    conf.setBoolean(OMConfigKeys.OZONE_OM_ENABLE_FILESYSTEM_PATHS,
        true);
    clusterId = UUID.randomUUID().toString();
    scmId = UUID.randomUUID().toString();
    omId = UUID.randomUUID().toString();
    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 3);
    conf.setInt(OZONE_CLIENT_LIST_CACHE_SIZE, 3);
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

    // create a volume and a LEGACY bucket
    legacyOzoneBucket = TestDataUtil
        .createVolumeAndBucket(cluster, BucketLayout.LEGACY);
    String volumeName = legacyOzoneBucket.getVolumeName();

    OzoneClient client = cluster.getClient();
    OzoneVolume ozoneVolume = client.getObjectStore().getVolume(volumeName);

    // create buckets
    BucketArgs omBucketArgs;
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    omBucketArgs = builder.build();

    String fsoBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(fsoBucketName, omBucketArgs);
    fsoOzoneBucket = ozoneVolume.getBucket(fsoBucketName);

    fsoBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(fsoBucketName, omBucketArgs);
    fsoOzoneBucket2 = ozoneVolume.getBucket(fsoBucketName);

    builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.LEGACY);
    omBucketArgs = builder.build();
    String legacyBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(legacyBucketName, omBucketArgs);
    legacyOzoneBucket2 = ozoneVolume.getBucket(legacyBucketName);

    initFSNameSpace();
  }

  @AfterClass
  public static void teardownClass() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private static void initFSNameSpace() throws Exception {
    /*
    Keys Namespace:

    "/a1/b1/c1/c1.tx";
    "/a1/b1/c2/c2.tx";

    "/a1/b2/d1/d11.tx";
    "/a1/b2/d2/d21.tx";
    "/a1/b2/d2/d22.tx";
    "/a1/b2/d3/d31.tx";

    "/a1/b3/e1/e11.tx";
    "/a1/b3/e2/e21.tx";
    "/a1/b3/e3/e31.tx";
     */
    buildNameSpaceTree(legacyOzoneBucket);
    buildNameSpaceTree(fsoOzoneBucket);

    buildNameSpaceTree2(legacyOzoneBucket2);
    buildNameSpaceTree2(fsoOzoneBucket2);
  }

  @Test
  public void testListKeysWithValidStartKey() throws Exception {
    // case-1: StartKey LeafNode is lexographically behind than prefixKey.
    // So, will return EmptyList
    // a1/b2 < a1/b2Invalid
    List<String> expectedKeys =
        getExpectedKeyList("a1/b2", "a1", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1", expectedKeys, fsoOzoneBucket);

    // case-2: Same prefixKey and startKey, but with an ending slash
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2/", legacyOzoneBucket);
    /**
     * a1/b2/d1/
     * a1/b2/d1/d11.tx
     * a1/b2/d2/
     * a1/b2/d2/d21.tx
     * a1/b2/d2/d22.tx
     * a1/b2/d3/
     * a1/b2/d3/d31.tx
     */
    checkKeyList("a1/b2", "a1/b2/", expectedKeys, fsoOzoneBucket);

    // case-3: Same prefixKey and startKey, but without an ending slash.
    //  StartKey(dir) to be included in the finalList, if its a
    //  directory and not ended with trailing slash.
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2", legacyOzoneBucket);
    /**
     * a1/b2/
     * a1/b2/d1/
     * a1/b2/d1/d11.tx
     * a1/b2/d2/
     * a1/b2/d2/d21.tx
     * a1/b2/d2/d22.tx
     * a1/b2/d3/
     * a1/b2/d3/d31.tx
     */
    checkKeyList("a1/b2", "a1/b2", expectedKeys, fsoOzoneBucket);

    // case-4: StartKey is a file with an ending slash.
    //  StartKey(file) with or without an ending slash
    //  to be excluded in the finalList.
    expectedKeys =
        getExpectedKeyList("a1/b2/d2", "a1/b2/d2/d22.tx/", legacyOzoneBucket);
    checkKeyList("a1/b2/d2", "a1/b2/d2/d22.tx/", expectedKeys, fsoOzoneBucket);

    // case-5:
    // StartKey "a1/b2/d2/d22.tx" is a file and get all the keys lexographically
    // greater than "a1/b2/d2/d22.tx".
    expectedKeys =
        getExpectedKeyList("a1/b2", "a1/b2/d2/d22.tx", legacyOzoneBucket);
    /**
     1 = "a1/b2/d3/"
     2 = "a1/b2/d3/d31.tx"
     */
    checkKeyList("a1/b2", "a1/b2/d2/d22.tx", expectedKeys, fsoOzoneBucket);

    // case-6:
    // StartKey "a1/b2/d2" is a dir and get all the keys lexographically
    // greater than "a1/b2/d2".
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2/d2/", legacyOzoneBucket);
    /**
     1 = "a1/b2/d2/d21.tx"
     2 = "a1/b2/d2/d22.tx"
     3 = "a1/b2/d3/"
     4 = "a1/b2/d3/d31.tx"
     */
    checkKeyList("a1/b2", "a1/b2/d2/", expectedKeys, fsoOzoneBucket);

    // case-7: In below case, the startKey is a directory which is included
    // in the finalList. So, should we include startKey file in the finalList ?
    expectedKeys =
        getExpectedKeyList("a1", "a1/b2/d2/d21.tx", legacyOzoneBucket);
    /**
     1 = "a1/b2/d2/d22.tx"
     2 = "a1/b2/d3/"
     3 = "a1/b2/d3/d31.tx"
     4 = "a1/b3/"
     5 = "a1/b3/e1/"
     6 = "a1/b3/e1/e11.tx"
     7 = "a1/b3/e2/"
     8 = "a1/b3/e2/e21.tx"
     9 = "a1/b3/e3/"
     10 = "a1/b3/e3/e31.tx"
     */
    checkKeyList("a1", "a1/b2/d2/d21.tx", expectedKeys, fsoOzoneBucket);

    // case-8: StartKey(dir) to be included in the finalList, if its a
    //  directory and not ended with trailing slash.
    expectedKeys = getExpectedKeyList("a1", "a1/b2/d2/", legacyOzoneBucket);
    /**
     1 = "a1/b2/d2/d21.tx"
     2 = "a1/b2/d2/d22.tx"
     3 = "a1/b2/d3/"
     4 = "a1/b2/d3/d31.tx"
     5 = "a1/b3/"
     .....
     10 = "a1/b3/e3/"
     11 = "a1/b3/e3/e31.tx"
     */
    checkKeyList("a1", "a1/b2/d2/", expectedKeys, fsoOzoneBucket);

    // case-9: Reached Last Element, return EmptyList
    expectedKeys =
        getExpectedKeyList("a1", "a1/b3/e3/e31.tx", legacyOzoneBucket);
    checkKeyList("a1", "a1/b3/e3/e31.tx", expectedKeys, fsoOzoneBucket);
  }

  @Test
  public void testListKeysWithNonExistentStartKey() throws Exception {
    // case-1: StartKey LeafNode is lexographically ahead than prefixKey.
    // So, will return EmptyList
    // a1/b2 < a1/b2Invalid
    List<String> expectedKeys =
        getExpectedKeyList("a1", "a1/a111/b111", legacyOzoneBucket);
    checkKeyList("a1", "a1/a111/b111", expectedKeys, fsoOzoneBucket);
    // a1/b2 < a1/b20
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b20", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1/b20", expectedKeys, fsoOzoneBucket);

    // case-2: StartKey LeafNode's parent is lexographically ahead than
    // prefixKey. So, will return EmptyList
    // a1/b1 < a1/b2
    expectedKeys = getExpectedKeyList("a1/b1", "a1/b2/d0", legacyOzoneBucket);
    checkKeyList("a1/b1", "a1/b2/d0", expectedKeys, fsoOzoneBucket);

    // case-3:
    // StartKey LeafNode's parent is not matching with than prefixKey's parent.
    // So, will return EmptyList
    expectedKeys =
        getExpectedKeyList("a1/b2", "a0/b123Invalid", legacyOzoneBucket);
    checkKeyList("a1/b2", "a0/b123Invalid", expectedKeys, fsoOzoneBucket);

    // case-4: StartKey LeafNode is lexographically behind prefixKey.
    // So will return all the sub-paths of prefixKey
    // startKey=a1/b123Invalid is lexographically before prefixKey=a1/b2
    expectedKeys =
        getExpectedKeyList("a1/b2", "a1/b123Invalid", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1/b123Invalid", expectedKeys, fsoOzoneBucket);

    // case-5: StartKey LeafNode is a sub-directory of prefixKey.
    // So will fetch and return all the sub-paths after d0.
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2/d0", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1/b2/d0", expectedKeys, fsoOzoneBucket);

    // case-6: StartKey LeafNode is a sub-file of prefixKey.
    // So will fetch and return all the sub-paths after d111.txt.
    expectedKeys =
        getExpectedKeyList("a1/b2", "a1/b2/d111.txt", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1/b2/d111.txt", expectedKeys, fsoOzoneBucket);

    // case-7: StartKey LeafNode is a sub-file of prefixKey.
    // So will fetch and return all the sub-paths after "d3/d4111.tx".
    // Since there is no sub-paths after "d3" it will return emptyList
    expectedKeys =
        getExpectedKeyList("a1/b2", "a1/b2/d3/d4111.tx", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1/b2/d3/d4111.tx", expectedKeys, fsoOzoneBucket);

    // case-8: StartKey LeafNode is a sub-dir of prefixKey.
    // So will fetch and return all the sub-paths after "d311111".
    expectedKeys = getExpectedKeyList("a1", "a1/b2/d311111", legacyOzoneBucket);
    checkKeyList("a1", "a1/b2/d311111", expectedKeys, fsoOzoneBucket);

    // case-9:
    // Immediate child of prefixKey is lexographically greater than "a1/b1".
    // So will fetch and return all the sub-paths after "b11111",
    // which is "a1/b2"
    expectedKeys =
        getExpectedKeyList("a1", "a1/b11111/d311111", legacyOzoneBucket);
    checkKeyList("a1", "a1/b11111/d311111", expectedKeys, fsoOzoneBucket);

    // case-10:
    // StartKey "a1/b2/d2" is valid and get all the keys lexographically
    // greater than "a1/b2/d2/d11111".
    expectedKeys =
        getExpectedKeyList("a1", "a1/b2/d2/d21111", legacyOzoneBucket);
    checkKeyList("a1", "a1/b2/d2/d21111", expectedKeys, fsoOzoneBucket);

    // case-11: StartKey is a sub-path of prefixKey.
    // So will fetch and return all the sub-paths after "e311111".
    // Return EmptyList as we reached the end of the tree
    expectedKeys =
        getExpectedKeyList("a1", "a1/b3/e3/e311111.tx", legacyOzoneBucket);
    checkKeyList("a1", "a1/b3/e3/e311111.tx", expectedKeys, fsoOzoneBucket);

    // case-12: StartKey is a sub-path of prefixKey.
    // So will fetch and return all the sub-paths after "e4444".
    // Return EmptyList as we reached the end of the tree
    expectedKeys =
        getExpectedKeyList("a1/b2", "a1/b3/e4444", legacyOzoneBucket);
    checkKeyList("a1/b2", "a1/b3/e4444", expectedKeys, fsoOzoneBucket);

    // case-13:
    // StartKey is a sub-path of prefixKey and startKey with a trailing slash.
    // So will fetch and return all the sub-paths after "e".
    expectedKeys = getExpectedKeyList("a1/b", "a1/b3/e/", legacyOzoneBucket);
    checkKeyList("a1/b3", "a1/b3/e/", expectedKeys, fsoOzoneBucket);

    // case-14: PrefixKey is empty and search should consider startKey.
    // Fetch all the keys after, a1/b2/d
    expectedKeys = getExpectedKeyList("", "a1/b2/d", legacyOzoneBucket);
    checkKeyList("", "a1/b2/d", expectedKeys, fsoOzoneBucket);

    // case-15: PrefixKey is empty and search should consider startKey.
    expectedKeys = getExpectedKeyList("", "a1/b2/d2/d21111", legacyOzoneBucket);
    checkKeyList("", "a1/b2/d2/d21111", expectedKeys, fsoOzoneBucket);

    // case-16: PrefixKey is empty and search should consider startKey.
    expectedKeys = getExpectedKeyList("", "a0/b2/d2/d21111", legacyOzoneBucket);
    checkKeyList("", "a0/b2/d2/d21111", expectedKeys, fsoOzoneBucket);

    // case-17: Partial prefixKey and seek till prefixKey.
    expectedKeys = getExpectedKeyList("a1/b", "a1/b1/e/", legacyOzoneBucket);
    checkKeyList("a1/b", "a1/b1/e/", expectedKeys, fsoOzoneBucket);

    // case-18: Partial prefixKey and seek till prefixKey.
    expectedKeys =
        getExpectedKeyList("a1/b1/c", "a1/b1/01/f/g/h", legacyOzoneBucket);
    checkKeyList("a1/b1/c", "a1/b1/01/f/g/h", expectedKeys, fsoOzoneBucket);

    // case-19: Partial prefixKey and seek till prefixKey.
    expectedKeys =
        getExpectedKeyList("a1/b1", "a1/01/e/f/g/h", legacyOzoneBucket);
    checkKeyList("a1/b1", "a1/01/e/f/g/h", expectedKeys, fsoOzoneBucket);

    // case-20: Partial prefixKey and seek till prefixKey.
    expectedKeys = getExpectedKeyList("a", "a1/b1/e/", legacyOzoneBucket);
    checkKeyList("a", "a1/b1/e/", expectedKeys, fsoOzoneBucket);
  }

  @Test
  public void testListKeysWithMixOfDirsAndFiles() throws Exception {
    // case-1: StartKey LeafNode is lexographically ahead than prefixKey.
    // So, will return EmptyList
    // a1/b2 < a1/b2Invalid
    List<String> expectedKeys = getExpectedKeyList("", "", legacyOzoneBucket2);
    checkKeyList("", "", expectedKeys, fsoOzoneBucket2);

    expectedKeys = getExpectedKeyList("", "a", legacyOzoneBucket2);
    checkKeyList("", "a", expectedKeys, fsoOzoneBucket2);

    expectedKeys = getExpectedKeyList("a", "a", legacyOzoneBucket2);
    checkKeyList("a", "a", expectedKeys, fsoOzoneBucket2);

    expectedKeys = getExpectedKeyList("a", "a1", legacyOzoneBucket2);
    checkKeyList("a", "a1", expectedKeys, fsoOzoneBucket2);
  }

  /**
   * Verify listKeys at different levels.
   *
   *                  buck-1
   *                    |
   *                    a1
   *                    |
   *      -----------------------------------
   *     |              |                       |
   *     b1             b2                      b3
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
    keys.add("/a1/b1/c1111.tx");
    keys.add("/a1/b1/c1222.tx");
    keys.add("/a1/b1/c1333.tx");
    keys.add("/a1/b1/c1444.tx");
    keys.add("/a1/b1/c1555.tx");
    keys.add("/a1/b1/c1/c1.tx");
    keys.add("/a1/b1/c12/c2.tx");
    keys.add("/a1/b1/c12/c3.tx");

    keys.add("/a1/b2/d1/d11.tx");
    keys.add("/a1/b2/d2/d21.tx");
    keys.add("/a1/b2/d2/d22.tx");
    keys.add("/a1/b2/d3/d31.tx");

    keys.add("/a1/b3/e1/e11.tx");
    keys.add("/a1/b3/e2/e21.tx");
    keys.add("/a1/b3/e3/e31.tx");

    createKeys(ozoneBucket, keys);
  }

  private static void buildNameSpaceTree2(OzoneBucket ozoneBucket)
      throws Exception {
    LinkedList<String> keys = new LinkedList<>();
    keys.add("/a1/b1/c1/c1.tx");
    keys.add("/a110.tx");
    keys.add("/a111.tx");
    keys.add("/a112.tx");
    keys.add("/a113.tx");
    keys.add("/a114.tx");
    keys.add("/a115.tx");
    keys.add("/a2/b1/c1/c1.tx");
    keys.add("/a220.tx");
    keys.add("/a221.tx");
    keys.add("/a222.tx");
    keys.add("/a223.tx");
    keys.add("/a224.tx");
    keys.add("/a225.tx");
    keys.add("/a3/b1/c1/c1.tx");

    keys.add("/x/y/z/z1.tx");

    keys.add("/dir1/dir2/dir3/d11.tx");

    createKeys(ozoneBucket, keys);
  }


  private static List<String> getExpectedKeyList(String keyPrefix,
      String startKey, OzoneBucket legacyBucket) throws Exception {
    Iterator<? extends OzoneKey> ozoneKeyIterator =
        legacyBucket.listKeys(keyPrefix, startKey);

    List<String> keys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      keys.add(ozoneKey.getName());
    }
    return keys;
  }

  private void checkKeyList(String keyPrefix, String startKey,
      List<String> keys, OzoneBucket fsoBucket) throws Exception {

    Iterator<? extends OzoneKey> ozoneKeyIterator =
        fsoBucket.listKeys(keyPrefix, startKey);

    List <String> keyLists = new ArrayList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
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
    Assert.assertEquals(keys, outputKeysList);
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

    Assert.assertEquals(new String(input, StandardCharsets.UTF_8),
        new String(read, StandardCharsets.UTF_8));
  }
}
