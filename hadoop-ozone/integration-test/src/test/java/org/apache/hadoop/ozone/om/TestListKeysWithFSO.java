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
import java.util.TreeSet;
import java.util.UUID;

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
    cluster = MiniOzoneCluster.newBuilder(conf).setClusterId(clusterId)
        .setScmId(scmId).setOmId(omId).build();
    cluster.waitForClusterToBeReady();

    // create a volume and a LEGACY bucket
    legacyOzoneBucket = TestDataUtil
        .createVolumeAndBucket(cluster, BucketLayout.LEGACY);
    String volumeName = legacyOzoneBucket.getVolumeName();

    // create a volume and a FSO bucket
    BucketArgs omBucketArgs;
    BucketArgs.Builder builder = BucketArgs.newBuilder();
    builder.setStorageType(StorageType.DISK);
    builder.setBucketLayout(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    omBucketArgs = builder.build();
    OzoneClient client = cluster.getClient();
    OzoneVolume ozoneVolume = client.getObjectStore().getVolume(volumeName);

    String fsoBucketName = "bucket" + RandomStringUtils.randomNumeric(5);
    ozoneVolume.createBucket(fsoBucketName, omBucketArgs);
    fsoOzoneBucket = ozoneVolume.getBucket(fsoBucketName);

    // Set the number of keys to be processed during batch operate.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 2);

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
  }

  @Test
  public void testListKeysWithValidStartKey() throws Exception {
    // case-1: StartKey LeafNode is lexographically behind than prefixKey.
    // So, will return EmptyList
    // a1/b2 < a1/b2Invalid
    List<String> expectedKeys = getExpectedKeyList("a1/b2", "a1");
    checkKeyList("a1/b2", "a1", expectedKeys);

    // case-2: Same prefixKey and startKey. but startKey with ending slash
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2/");
    /**
     * a1/b2/d1/
     * a1/b2/d1/d11.tx
     * a1/b2/d2/
     * a1/b2/d2/d21.tx
     * a1/b2/d2/d22.tx
     * a1/b2/d3/
     * a1/b2/d3/d31.tx
     */
    checkKeyList("a1/b2", "a1/b2/", expectedKeys);

    // case-3:
    // StartKey "a1/b2/d2/d22.tx" is a file and get all the keys lexographically
    // greater than "a1/b2/d2/d22.tx".
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2/d2/d22.tx");
    /**
     1 = "a1/b2/d3/"
     2 = "a1/b2/d3/d31.tx"
     */
    checkKeyList("a1/b2", "a1/b2/d2/d22.tx", expectedKeys);

    // case-4:
    // StartKey "a1/b2/d2" is a dir and get all the keys lexographically
    // greater than "a1/b2/d2".
    expectedKeys = getExpectedKeyList("a1/b2", "a1/b2/d2/");
    /**
     1 = "a1/b2/d2/d21.tx"
     2 = "a1/b2/d2/d22.tx"
     3 = "a1/b2/d3/"
     4 = "a1/b2/d3/d31.tx"
     */
    checkKeyList("a1/b2", "a1/b2/d2/", expectedKeys);

    // case-5: In below case, the startKey is a directory which is included
    // in the finalList. So, should we include startKey file in the finalList ?
    expectedKeys = getExpectedKeyList("a1", "a1/b2/d2/d21.tx");
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
    checkKeyList("a1", "a1/b2/d2/d21.tx", expectedKeys);

    // case-76 Here need to discuss - whether the startKey(dir) to be included
    // in the finalList ?
    expectedKeys = getExpectedKeyList("a1", "a1/b2/d2/");
    /**
     1 = "a1/b2/d2/d21.tx"
     2 = "a1/b2/d2/d22.tx"
     3 = "a1/b2/d3/"
     4 = "a1/b2/d3/d31.tx"
     5 = "a1/b3/"
     6 = "a1/b3/e1/"
     7 = "a1/b3/e1/e11.tx"
     8 = "a1/b3/e2/"
     9 = "a1/b3/e2/e21.tx"
     10 = "a1/b3/e3/"
     11 = "a1/b3/e3/e31.tx"
     */
    checkKeyList("a1", "a1/b2/d2/", expectedKeys);

    // case-7: Reached Last Element, return EmptyList
    expectedKeys = getExpectedKeyList("a1", "a1/b3/e3/e31.tx");
    checkKeyList("a1", "a1/b3/e3/e31.tx", expectedKeys);
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

  private static List<String> getExpectedKeyList(String keyPrefix,
      String startKey) throws Exception {
    Iterator<? extends OzoneKey> ozoneKeyIterator =
        legacyOzoneBucket.listKeys(keyPrefix, startKey);

//    fsoOzoneBucket.listStatus(keyPrefix, false, startKey, 5);
    List<String> keys = new LinkedList<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      keys.add(ozoneKey.getName());
    }
    return keys;
  }

  private void checkKeyList(String keyPrefix, String startKey,
      List<String> keys) throws Exception {

    Iterator<? extends OzoneKey> ozoneKeyIterator =
        fsoOzoneBucket.listKeys(keyPrefix, startKey);

    TreeSet<String> outputKeys = new TreeSet<>();
    while (ozoneKeyIterator.hasNext()) {
      OzoneKey ozoneKey = ozoneKeyIterator.next();
      outputKeys.add(ozoneKey.getName());
    }
    LinkedList outputKeysList = new LinkedList(outputKeys);
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
