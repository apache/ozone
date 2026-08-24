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

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_ITERATE_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple test that asserts that list status output is sorted.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestListStatus implements NonHATests.TestCase {
  private static final Logger LOG = LoggerFactory.getLogger(TestListStatus.class);

  private OzoneBucket fsoOzoneBucket;
  private OzoneClient client;

  @BeforeAll
  void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(cluster().getConf());
    // Set the number of keys to be processed during batch operated.
    conf.setInt(OZONE_FS_ITERATE_BATCH_SIZE, 5);

    client = OzoneClientFactory.getRpcClient(conf);

    // create a volume and a LEGACY bucket
    fsoOzoneBucket = TestDataUtil
        .createVolumeAndBucket(client, BucketLayout.FILE_SYSTEM_OPTIMIZED);

    buildNameSpaceTree(fsoOzoneBucket);
  }

  @AfterAll
  void cleanup() {
    IOUtils.closeQuietly(client);
  }

  @MethodSource("sortedListStatusParametersSource")
  @ParameterizedTest(name = "{index} {5}")
  public void testSortedListStatus(String keyPrefix, String startKey, int numEntries, int expectedNumKeys,
                                   boolean isPartialPrefix, String testName) throws Exception {
    checkKeyList(keyPrefix, startKey, numEntries, expectedNumKeys, isPartialPrefix);
  }

  private static Stream<Arguments> sortedListStatusParametersSource() {
    return Stream.of(
        arguments("", "", 1000, 10, false, "Test if output is sorted"),
        arguments("", "", 2, 2, false, "Number of keys returns is expected"),
        arguments("a1", "", 100, 3, false, "Check if the full prefix works"),
        arguments("a1", "", 2, 2, false, "Check if full prefix with numEntries work"),
        arguments("a1", "a1/a12", 100, 2, false, "Check if existing start key >>>"),
        arguments("", "a7", 100, 6, false, "Check with a non-existing start key"),
        arguments("b", "", 100, 4, true, "Check if half-prefix works"),
        arguments("b", "b5", 100, 2, true, "Check half prefix with non-existing start key"),
        arguments("b", "c", 100, 0, true, "Check half prefix with non-existing parent in a start key"),
        arguments("b", "b/g5", 100, 4, true, "Check half prefix with non-existing parent in a start key"),
        arguments("b", "c/g5", 100, 0, true, "Check half prefix with non-existing parent in a start key"),
        arguments("a1/a111", "a1/a111/a100", 100, 0, true, "Check prefix with a non-existing prefix key\n" +
            " and non-existing parent in a start key"),
        arguments("a1/a111", null, 100, 0, true, "Check start key is null")
    );
  }

  private static void createFile(OzoneBucket bucket, String keyName)
      throws IOException {
    try (OzoneOutputStream oos = bucket.createFile(keyName, 0,
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE),
        true, false)) {
      oos.flush();
    }
  }

  private static void buildNameSpaceTree(OzoneBucket ozoneBucket)
      throws Exception {
    /*
    FileSystem Namespace:
    "a1"      Dir
    "a1/a11"  Dir
    "a1/a12"  File
    "a1/a13"  File
    "a2"      File
    "a3"      Dir
    "a3/a31"  Dir
    "a3/a32"  File
    "a8"      File
    "a9"      Dir
    "a10"     File

    "b1"      File
    "b2"      File
    "b7"      File
    "b8"      File
     */
    ozoneBucket.createDirectory("/a1");
    createFile(ozoneBucket, "/a2");
    ozoneBucket.createDirectory("/a3");
    createFile(ozoneBucket, "/a8");
    ozoneBucket.createDirectory("/a9");
    createFile(ozoneBucket, "/a10");

    ozoneBucket.createDirectory("/a1/a11");
    createFile(ozoneBucket, "/a1/a12");
    createFile(ozoneBucket, "/a1/a13");

    ozoneBucket.createDirectory("/a3/a31");
    createFile(ozoneBucket, "/a3/a32");

    createFile(ozoneBucket, "/b1");
    createFile(ozoneBucket, "/b2");
    createFile(ozoneBucket, "/b7");
    createFile(ozoneBucket, "/b8");
  }

  private void checkKeyList(String keyPrefix, String startKey, long numEntries, int expectedNumKeys,
                            boolean isPartialPrefix) throws Exception {

    List<OzoneFileStatus> statuses =
        fsoOzoneBucket.listStatus(keyPrefix, false, startKey,
            numEntries, isPartialPrefix);
    assertEquals(expectedNumKeys, statuses.size());

    LOG.info("BEGIN:::keyPrefix---> {} :::---> {}", keyPrefix, startKey);

    for (int i = 0; i < statuses.size() - 1; i++) {
      OzoneFileStatus stCurr = statuses.get(i);
      OzoneFileStatus stNext = statuses.get(i + 1);

      LOG.info("status: {}", stCurr);
      assertThat(stCurr.getPath().compareTo(stNext.getPath())).isLessThan(0);
    }

    if (!statuses.isEmpty()) {
      OzoneFileStatus stNext = statuses.get(statuses.size() - 1);
      LOG.info("status: {}", stNext);
    }

    LOG.info("END:::keyPrefix---> {}:::---> {}", keyPrefix, startKey);
  }
}
