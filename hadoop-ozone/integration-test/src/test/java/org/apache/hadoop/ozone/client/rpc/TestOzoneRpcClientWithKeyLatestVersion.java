/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;


/**
 * Main purpose of this test is with OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION
 * set/unset key create/read works properly or not for buckets
 * with/without versioning.
 * TODO: can be merged with other test class
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestOzoneRpcClientWithKeyLatestVersion {

  private MiniOzoneCluster cluster;

  @BeforeAll
  void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OZONE_SCM_PIPELINE_OWNER_CONTAINER_COUNT, 1);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  void tearDown() {
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testWithGetLatestVersion(boolean getLatestVersionOnly) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration(cluster.getConf());
    conf.setBoolean(OZONE_CLIENT_KEY_LATEST_VERSION_LOCATION,
        getLatestVersionOnly);

    try (OzoneClient client = OzoneClientFactory.getRpcClient(conf)) {
      String volumeName = UUID.randomUUID().toString();
      ObjectStore objectStore = client.getObjectStore();
      objectStore.createVolume(volumeName);
      OzoneVolume volume = objectStore.getVolume(volumeName);

      for (boolean versioning : new boolean[] {false, true}) {
        String bucketName = UUID.randomUUID().toString();
        volume.createBucket(bucketName,
            BucketArgs.newBuilder()
                .setVersioning(versioning)
                .build());

        OzoneBucket bucket = volume.getBucket(bucketName);
        String keyName = UUID.randomUUID().toString();
        byte[] content = RandomUtils.nextBytes(128);
        int versions = RandomUtils.nextInt(2, 5);

        createAndOverwriteKey(bucket, keyName, versions, content);

        assertKeyContent(bucket, keyName, content);

        int expectedVersionCount =
            versioning && !getLatestVersionOnly ? versions : 1;
        assertListStatus(bucket, keyName, expectedVersionCount);
      }
    }
  }

  /** Repeatedly write {@code key}, first some random data, then
   * {@code content}. */
  private void createAndOverwriteKey(OzoneBucket bucket, String key,
      int versions, byte[] content) throws IOException {
    ReplicationConfig replication = RatisReplicationConfig.getInstance(THREE);
    for (int i = 1; i < versions; i++) {
      writeKey(bucket, key, RandomUtils.nextBytes(content.length), replication);
    }
    // overwrite it
    writeKey(bucket, key, content, replication);
  }

  private static void writeKey(OzoneBucket bucket, String key, byte[] content,
      ReplicationConfig replication) throws IOException {
    try (OutputStream out = bucket.createKey(key, content.length, replication,
        new HashMap<>())) {
      out.write(content);
    }
  }

  public static void assertKeyContent(OzoneBucket bucket, String key,
      byte[] expected) throws Exception {
    try (InputStream in = bucket.readKey(key)) {
      assertArrayEquals(expected, IOUtils.readFully(in, expected.length));
    }
  }

  private void assertListStatus(OzoneBucket bucket, String keyName,
      int expectedVersionCount) throws Exception {
    List<OzoneFileStatus> files = bucket.listStatus(keyName, false, "", 1);

    assertNotNull(files);
    assertEquals(1, files.size());

    List<?> versions = files.get(0).getKeyInfo().getKeyLocationVersions();
    assertEquals(expectedVersionCount, versions.size());
  }
}
