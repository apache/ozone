/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.ozone;

import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.TestDataUtil;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.util.StringUtils;
import org.junit.Rule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/**
 * Test FileChecksum API.
 */
public class TestOzoneFileChecksum {

  @Rule
  public Timeout timeout = Timeout.seconds(100);

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster = null;
  private FileSystem fs;
  private RootedOzoneFileSystem ofs;
  private BasicRootedOzoneClientAdapterImpl adapter;
  private String rootPath;

  @BeforeEach
  public void setup() throws IOException,
      InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);
    ofs = (RootedOzoneFileSystem) fs;
    adapter = (BasicRootedOzoneClientAdapterImpl) ofs.getAdapter();
  }

  @AfterEach
  public void teardown() {
    if (cluster != null) {
      cluster.shutdown();
    }
    IOUtils.closeQuietly(fs);
  }

  /**
   *  Test EC checksum with Replicated checksum.
   */
  @ParameterizedTest
  @MethodSource("dataSizeMissingIndexes")
  public void testEcFileChecksum(double size, List<Integer> missingIndexes)
      throws IOException {

    // Size in multiples of MB
    int dataLen = (int) (1024 * 1024 * size);
    byte[] data = RandomStringUtils.randomAlphabetic(dataLen)
        .getBytes(UTF_8);

    BucketArgs omBucketArgs1 = BucketArgs.newBuilder()
        .setStorageType(StorageType.DISK)
        .setBucketLayout(BucketLayout.LEGACY)
        .build();

    String vol2 = UUID.randomUUID().toString();
    String legacyBucket = UUID.randomUUID().toString();
    TestDataUtil.createVolumeAndBucket(cluster, vol2,
        legacyBucket, BucketLayout.LEGACY, omBucketArgs1);

    try (OzoneFSOutputStream file = adapter.createFile(vol2 +
        "/" + legacyBucket + "/test", (short) 3, true, false)) {
      file.write(data);
    }

    Path parent1 = new Path("/" + vol2 + "/" + legacyBucket + "/");
    Path replicatedKey = new Path(parent1, "test");
    FileChecksum replicatedChecksum =  fs.getFileChecksum(replicatedKey);
    String replicatedChecksumString = StringUtils.byteToHexString(
        replicatedChecksum.getBytes(), 0, replicatedChecksum.getLength());

    BucketArgs omBucketArgs = BucketArgs.newBuilder()
        .setStorageType(StorageType.DISK)
        .setBucketLayout(BucketLayout.LEGACY)
        .setDefaultReplicationConfig(
            new DefaultReplicationConfig(ReplicationType.EC,
                new ECReplicationConfig("RS-3-2-1024k")))
        .build();

    String vol = UUID.randomUUID().toString();
    String ecBucket = UUID.randomUUID().toString();
    final OzoneBucket bucket101 = TestDataUtil
        .createVolumeAndBucket(cluster, vol, ecBucket, BucketLayout.LEGACY,
            omBucketArgs);

    Assertions.assertEquals(ReplicationType.EC.name(),
        bucket101.getReplicationConfig().getReplicationType().name());

    try (OzoneFSOutputStream file = adapter
        .createFile(vol + "/" + ecBucket + "/test", (short) 3, true, false)) {
      file.write(data);
    }

    // Fail DataNodes
    for (int index: missingIndexes) {
      cluster.shutdownHddsDatanode(index);
    }

    // Compute checksum after failed DNs
    Path parent = new Path("/" + vol + "/" + ecBucket + "/");
    Path ecKey = new Path(parent, "test");
    FileChecksum ecChecksum = fs.getFileChecksum(ecKey);
    String ecChecksumString = StringUtils.byteToHexString(
        ecChecksum.getBytes(), 0, ecChecksum.getLength());

    Assertions.assertEquals(replicatedChecksumString, ecChecksumString);
  }

  public static Stream<Arguments> dataSizeMissingIndexes() {
    return Stream.of(
        arguments(0.5, ImmutableList.of(0, 1)),
        arguments(1, ImmutableList.of(1, 2)),
        arguments(1.5, ImmutableList.of(2, 3)),
        arguments(2, ImmutableList.of(3, 4)),
        arguments(7, ImmutableList.of(0, 3)),
        arguments(8, ImmutableList.of(0, 4)));
  }
}
