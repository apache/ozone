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

package org.apache.hadoop.fs.ozone;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_CHUNK_SIZE_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE;
import static org.apache.hadoop.ozone.TestDataUtil.createBucket;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.DoubleStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.conf.StorageUnit;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.util.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test FileChecksum API.
 */
public class TestOzoneFileChecksum {

  private static final boolean[] TOPOLOGY_AWARENESS = new boolean[] {
      true, false
  };

  private static final int[] DATA_SIZES_1 = DoubleStream.of(0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 9, 10)
      .mapToInt(mb -> (int) (1024 * 1024 * mb) + 510000)
      .toArray();

  private static final int[] DATA_SIZES_2 = DoubleStream.of(0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 6, 7, 8, 9, 10)
      .mapToInt(mb -> (int) (1024 * 1024 * mb) + 820000)
      .toArray();

  private int[] dataSizes = new int[DATA_SIZES_1.length + DATA_SIZES_2.length];

  private OzoneConfiguration conf;
  private MiniOzoneCluster cluster = null;
  private FileSystem fs;
  private OzoneClient client;

  @BeforeEach
  void setup() throws IOException,
      InterruptedException, TimeoutException {
    conf = new OzoneConfiguration();
    conf.setStorageSize(OZONE_SCM_CHUNK_SIZE_KEY, 1024 * 1024, StorageUnit.BYTES);
    conf.setStorageSize(OZONE_SCM_BLOCK_SIZE, 2 * 1024 * 1024, StorageUnit.BYTES);
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    String rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    String disableCache = String.format("fs.%s.impl.disable.cache",
        OzoneConsts.OZONE_OFS_URI_SCHEME);
    conf.setBoolean(disableCache, true);
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    System.arraycopy(DATA_SIZES_1, 0, dataSizes, 0, DATA_SIZES_1.length);
    System.arraycopy(DATA_SIZES_2, 0, dataSizes, DATA_SIZES_1.length, DATA_SIZES_2.length);
  }

  @AfterEach
  void teardown() {
    IOUtils.closeQuietly(client, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   *  Test EC checksum with Replicated checksum.
   */
  @ParameterizedTest
  @MethodSource("missingIndexesAndChecksumSize")
  void testEcFileChecksum(List<Integer> missingIndexes, double checksumSizeInMB) throws IOException {

    conf.setInt("ozone.client.bytes.per.checksum", (int) (checksumSizeInMB * 1024 * 1024));
    fs = FileSystem.get(conf);
    RootedOzoneFileSystem ofs = (RootedOzoneFileSystem) fs;
    BasicRootedOzoneClientAdapterImpl adapter = (BasicRootedOzoneClientAdapterImpl) ofs.getAdapter();
    String volumeName = UUID.randomUUID().toString();
    String legacyBucket = UUID.randomUUID().toString();
    String ecBucketName = UUID.randomUUID().toString();

    client.getObjectStore().createVolume(volumeName);

    BucketArgs.Builder bucketArgs = BucketArgs.newBuilder()
        .setStorageType(StorageType.DISK)
        .setBucketLayout(BucketLayout.LEGACY);

    createBucket(client, volumeName, bucketArgs.build(), legacyBucket);

    bucketArgs.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig("RS-3-2-1024k")));

    final OzoneBucket ecBucket =
        createBucket(client, volumeName, bucketArgs.build(), ecBucketName);

    assertEquals(ReplicationType.EC.name(),
        ecBucket.getReplicationConfig().getReplicationType().name());

    Map<Integer, String> replicatedChecksums = new HashMap<>();

    for (int dataLen : dataSizes) {
      byte[] data = RandomStringUtils.secure().nextAlphabetic(dataLen).getBytes(UTF_8);

      try (OutputStream file = adapter.createFile(volumeName + "/"
          + legacyBucket + "/test" + dataLen, (short) 3, true, false)) {
        file.write(data);
      }

      Path parent1 = new Path("/" + volumeName + "/" + legacyBucket + "/");
      Path replicatedKey = new Path(parent1, "test" + dataLen);
      FileChecksum replicatedChecksum = fs.getFileChecksum(replicatedKey);
      String replicatedChecksumString = StringUtils.byteToHexString(
          replicatedChecksum.getBytes(), 0, replicatedChecksum.getLength());
      replicatedChecksums.put(dataLen, replicatedChecksumString);

      try (OutputStream file = adapter.createFile(volumeName + "/"
          + ecBucketName + "/test" + dataLen, (short) 3, true, false)) {
        file.write(data);
      }
    }

    // Fail DataNodes
    for (int index: missingIndexes) {
      cluster.shutdownHddsDatanode(index);
    }

    for (boolean topologyAware : TOPOLOGY_AWARENESS) {
      OzoneConfiguration clientConf = new OzoneConfiguration(conf);
      clientConf.setBoolean(OZONE_NETWORK_TOPOLOGY_AWARE_READ_KEY,
          topologyAware);
      try (FileSystem fsForRead = FileSystem.get(clientConf)) {
        for (int dataLen : dataSizes) {
          // Compute checksum after failed DNs
          Path parent = new Path("/" + volumeName + "/" + ecBucketName + "/");
          Path ecKey = new Path(parent, "test" + dataLen);
          FileChecksum ecChecksum = fsForRead.getFileChecksum(ecKey);
          String ecChecksumString = StringUtils.byteToHexString(
              ecChecksum.getBytes(), 0, ecChecksum.getLength());

          assertEquals(replicatedChecksums.get(dataLen), ecChecksumString,
              () -> "Checksum mismatch for data size: " + dataLen +
                  ", topologyAware: " + topologyAware +
                  ", failed nodes: " + missingIndexes);
        }
      }
    }
  }

  static Stream<Arguments> missingIndexesAndChecksumSize() {
    return Stream.of(
        arguments(ImmutableList.of(0, 1), 0.001),
        arguments(ImmutableList.of(1, 2), 0.01),
        arguments(ImmutableList.of(2, 3), 0.1),
        arguments(ImmutableList.of(3, 4), 0.5),
        arguments(ImmutableList.of(0, 3), 1),
        arguments(ImmutableList.of(0, 4), 2));
  }
}
