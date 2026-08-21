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

package org.apache.hadoop.ozone.om.request.key;

import static org.apache.hadoop.ozone.om.request.file.OMFileRequest.getOmKeyInfo;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.cache.CacheKey;
import org.apache.hadoop.hdds.utils.db.cache.CacheValue;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.om.execution.flowcontrol.ExecutionContext;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDirectoryInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketNameInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PurgePathRequest;
import org.apache.hadoop.util.Time;
import org.apache.ozone.test.GenericTestUtils;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.event.Level;

/**
 * Handler-level microbenchmark for {@link OMDirectoriesPurgeRequestWithFSO#validateAndUpdateCache}.
 *
 * <p>The OM applies write transactions on a single serial apply thread, so the wall time of
 * {@code validateAndUpdateCache} directly bounds how long all other writes/renames/mkdirs wait behind a background
 * directory purge. This benchmark times that apply call over a single large single-bucket {@code PurgeDirectories}
 * transaction, isolating the apply-thread CPU that the workspace changes reduce:
 * <ul>
 *   <li>sub-entries are read as scalars straight from the protobuf instead of building a full {@link OmKeyInfo}
 *       (with its key-location, ACL, metadata and encryption objects) per entry, and</li>
 *   <li>the owning bucket's cache entry is memoized once per apply instead of looked up per entry.</li>
 * </ul>
 *
 * <p>The {@code benchmark} tag is excluded from {@code mvn test} and CI by default, so it must be re-enabled
 * explicitly to run on demand:
 * <pre>
 *   mvn -pl :ozone-manager test -DskipShade -DskipRecon -DskipDocs \
 *     -Dtest=TestOMDirectoriesPurgeApplyPerf -Dgroups=benchmark -Dexcluded-test-groups= \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 * Tunables (system properties): {@code bench.subEntries} (default 1000), {@code bench.iters} (default 20),
 * {@code bench.warmup} (default 5).
 */
@Tag("benchmark")
public class TestOMDirectoriesPurgeApplyPerf extends OMKeyRequestTests {

  private static final long DIR_OBJECT_ID = 1L;
  private static final long BLOCK_LENGTH = 100L;
  private static final int VERSIONS_PER_FILE = 2;
  private static final int BLOCKS_PER_VERSION = 3;

  @Test
  public void benchmarkApplyThreadTime() throws Exception {
    // Match production: the debug-guarded success audit is off by default, so it should not colour the timing.
    GenericTestUtils.setLogLevel(OMKeyRequest.class, Level.INFO);

    final int subEntries = Integer.getInteger("bench.subEntries", 1000);
    final int iters = Integer.getInteger("bench.iters", 20);
    final int warmup = Integer.getInteger("bench.warmup", 5);
    final long applies = (long) (iters + warmup);

    when(ozoneManager.getDefaultReplicationConfig())
        .thenReturn(RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE));

    String bucket = "bucket" + UUID.randomUUID();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucket, omMetadataManager,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucket);
    OmBucketInfo bucketInfo = omMetadataManager.getBucketTable().get(bucketKey);
    long volumeId = omMetadataManager.getVolumeId(volumeName);
    long bucketId = bucketInfo.getObjectID();

    // Seed enough quota that the per-apply decrements (repeated across warmup + measured runs) never underflow.
    long perFileReplicatedBytes = replicatedBytesPerFile();
    bucketInfo.incrUsedBytes(applies * subEntries * perFileReplicatedBytes * 2);
    bucketInfo.incrUsedNamespace(applies * subEntries * 2L * 2);
    omMetadataManager.getBucketTable().addCacheEntry(new CacheKey<>(bucketKey), CacheValue.get(1L, bucketInfo));
    omMetadataManager.getBucketTable().put(bucketKey, bucketInfo);

    OMRequest preExecuted = preExecutePurgeRequest(
        buildSingleBucketPurgeRequest(volumeId, bucketId, bucket, subEntries));

    // Warmup (let the JIT compile the apply path) then measure.
    for (int i = 0; i < warmup; i++) {
      applyOnce(preExecuted);
    }
    long[] samplesNs = new long[iters];
    long blackhole = 0;
    for (int i = 0; i < iters; i++) {
      long t0 = System.nanoTime();
      blackhole += applyOnce(preExecuted);
      samplesNs[i] = System.nanoTime() - t0;
    }

    Arrays.sort(samplesNs);
    long sum = 0;
    for (long s : samplesNs) {
      sum += s;
    }
    double meanUs = sum / (double) iters / 1000.0;
    double p50Us = samplesNs[iters / 2] / 1000.0;
    double p90Us = samplesNs[Math.min(iters - 1, (int) Math.ceil(iters * 0.9) - 1)] / 1000.0;
    double minUs = samplesNs[0] / 1000.0;
    double maxUs = samplesNs[iters - 1] / 1000.0;

    // Single grep-able line so baseline and changed runs can be compared directly.
    System.out.printf("BENCH apply subEntries=%d (subFiles+subDirs) iters=%d warmup=%d "
            + "meanUs=%.1f p50Us=%.1f p90Us=%.1f minUs=%.1f maxUs=%.1f totalMs=%.1f blackhole=%d%n",
        subEntries, iters, warmup, meanUs, p50Us, p90Us, minUs, maxUs, sum / 1_000_000.0, blackhole);
  }

  private long applyOnce(OMRequest preExecuted) {
    OMDirectoriesPurgeRequestWithFSO request = new OMDirectoriesPurgeRequestWithFSO(preExecuted);
    // Only validateAndUpdateCache runs on the apply thread; the RocksDB batch flush happens off-thread later, so it is
    // intentionally excluded here.
    ExecutionContext context = ExecutionContext.of(100L, TermIndex.valueOf(1L, 100L));
    return request.validateAndUpdateCache(ozoneManager, context) == null ? 0 : 1;
  }

  private long replicatedBytesPerFile() {
    // getReplicatedSize(len, RATIS THREE) == 3 * len; VERSIONS_PER_FILE * BLOCKS_PER_VERSION blocks per file.
    return (long) VERSIONS_PER_FILE * BLOCKS_PER_VERSION * BLOCK_LENGTH * 3;
  }

  private OMRequest preExecutePurgeRequest(OMRequest omRequest) throws java.io.IOException {
    return new OMKeyPurgeRequest(omRequest).preExecute(ozoneManager);
  }

  /**
   * Builds one {@link PurgePathRequest} that marks {@code subEntries} sub-directories deleted and moves
   * {@code subEntries} sub-files, all under a single bucket, wrapped in a {@code PurgeDirectories} {@link OMRequest}.
   */
  private OMRequest buildSingleBucketPurgeRequest(long volumeId, long bucketId, String bucket, int subEntries) {
    PurgePathRequest.Builder path = PurgePathRequest.newBuilder()
        .setVolumeId(volumeId)
        .setBucketId(bucketId);

    long objectId = 100L;
    for (int i = 0; i < subEntries; i++) {
      OmDirectoryInfo subdir = OmDirectoryInfo.newBuilder()
          .setName("subdir" + i)
          .setCreationTime(Time.now())
          .setModificationTime(Time.now())
          .setObjectID(objectId++)
          .setParentObjectID(DIR_OBJECT_ID)
          .setUpdateID(0)
          .build();
      OmKeyInfo subDirKey = getOmKeyInfo(volumeName, bucket, subdir, "dir1/" + subdir.getName());
      path.addMarkDeletedSubDirs(subDirKey.getProtobuf(ClientVersion.CURRENT_VERSION));

      OmKeyInfo subFile = OMRequestTestUtils.createOmKeyInfo(volumeName, bucket, "dir1/file" + i,
              RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE))
          .setObjectID(objectId++)
          .setParentObjectID(DIR_OBJECT_ID)
          .setUpdateID(100L)
          .build();
      subFile.setKeyLocationVersions(buildLocationVersions());
      path.addDeletedSubFiles(subFile.getProtobuf(true, ClientVersion.CURRENT_VERSION));
    }

    OzoneManagerProtocolProtos.PurgeDirectoriesRequest.Builder purgeDir =
        OzoneManagerProtocolProtos.PurgeDirectoriesRequest.newBuilder()
            .addDeletedPath(path.build())
            .addBucketNameInfos(BucketNameInfo.newBuilder()
                .setVolumeName(volumeName).setBucketName(bucket)
                .setVolumeId(volumeId).setBucketId(bucketId).build());

    return OMRequest.newBuilder()
        .setCmdType(OzoneManagerProtocolProtos.Type.PurgeDirectories)
        .setPurgeDirectoriesRequest(purgeDir)
        .setClientId(UUID.randomUUID().toString())
        .build();
  }

  private List<OmKeyLocationInfoGroup> buildLocationVersions() {
    List<OmKeyLocationInfoGroup> versions = new ArrayList<>(VERSIONS_PER_FILE);
    for (int v = 0; v < VERSIONS_PER_FILE; v++) {
      List<OmKeyLocationInfo> locations = new ArrayList<>(BLOCKS_PER_VERSION);
      for (int b = 0; b < BLOCKS_PER_VERSION; b++) {
        locations.add(new OmKeyLocationInfo.Builder()
            .setLength(BLOCK_LENGTH)
            .setBlockID(new BlockID(1 + v, 1 + b))
            .build());
      }
      versions.add(new OmKeyLocationInfoGroup(v, locations, false));
    }
    return versions;
  }

  @Override
  public BucketLayout getBucketLayout() {
    return BucketLayout.FILE_SYSTEM_OPTIMIZED;
  }
}
