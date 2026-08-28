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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_ADDRESS_KEY;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

/**
 * On-demand benchmark (not part of {@code mvn test}) for HDDS-15678.
 *
 * <p>Measures OFS {@link FileSystem#isFile}/{@link FileSystem#isDirectory}
 * (the metadata-only head-op path added by this change) against a full
 * {@link FileSystem#getFileStatus} on the same <b>file</b> path. For a file,
 * the full path makes the OM contact SCM to refresh pipeline/block locations;
 * the head-op path skips that round-trip. The A/B in a single run isolates the
 * eliminated SCM refresh (FULL = pre-fix behaviour, HEAD = this fix).
 *
 * <p>To instead run a classic before/after across two builds, revert the
 * {@code isDirectory}/{@code isFile} overrides in
 * {@link BasicRootedOzoneFileSystem} and compare the HEAD numbers.
 *
 * <p>The {@code benchmark} tag is excluded from {@code mvn test} and CI by
 * default, so it must be re-enabled explicitly to run on demand:
 *
 * <pre>
 *   mvn -pl hadoop-ozone/integration-test test \
 *     -Dtest=TestOFSIsDirectoryBenchmark -Dgroups=benchmark \
 *     -Dexcluded-test-groups= -DskipShade -DskipRecon \
 *     -Dsurefire.failIfNoSpecifiedTests=false
 * </pre>
 */
@Tag("benchmark")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestOFSIsDirectoryBenchmark {

  private static final int WARMUP = 2_000;
  private static final int ITERATIONS = 20_000;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private FileSystem fs;
  private Path filePath;

  @BeforeAll
  void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(OMConfigKeys.OZONE_DEFAULT_BUCKET_LAYOUT,
        BucketLayout.FILE_SYSTEM_OPTIMIZED.name());
    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();

    ObjectStore store = client.getObjectStore();
    store.createVolume("vol");
    store.getVolume("vol").createBucket("bucket");

    String rootPath = String.format("%s://%s/",
        OzoneConsts.OZONE_OFS_URI_SCHEME, conf.get(OZONE_OM_ADDRESS_KEY));
    conf.set(FS_DEFAULT_NAME_KEY, rootPath);
    fs = FileSystem.get(conf);

    filePath = new Path("/vol/bucket/file");
    try (FSDataOutputStream out = fs.create(filePath, true)) {
      out.write(new byte[4096]);
    }
  }

  @AfterAll
  void cleanup() throws IOException {
    if (fs != null) {
      fs.close();
    }
    if (client != null) {
      client.close();
    }
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @FunctionalInterface
  private interface Op {
    void run() throws IOException;
  }

  private long timeNanos(Op op) throws IOException {
    long start = System.nanoTime();
    for (int i = 0; i < ITERATIONS; i++) {
      op.run();
    }
    return System.nanoTime() - start;
  }

  @Test
  @Timeout(value = 600, unit = TimeUnit.SECONDS)
  @SuppressWarnings("deprecation") // FileSystem.isFile is the API under test
  void benchmarkHeadOpVsFullStatus() throws IOException {
    // Warm up both paths.
    for (int i = 0; i < WARMUP; i++) {
      fs.isFile(filePath);
      fs.getFileStatus(filePath);
    }

    long headNanos = timeNanos(() -> fs.isFile(filePath));
    long fullNanos = timeNanos(() -> fs.getFileStatus(filePath));

    double headOps = ITERATIONS * 1_000_000_000.0 / headNanos;
    double fullOps = ITERATIONS * 1_000_000_000.0 / fullNanos;

    System.out.println();
    System.out.println("=== HDDS-15678 OFS type-check benchmark ===");
    System.out.printf("iterations=%d on a 1-block file%n", ITERATIONS);
    System.out.printf("FULL getFileStatus (pre-fix): %,10.0f ops/s  %6.1f us/op%n",
        fullOps, fullNanos / 1000.0 / ITERATIONS);
    System.out.printf("HEAD isFile      (this fix ): %,10.0f ops/s  %6.1f us/op%n",
        headOps, headNanos / 1000.0 / ITERATIONS);
    System.out.printf("speedup (head/full): %.2fx%n", headOps / fullOps);
  }
}
