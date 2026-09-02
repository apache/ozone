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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_CONTAINER_REPORT_INTERVAL;
import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compares the cost of a server side CopyKey, which only writes metadata, with
 * the read-and-rewrite copy the S3 gateway and {@code ozone sh key cp} perform
 * today.
 *
 * <p>Run with:
 * {@code mvn -pl :ozone-integration-test test -Dtest=BenchmarkCopyKey}
 *
 * <p>Everything runs on one host, so the byte copy never pays for real network
 * transfer and is as fast here as it will ever be. The ratios below are
 * therefore a lower bound on what a real cluster would show.
 */
public class BenchmarkCopyKey {

  private static final Logger LOG = LoggerFactory.getLogger(BenchmarkCopyKey.class);

  private static final int MIB = 1024 * 1024;
  private static final long[] SIZES_MIB = {1, 16, 64, 256};
  private static final int ITERATIONS = 5;

  private static MiniOzoneCluster cluster;
  private static OzoneClient client;
  private static OzoneBucket bucket;

  @BeforeAll
  public static void setup() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setTimeDuration(HDDS_HEARTBEAT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setTimeDuration(HDDS_CONTAINER_REPORT_INTERVAL, 200, TimeUnit.MILLISECONDS);
    conf.setQuietMode(false);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = OzoneClientFactory.getRpcClient(conf);
    ObjectStore store = client.getObjectStore();

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName, BucketArgs.newBuilder()
        .setBucketLayout(BucketLayout.OBJECT_STORE).build());
    bucket = volume.getBucket(bucketName);
  }

  @AfterAll
  public static void shutdown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void compareServerSideCopyWithByteCopy() throws Exception {
    List<String> report = new ArrayList<>();
    report.add(String.format("%10s %14s %14s %10s", "size", "byte copy ms", "CopyKey ms", "speedup"));

    for (long sizeMib : SIZES_MIB) {
      long size = sizeMib * MIB;
      String sourceKey = "src-" + sizeMib + "m";
      writeKey(sourceKey, size);

      // Warm up both paths so JIT and connection setup are not attributed to
      // whichever runs first.
      timeByteCopy(sourceKey, "warmup-byte-" + sizeMib, size);
      timeServerSideCopy(sourceKey, "warmup-copykey-" + sizeMib);

      List<Long> byteCopyNanos = new ArrayList<>();
      List<Long> copyKeyNanos = new ArrayList<>();
      for (int i = 0; i < ITERATIONS; i++) {
        // Alternate the order: within one iteration whichever runs second
        // benefits from the warming the first one caused.
        if (i % 2 == 0) {
          byteCopyNanos.add(timeByteCopy(sourceKey, "byte-" + sizeMib + "-" + i, size));
          copyKeyNanos.add(timeServerSideCopy(sourceKey, "copykey-" + sizeMib + "-" + i));
        } else {
          copyKeyNanos.add(timeServerSideCopy(sourceKey, "copykey-" + sizeMib + "-" + i));
          byteCopyNanos.add(timeByteCopy(sourceKey, "byte-" + sizeMib + "-" + i, size));
        }
      }

      double byteCopyMs = medianMillis(byteCopyNanos);
      double copyKeyMs = medianMillis(copyKeyNanos);
      report.add(String.format("%9dM %14.1f %14.1f %9.0fx",
          sizeMib, byteCopyMs, copyKeyMs, byteCopyMs / copyKeyMs));
      LOG.info("size={}MiB byteCopy={}ms copyKey={}ms", sizeMib, byteCopyMs, copyKeyMs);
    }

    // Destination keys are deliberately never deleted during the run: block
    // reclamation of a deleted key would otherwise run inside a measurement.
    LOG.info("\n=== CopyKey vs byte copy ===\n{}", String.join("\n", report));
    report.forEach(System.out::println);
  }

  private static long timeByteCopy(String sourceKey, String destinationKey, long size)
      throws IOException {
    long start = System.nanoTime();
    try (InputStream in = bucket.readKey(sourceKey);
         OutputStream out = bucket.createKey(destinationKey, size)) {
      org.apache.hadoop.io.IOUtils.copyBytes(in, out, 4 * MIB);
    }
    return System.nanoTime() - start;
  }

  private static long timeServerSideCopy(String sourceKey, String destinationKey)
      throws IOException {
    long start = System.nanoTime();
    bucket.copyKey(sourceKey, destinationKey, Collections.emptyMap());
    return System.nanoTime() - start;
  }

  private static void writeKey(String keyName, long size) throws IOException {
    byte[] chunk = new byte[MIB];
    for (int i = 0; i < chunk.length; i++) {
      chunk[i] = (byte) (i % 251);
    }
    try (OutputStream out = bucket.createKey(keyName, size)) {
      long written = 0;
      while (written < size) {
        int toWrite = (int) Math.min(chunk.length, size - written);
        out.write(chunk, 0, toWrite);
        written += toWrite;
      }
    }
  }

  /**
   * Reads both keys and compares their digests, so the timings above are known
   * to describe copies that actually produced the same bytes.
   */
  @Test
  public void copiesAreByteIdentical() throws Exception {
    String sourceKey = "verify-src";
    String copiedKey = "verify-copy";
    writeKey(sourceKey, 8L * MIB);
    bucket.copyKey(sourceKey, copiedKey, Collections.emptyMap());
    assertArrayEquals(digest(sourceKey), digest(copiedKey));
  }

  private static byte[] digest(String keyName) throws Exception {
    MessageDigest md5 = MessageDigest.getInstance("MD5");
    byte[] buffer = new byte[64 * 1024];
    try (InputStream in = bucket.readKey(keyName)) {
      int read;
      while ((read = in.read(buffer)) > 0) {
        md5.update(buffer, 0, read);
      }
    }
    return md5.digest();
  }

  private static double medianMillis(List<Long> nanos) {
    List<Long> sorted = new ArrayList<>(nanos);
    Collections.sort(sorted);
    long median = sorted.get(sorted.size() / 2);
    return median / 1_000_000.0;
  }
}
