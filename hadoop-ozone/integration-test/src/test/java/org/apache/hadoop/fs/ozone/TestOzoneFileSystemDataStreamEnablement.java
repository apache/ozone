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

import static org.apache.hadoop.hdds.HddsConfigKeys.HDDS_HEARTBEAT_INTERVAL;
import static org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name.RATIS_DATASTREAM;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_DEADNODE_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_CREATION_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_PIPELINE_SCRUB_INTERVAL;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_RATIS_PIPELINE_LIMIT;
import static org.apache.hadoop.hdds.scm.ScmConfigKeys.OZONE_SCM_STALENODE_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_ENABLED;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_URI_SCHEME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BooleanSupplier;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineManagerImpl;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.ClientConfigForTesting;
import org.apache.hadoop.ozone.DataTestUtil;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.io.SelectorOutputStream;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end tests for enabling Ratis DataStream on a running cluster
 * (HDDS-12991). The two tests are isolated (separate clusters):
 * <ul>
 *   <li>{@link #testDataStreamFallbackAndPortRefresh()}: a streaming client
 *   gracefully falls back to the non-streaming path while a pipeline lacks the
 *   RATIS_DATASTREAM port, and SCM refreshes a datanode's ports when it
 *   re-registers with datastream enabled.</li>
 *   <li>{@link #testCloseNonStreamablePipelineThenStream()}: SCM closes a
 *   pipeline created before datastream (which can never stream in place) so a
 *   fresh streaming-capable pipeline replaces it, after which a streaming write
 *   succeeds end-to-end.</li>
 * </ul>
 *
 * <p>Writes to portless pipelines throw instead of falling back
 * (HDDS-12991 part 1 is not yet implemented), so the pre-enable writes in each
 * test use a non-streaming FileSystem. The post-enable writes use a retry loop
 * to absorb the transition window while the background scrubber closes portless
 * pipelines and a fresh streaming-capable pipeline is created.
 */
public class TestOzoneFileSystemDataStreamEnablement {

  // Small threshold/payload keep the writes fast while still selecting the
  // streaming path (payload > threshold).
  private static final int AUTO_THRESHOLD = 4 << 10;
  private static final int WRITE_SIZE = 256 << 10;
  // Retry budget for writes in the pipeline-transition window.
  private static final int MAX_WRITE_ATTEMPTS = 10;
  private static final long WRITE_RETRY_DELAY_MS = 3_000L;

  private MiniOzoneCluster cluster;
  private OzoneClient client;
  private OzoneBucket bucket;
  private OzoneConfiguration conf;

  private void startClusterWithDatanodeStreamDisabled() throws Exception {
    conf = new OzoneConfiguration();
    // Datanode side: datastream initially disabled, so pipelines are created
    // without the RATIS_DATASTREAM port.
    conf.setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, false);
    // Client side: always attempt streaming writes.
    conf.setBoolean(OZONE_FS_DATASTREAM_ENABLED, true);
    conf.set(OZONE_FS_DATASTREAM_AUTO_THRESHOLD, AUTO_THRESHOLD + "B");
    conf.setInt(OZONE_SCM_RATIS_PIPELINE_LIMIT, 10);
    // A long stale interval keeps the OPEN pipeline alive across the (no
    // stop-wait) rolling restart, so the test drives pipeline closure itself.
    conf.set(OZONE_SCM_STALENODE_INTERVAL, "5m");
    conf.set(OZONE_SCM_DEADNODE_INTERVAL, "10m");
    conf.set(HDDS_HEARTBEAT_INTERVAL, "1s");
    conf.set(OZONE_SCM_HEARTBEAT_PROCESS_INTERVAL, "1s");
    // Recreate pipelines quickly after a close so the test does not wait the
    // default two minutes for a fresh RATIS/THREE pipeline.
    conf.set(OZONE_SCM_PIPELINE_CREATION_INTERVAL, "1s");
    // Run the port-scrubber frequently so portless pipelines are closed quickly.
    conf.set(OZONE_SCM_PIPELINE_SCRUB_INTERVAL, "5s");

    final int chunkSize = 16 << 10;
    ClientConfigForTesting.newBuilder(StorageUnit.BYTES)
        .setChunkSize(chunkSize)
        .setStreamBufferFlushSize(32 << 10)
        .setStreamBufferMaxSize(64 << 10)
        .setDataStreamBufferFlushSize(64 << 10)
        .setDataStreamMinPacketSize(chunkSize)
        .setDataStreamWindowSize(5 * chunkSize)
        .setBlockSize(1 << 20)
        .applyTo(conf);

    cluster = MiniOzoneCluster.newBuilder(conf).setNumDatanodes(3).build();
    cluster.waitForClusterToBeReady();
    client = cluster.newClient();
    bucket = DataTestUtil.createVolumeAndBucket(client,
        BucketLayout.FILE_SYSTEM_OPTIMIZED);
  }

  @AfterEach
  public void teardown() {
    IOUtils.closeQuietly(client);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  private FileSystem fs() throws IOException {
    final String rootPath = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    return FileSystem.get(conf);
  }

  /** A FileSystem backed by the same bucket but with datastream disabled. */
  private FileSystem nonStreamingFs() throws IOException {
    final String rootPath = String.format("%s://%s.%s/",
        OZONE_URI_SCHEME, bucket.getName(), bucket.getVolumeName());
    final OzoneConfiguration noStream = new OzoneConfiguration(conf);
    noStream.setBoolean(OZONE_FS_DATASTREAM_ENABLED, false);
    noStream.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, rootPath);
    // newInstance bypasses the FileSystem cache so the streaming=false setting
    // is not shadowed by the streaming=true FS cached under the same URI.
    return FileSystem.newInstance(noStream);
  }

  /**
   * Retries {@link #writeAndGetUnderlying} on IOException to absorb the window
   * while SCM closes a portless pipeline and a new streaming one is created.
   */
  private Class<?> writeWithRetry(FileSystem fs, Path path, byte[] data)
      throws Exception {
    for (int attempt = 1; attempt < MAX_WRITE_ATTEMPTS; attempt++) {
      try {
        return writeAndGetUnderlying(fs, path, data);
      } catch (IOException ignored) {
        Thread.sleep(WRITE_RETRY_DELAY_MS);
      }
    }
    return writeAndGetUnderlying(fs, path, data);
  }

  /** Write {@code data} and return the underlying stream selected by the FS. */
  private static Class<?> writeAndGetUnderlying(FileSystem fs, Path path,
      byte[] data) throws IOException {
    final FSDataOutputStream out = fs.create(path, true);
    out.write(data);
    final SelectorOutputStream<?> selector =
        (SelectorOutputStream<?>) out.getWrappedStream();
    out.close();
    return selector.getUnderlying().getClass();
  }

  private static void assertRoundTrips(FileSystem fs, Path path, byte[] expected)
      throws IOException {
    final byte[] read = new byte[expected.length];
    try (FSDataInputStream in = fs.open(path)) {
      in.readFully(read);
    }
    assertArrayEquals(expected, read);
  }

  private static byte[] randomBytes() {
    final byte[] bytes = new byte[WRITE_SIZE];
    ThreadLocalRandom.current().nextBytes(bytes);
    return bytes;
  }

  private List<Pipeline> openRatisThreePipelines() {
    return cluster.getStorageContainerManager().getPipelineManager()
        .getPipelines(RatisReplicationConfig.getInstance(THREE),
            Pipeline.PipelineState.OPEN);
  }

  private static boolean noNodesHaveDatastreamPort(Pipeline pipeline) {
    return pipeline.getNodes().stream()
        .noneMatch(n -> n.hasPort(RATIS_DATASTREAM));
  }

  private static boolean allNodesHaveDatastreamPort(Pipeline pipeline) {
    return pipeline.getNodes().stream()
        .allMatch(n -> n.hasPort(RATIS_DATASTREAM));
  }

  /**
   * Enable datastream on every datanode via a rolling restart. {@code false}
   * (no stop-wait) keeps each restart short; combined with the long stale
   * interval the OPEN pipeline survives, so its node snapshot stays portless.
   * Also reflects the enablement in the SCM config so that
   * {@code closePipelinesMissingDataStreamPort} does not skip portless detection.
   */
  private void rollingRestartEnablingDataStream() throws Exception {
    for (int i = 0; i < cluster.getHddsDatanodes().size(); i++) {
      cluster.getHddsDatanodes().get(i).getConf()
          .setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
      cluster.restartHddsDatanode(i, false);
    }
    cluster.waitForClusterToBeReady();
    // Update the shared conf (picked up by a restarted SCM) and the currently
    // running SCM so closePipelinesMissingDataStreamPort sees the feature enabled.
    conf.setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
    cluster.getStorageContainerManager().getConfiguration()
        .setBoolean(HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED, true);
  }

  /** Poll until SCM's node records all expose RATIS_DATASTREAM (validates D). */
  private void waitForAllRegisteredNodesToHaveDatastreamPort()
      throws InterruptedException, TimeoutException {
    final BooleanSupplier ready = () -> {
      final List<? extends DatanodeDetails> nodes = cluster
          .getStorageContainerManager().getScmNodeManager().getAllNodes();
      return nodes.size() == cluster.getHddsDatanodes().size()
          && nodes.stream().allMatch(n -> n.hasPort(RATIS_DATASTREAM));
    };
    GenericTestUtils.waitFor(ready, 500, 30_000);
  }

  /** Poll until an OPEN RATIS/THREE pipeline exposes RATIS_DATASTREAM ports. */
  private void waitForStreamablePipeline()
      throws InterruptedException, TimeoutException {
    final BooleanSupplier ready = () -> openRatisThreePipelines().stream()
        .anyMatch(TestOzoneFileSystemDataStreamEnablement
            ::allNodesHaveDatastreamPort);
    GenericTestUtils.waitFor(ready, 500, 60_000);
  }

  /**
   * After a rolling restart enables datastream, SCM refreshes the datanodes'
   * ports. Closing the pre-existing portless pipeline (and replacing it with
   * a streaming-capable one) may require a few retries because the SCM Ratis
   * group can briefly lose leadership stability right after the rolling
   * restart. A write that retries during the transition window eventually
   * succeeds over the new streaming pipeline.
   */
  @Test
  @Timeout(value = 240, unit = TimeUnit.SECONDS)
  public void testDataStreamFallbackAndPortRefresh() throws Exception {
    startClusterWithDatanodeStreamDisabled();

    try (FileSystem fs = fs()) {
      final byte[] data = randomBytes();

      // Write before enabling datastream. The streaming path would throw on a
      // portless pipeline (HDDS-12991 part 1 not yet implemented), so use a
      // non-streaming FS to populate the cluster and open a portless pipeline.
      final Path before = new Path("/before-enable.dat");
      try (FileSystem noStream = nonStreamingFs()) {
        try (FSDataOutputStream out = noStream.create(before, true)) {
          out.write(data);
        }
      }
      assertRoundTrips(fs, before, data);

      rollingRestartEnablingDataStream();
      waitForAllRegisteredNodesToHaveDatastreamPort();

      // Close portless pipelines via explicit scrub calls (retried so any
      // transient SCM Ratis leader disruption from the rolling restart is
      // absorbed) and wait for a streaming-capable replacement to appear.
      final PipelineManagerImpl pipelineManager =
          (PipelineManagerImpl) cluster.getStorageContainerManager().getPipelineManager();
      final BooleanSupplier streamingPipelineReady = () -> {
        pipelineManager.scrubAndClosePipelinesMissingDataStreamPort();
        return openRatisThreePipelines().stream()
            .anyMatch(TestOzoneFileSystemDataStreamEnablement::allNodesHaveDatastreamPort);
      };
      GenericTestUtils.waitFor(streamingPipelineReady, 1_000, 120_000);

      final Path after = new Path("/after-enable.dat");
      assertEquals(CapableOzoneFSDataStreamOutput.class,
          writeWithRetry(fs, after, data));
      assertRoundTrips(fs, after, data);
    }
  }

  /**
   * A pipeline created while datastream was disabled keeps a portless node
   * snapshot and a stale datastream address in its Raft group, so it can
   * never serve streaming even after the datanodes restart. SCM closes it so a
   * fresh, streaming-capable pipeline is created; a streaming write then
   * succeeds over the new pipeline (HDDS-12991).
   */
  @Test
  @Timeout(value = 70, unit = TimeUnit.SECONDS)
  public void testCloseNonStreamablePipelineThenStream() throws Exception {
    startClusterWithDatanodeStreamDisabled();

    try (FileSystem fs = fs()) {
      final byte[] data = randomBytes();

      // Create a portless OPEN pipeline. The streaming path would throw on a
      // portless pipeline (HDDS-12991 part 1 not yet implemented), so use a
      // non-streaming FS to open a pipeline without the RATIS_DATASTREAM port.
      final Path p1 = new Path("/legacy.dat");
      try (FileSystem noStream = nonStreamingFs()) {
        try (FSDataOutputStream out = noStream.create(p1, true)) {
          out.write(data);
        }
      }
      final List<Pipeline> before = openRatisThreePipelines();
      assertFalse(before.isEmpty());
      before.forEach(p -> assertTrue(noNodesHaveDatastreamPort(p),
          "pipeline should be portless before enabling datastream"));

      rollingRestartEnablingDataStream();
      waitForAllRegisteredNodesToHaveDatastreamPort();

      // SCM restart reloads the persisted (still portless) pipeline while the
      // datanodes are registered with the port (no re-registration event fires).
      cluster.restartStorageContainerManager(true);
      waitForAllRegisteredNodesToHaveDatastreamPort();

      final PipelineManagerImpl pipelineManager =
          (PipelineManagerImpl) cluster.getStorageContainerManager().getPipelineManager();
      final List<Pipeline> reloaded = openRatisThreePipelines();
      reloaded.retainAll(before);
      reloaded.forEach(p -> assertTrue(noNodesHaveDatastreamPort(p),
          "reloaded pipeline should still be portless"));

      // Close the pipeline(s) exposing the new datastream port; a fresh
      // streaming-capable pipeline is created in their place by
      // BackgroundPipelineCreator.
      pipelineManager.scrubAndClosePipelinesMissingDataStreamPort();
      waitForStreamablePipeline();

      // The new pipeline serves a streaming write end-to-end.
      final Path p2 = new Path("/after-recreate.dat");
      assertEquals(CapableOzoneFSDataStreamOutput.class,
          writeWithRetry(fs, p2, data));
      assertRoundTrips(fs, p2, data);
    }
  }

  /**
   * Full lifecycle over a batch of files: write several files while datastream
   * is disabled (using a non-streaming FS since the streaming path would throw
   * on portless pipelines), then enable datastream (rolling restart + SCM
   * restart + close the non-streamable pipeline), then write several more files
   * that must all succeed over a streaming-capable pipeline. Asserts that none
   * of the writes fail, the post-enablement writes take the streaming path, and
   * an OPEN pipeline exposing the RATIS_DATASTREAM port serves them.
   */
  @Test
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  public void testBatchWritesAcrossStreamingEnablement() throws Exception {
    startClusterWithDatanodeStreamDisabled();

    final int fileCount = 5;
    try (FileSystem fs = fs()) {
      // Phase 1: datastream disabled. The streaming path throws on portless
      // pipelines (HDDS-12991 part 1 not yet implemented), so write via a
      // non-streaming FS to confirm the cluster accepts writes.
      try (FileSystem noStream = nonStreamingFs()) {
        for (int i = 0; i < fileCount; i++) {
          final byte[] data = randomBytes();
          final Path p = new Path("/disabled-" + i + ".dat");
          try (FSDataOutputStream out = noStream.create(p, true)) {
            out.write(data);
          }
          assertRoundTrips(noStream, p, data);
        }
      }

      // Enable datastream on the datanodes and replace the legacy pipeline.
      rollingRestartEnablingDataStream();
      waitForAllRegisteredNodesToHaveDatastreamPort();
      cluster.restartStorageContainerManager(true);
      waitForAllRegisteredNodesToHaveDatastreamPort();
      ((PipelineManagerImpl) cluster.getStorageContainerManager().getPipelineManager())
          .scrubAndClosePipelinesMissingDataStreamPort();
      waitForStreamablePipeline();

      // Phase 2: datastream enabled -> every write streams, none fail.
      for (int i = 0; i < fileCount; i++) {
        final byte[] data = randomBytes();
        final Path p = new Path("/enabled-" + i + ".dat");
        assertEquals(CapableOzoneFSDataStreamOutput.class,
            writeWithRetry(fs, p, data),
            "write after enabling datastream must use the streaming path");
        assertRoundTrips(fs, p, data);
      }

      // The post-enablement writes are served by a streaming-capable pipeline.
      assertTrue(openRatisThreePipelines().stream()
          .anyMatch(TestOzoneFileSystemDataStreamEnablement
              ::allNodesHaveDatastreamPort),
          "an OPEN pipeline should expose the RATIS_DATASTREAM port");
    }
  }
}
