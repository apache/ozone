/*
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
package org.apache.hadoop.ozone.genesis;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.common.ChunkBuffer;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.volume.HddsVolume;
import org.apache.hadoop.ozone.container.common.volume.ImmutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerBlockStrategy;
import org.apache.hadoop.ozone.container.keyvalue.impl.FilePerChunkStrategy;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;

import static java.nio.charset.StandardCharsets.UTF_8;
import org.apache.commons.io.FileUtils;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_BLOCK;
import static org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion.FILE_PER_CHUNK;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark for ChunkManager implementations.
 */
@Warmup(time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkChunkManager {

  private static final String DEFAULT_TEST_DATA_DIR =
      "target" + File.separator + "test" + File.separator + "data";

  private static final AtomicLong CONTAINER_COUNTER = new AtomicLong();

  private static final DispatcherContext WRITE_STAGE =
      new DispatcherContext.Builder()
          .setStage(DispatcherContext.WriteChunkStage.WRITE_DATA)
          .build();

  private static final DispatcherContext COMMIT_STAGE =
      new DispatcherContext.Builder()
          .setStage(DispatcherContext.WriteChunkStage.COMMIT_DATA)
          .build();

  private static final long CONTAINER_SIZE = OzoneConsts.GB;
  private static final long BLOCK_SIZE = 256 * OzoneConsts.MB;

  private static final String SCM_ID = UUID.randomUUID().toString();
  private static final String DATANODE_ID = UUID.randomUUID().toString();

  /**
   * State for the benchmark.
   */
  @State(Scope.Benchmark)
  public static class BenchmarkState {

    @Param({"1048576", "4194304", "16777216", "67108864"})
    private int chunkSize;

    private File dir;
    private ChunkBuffer buffer;
    private VolumeSet volumeSet;
    private OzoneConfiguration config;

    private static File getTestDir() throws IOException {
      File dir = new File(DEFAULT_TEST_DATA_DIR).getAbsoluteFile();
      Files.createDirectories(dir.toPath());
      return dir;
    }

    @Setup(Level.Iteration)
    public void setup() throws IOException {
      dir = getTestDir();
      config = new OzoneConfiguration();
      HddsVolume volume = new HddsVolume.Builder(dir.getAbsolutePath())
          .conf(config)
          .datanodeUuid(DATANODE_ID)
          .build();

      volumeSet = new ImmutableVolumeSet(volume);

      byte[] arr = randomAlphanumeric(chunkSize).getBytes(UTF_8);
      buffer = ChunkBuffer.wrap(ByteBuffer.wrap(arr));
    }

    @TearDown(Level.Iteration)
    public void cleanup() {
      FileUtils.deleteQuietly(dir);
    }
  }

  @Benchmark
  public void writeMultipleFiles(BenchmarkState state, Blackhole sink)
      throws StorageContainerException {

    ChunkManager chunkManager = new FilePerChunkStrategy(true, null);
    benchmark(chunkManager, FILE_PER_CHUNK, state, sink);
  }

  @Benchmark
  public void writeSingleFile(BenchmarkState state, Blackhole sink)
      throws StorageContainerException {

    ChunkManager chunkManager = new FilePerBlockStrategy(true, null);
    benchmark(chunkManager, FILE_PER_BLOCK, state, sink);
  }

  private void benchmark(ChunkManager subject, ChunkLayOutVersion layout,
      BenchmarkState state, Blackhole sink)
      throws StorageContainerException {

    final long containerID = CONTAINER_COUNTER.getAndIncrement();

    KeyValueContainerData containerData =
        new KeyValueContainerData(containerID, layout,
            CONTAINER_SIZE, UUID.randomUUID().toString(),
            DATANODE_ID);
    KeyValueContainer container =
        new KeyValueContainer(containerData, state.config);
    container.create(state.volumeSet, (volumes, any) -> volumes.get(0), SCM_ID);

    final long blockCount = CONTAINER_SIZE / BLOCK_SIZE;
    final long chunkCount = BLOCK_SIZE / state.chunkSize;

    for (long b = 0; b < blockCount; b++) {
      final BlockID blockID = new BlockID(containerID, b);

      for (long c = 0; c < chunkCount; c++) {
        final String chunkName = String.format("block.%d.chunk.%d", b, c);
        final long offset = c * state.chunkSize;
        ChunkInfo chunkInfo = new ChunkInfo(chunkName, offset, state.chunkSize);
        ChunkBuffer data = state.buffer.duplicate(0, state.chunkSize);

        subject.writeChunk(container, blockID, chunkInfo, data, WRITE_STAGE);
        subject.writeChunk(container, blockID, chunkInfo, data, COMMIT_STAGE);

        sink.consume(chunkInfo);
      }
    }
  }

}
