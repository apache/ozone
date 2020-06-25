/*
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.freon;

import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.impl.ChunkLayOutVersion;
import org.apache.hadoop.ozone.container.common.interfaces.VolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.volume.MutableVolumeSet;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerFactory;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;

import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.RandomStringUtils.randomAscii;

/**
 * Data generator to use pure datanode XCeiver interface.
 */
@Command(name = "cmdw",
    aliases = "chunk-manager-disk-write",
    description = "Write chunks as fast as possible.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true,
    showDefaultValues = true)
public class ChunkManagerDiskWrite extends BaseFreonGenerator implements
    Callable<Void> {

  private static final Logger LOG =
      LoggerFactory.getLogger(ChunkManagerDiskWrite.class);

  @Option(names = {"-s", "--size"},
      description = "Size of the generated chunks (in bytes)",
      defaultValue = "1024")
  private int chunkSize;

  @Option(names = {"-c", "--chunks-per-block"},
      description = "The number of chunks to write per block",
      defaultValue = "16")
  private int chunksPerBlock;

  @Option(names = {"-l", "--layout"},
      description = "Strategy to layout files in the container",
      defaultValue = "FILE_PER_CHUNK"
  )
  private ChunkLayOutVersion chunkLayout;

  private ChunkManager chunkManager;

  private final Map<Integer, KeyValueContainer> containersPerThread =
      new ConcurrentHashMap<>();

  private Timer timer;

  private byte[] data;

  private long blockSize;

  private final ThreadLocal<AtomicLong> bytesWrittenInThread =
      ThreadLocal.withInitial(AtomicLong::new);

  @Override
  public Void call() throws Exception {
    try {
      init();
      OzoneConfiguration ozoneConfiguration = createOzoneConfiguration();

      VolumeSet volumeSet =
          new MutableVolumeSet("dnid", "clusterid", ozoneConfiguration);

      Random random = new Random();

      VolumeChoosingPolicy volumeChoicePolicy =
          new RoundRobinVolumeChoosingPolicy();

      final int threadCount = getThreadNo();

      //create a dedicated (NEW) container for each thread
      for (int i = 1; i <= threadCount; i++) {
        //use a non-negative container id
        long containerId = random.nextLong() & 0x0F_FF_FF_FF_FF_FF_FF_FFL;

        KeyValueContainerData keyValueContainerData =
            new KeyValueContainerData(containerId,
                chunkLayout,
                1_000_000L,
                getPrefix(),
                "nodeid");

        KeyValueContainer keyValueContainer =
            new KeyValueContainer(keyValueContainerData, ozoneConfiguration);

        keyValueContainer.create(volumeSet, volumeChoicePolicy, "scmid");

        containersPerThread.put(i, keyValueContainer);
      }

      blockSize = chunkSize * chunksPerBlock;
      data = randomAscii(chunkSize).getBytes(UTF_8);

      chunkManager = ChunkManagerFactory.createChunkManager(ozoneConfiguration,
          null);

      timer = getMetrics().timer("chunk-write");

      LOG.info("Running chunk write test: threads={} chunkSize={} " +
              "chunksPerBlock={} layout={}",
          threadCount, chunkSize, chunksPerBlock, chunkLayout);

      runTests(this::writeChunk);

    } finally {
      if (chunkManager != null) {
        chunkManager.shutdown();
      }
    }
    return null;
  }

  private void writeChunk(long l) {
    //based on the thread naming convention: pool-N-thread-M
    final int threadID =
        Integer.parseInt(Thread.currentThread().getName().split("-")[3]);
    KeyValueContainer container = containersPerThread.get(threadID);
    final long containerID = container.getContainerData().getContainerID();
    final long bytesWritten = bytesWrittenInThread.get().getAndAdd(chunkSize);
    final long offset = bytesWritten % blockSize;
    final long localID = bytesWritten / blockSize;
    BlockID blockId = new BlockID(containerID, localID);
    String chunkName = getPrefix() + "_chunk_" + l;
    ChunkInfo chunkInfo = new ChunkInfo(chunkName, offset, chunkSize);
    LOG.debug("Writing chunk {}: containerID:{} localID:{} offset:{} " +
            "bytesWritten:{}", l, containerID, localID, offset, bytesWritten);
    DispatcherContext context =
        new DispatcherContext.Builder()
            .setStage(WriteChunkStage.WRITE_DATA)
            .setTerm(1L)
            .setLogIndex(l)
            .setReadFromTmpFile(false)
            .build();
    ByteBuffer buffer = ByteBuffer.wrap(data);

    timer.time(() -> {
      try {
        chunkManager.writeChunk(container, blockId, chunkInfo, buffer, context);
      } catch (StorageContainerException e) {
        throw new UncheckedIOException(e);
      }
    });

  }

}
