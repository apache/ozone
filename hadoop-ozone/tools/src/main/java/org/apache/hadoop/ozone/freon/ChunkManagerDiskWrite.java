/**
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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.hadoop.ozone.container.common.helpers.ChunkInfo;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext;
import org.apache.hadoop.ozone.container.common.transport.server.ratis.DispatcherContext.WriteChunkStage;
import org.apache.hadoop.ozone.container.common.volume.RoundRobinVolumeChoosingPolicy;
import org.apache.hadoop.ozone.container.common.volume.VolumeSet;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainer;
import org.apache.hadoop.ozone.container.keyvalue.KeyValueContainerData;
import org.apache.hadoop.ozone.container.keyvalue.impl.ChunkManagerImpl;
import org.apache.hadoop.ozone.container.keyvalue.interfaces.ChunkManager;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

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

  private ChunkManager chunkManager;

  private byte[] data;

  private OzoneConfiguration ozoneConfiguration;

  private Map<Integer, KeyValueContainer> containersPerThread = new HashMap<>();

  private Timer timer;

  @Override
  public Void call() throws Exception {
    try {
      init();
      ozoneConfiguration = createOzoneConfiguration();

      VolumeSet volumeSet =
          new VolumeSet("dnid", "clusterid", ozoneConfiguration);

      Random random = new Random();


      //create a dedicated (NEW) container for each thread
      for (int i = 1; i <= getThreadNo(); i++) {

        //use a non-negative container id
        long containerId = random.nextLong() & 0x0F_FF_FF_FF_FF_FF_FF_FFL;

        KeyValueContainerData keyValueContainerData =
            new KeyValueContainerData(Math.abs(containerId),
                1_000_000L,
                getPrefix(),
                "nodeid");

        KeyValueContainer keyValueContainer =
            new KeyValueContainer(keyValueContainerData, ozoneConfiguration);

        keyValueContainer
            .create(volumeSet, new RoundRobinVolumeChoosingPolicy(), "scmid");

        containersPerThread.put(i, keyValueContainer);
      }

      data = RandomStringUtils.randomAscii(chunkSize)
          .getBytes(StandardCharsets.UTF_8);

      chunkManager = new ChunkManagerImpl(false);

      timer = getMetrics().timer("chunk-write");

      runTests(this::writeChunk);

    } finally {
      if (chunkManager != null) {
        chunkManager.shutdown();
      }
    }
    return null;
  }

  private void writeChunk(long l) throws Exception {
    //based on the thread naming convention: pool-1-thread-n
    int threadNo =
        Integer.parseInt(Thread.currentThread().getName().split("-")[3]);

    KeyValueContainer container = containersPerThread.get(threadNo);

    Preconditions.checkNotNull(container,
        "Container is not created for thread " + threadNo);

    BlockID blockId = new BlockID(l % 10, l);
    ChunkInfo chunkInfo = new ChunkInfo("chunk" + l, 0, chunkSize);
    ByteBuffer byteBuffer = ByteBuffer.wrap(data);
    DispatcherContext context =
        new DispatcherContext.Builder()
            .setStage(WriteChunkStage.WRITE_DATA)
            .setTerm(1L)
            .setLogIndex(l)
            .setReadFromTmpFile(false)
            .build();

    timer.time(() -> {
      try {

        chunkManager
            .writeChunk(container, blockId, chunkInfo,
                byteBuffer,
                context);

      } catch (StorageContainerException e) {
        throw new RuntimeException(e);
      }
      return null;
    });

  }

}
