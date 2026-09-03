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

package org.apache.hadoop.hdds.scm.storage;

import static org.apache.hadoop.hdds.scm.storage.PositionedReadTestHelper.SOURCE_SIZE;
import static org.apache.hadoop.hdds.scm.storage.TestChunkInputStream.generateRandomData;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.google.common.primitives.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ContainerBlockID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChecksumType;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ChunkInfo;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.MockPipeline;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.ozone.common.Checksum;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link MultipartInputStream}'s functionality.
 */
public class TestMultipartInputStream {

  private static final int PART_SIZE = SOURCE_SIZE / 2;

  @Test
  public void testPositionedReadPartialAtEof() throws Exception {
    int fileLen = 1024;
    byte[] partData = generateRandomData(fileLen);
    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    Function<BlockID, BlockLocationInfo> refreshFunction = mock(Function.class);
    Checksum checksum = new Checksum(ChecksumType.NONE, 1024);

    BlockInputStream part = createBlockStream(new BlockID(new ContainerBlockID(1, 1)),
        partData, pipeline, refreshFunction, clientConfig, checksum);
    try (MultipartInputStream multipartStream =
        new MultipartInputStream("test-key", Collections.singletonList(part))) {
      multipartStream.initialize();
      int position = 12;
      int expectedBytes = fileLen - position;
      ByteBuffer buffer = ByteBuffer.allocate(expectedBytes * 2);
      assertTrue(multipartStream.readFully(position, buffer));
      assertEquals(expectedBytes, buffer.position());
    }
  }

  @Test
  public void testConcurrentPositionedRead() throws Exception {
    byte[] part0Data = generateRandomData(PART_SIZE);
    byte[] part1Data = generateRandomData(PART_SIZE);
    byte[] keyData = Bytes.concat(part0Data, part1Data);

    OzoneConfiguration conf = new OzoneConfiguration();
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(false);
    Pipeline pipeline = MockPipeline.createSingleNodePipeline();
    Function<BlockID, BlockLocationInfo> refreshFunction = mock(Function.class);
    Checksum checksum = new Checksum(ChecksumType.NONE, 1024);

    BlockInputStream part0 = createBlockStream(new BlockID(new ContainerBlockID(1, 1)),
        part0Data, pipeline, refreshFunction, clientConfig, checksum);
    BlockInputStream part1 = createBlockStream(new BlockID(new ContainerBlockID(1, 2)),
        part1Data, pipeline, refreshFunction, clientConfig, checksum);

    List<BlockInputStream> parts = new ArrayList<>();
    parts.add(part0);
    parts.add(part1);
    try (MultipartInputStream multipartStream = new MultipartInputStream("test-key", parts)) {
      multipartStream.initialize();
      PositionedReadTestHelper.runConcurrentPositionedReads(keyData,
          (offset, buf) -> {
            if (!multipartStream.readFully(offset, buf)) {
              throw new AssertionError("stateless readFully returned false at " + offset);
            }
          });
    }
  }

  private static BlockInputStream createBlockStream(BlockID blockID, byte[] blockData,
      Pipeline pipeline, Function<BlockID, BlockLocationInfo> refreshFunction,
      OzoneClientConfig clientConfig, Checksum checksum) throws Exception {
    List<ChunkInfo> chunks = new ArrayList<>(1);
    Map<String, byte[]> chunkDataMap = new HashMap<>();
    String chunkName = "chunk-" + blockID.getLocalID();
    ChunkInfo chunkInfo = ChunkInfo.newBuilder()
        .setChunkName(chunkName)
        .setOffset(0)
        .setLen(blockData.length)
        .setChecksumData(checksum.computeChecksum(
            blockData, 0, blockData.length).getProtoBufMessage())
        .build();
    chunkDataMap.put(chunkName, blockData);
    chunks.add(chunkInfo);
    return new DummyBlockInputStream(blockID, blockData.length, pipeline, null,
        null, refreshFunction, chunks, chunkDataMap, clientConfig);
  }
}
