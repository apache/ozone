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

package org.apache.hadoop.ozone.client.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockInputStream;
import org.apache.hadoop.hdds.scm.storage.BlockLocationInfo;
import org.apache.hadoop.hdds.scm.storage.StreamBlockInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

/**
 * Tests for BlockInputStreamFactoryImpl.
 */
public class TestBlockInputStreamFactoryImpl {

  private OzoneConfiguration conf = new OzoneConfiguration();

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testNonECGivesBlockInputStream(boolean streamReadBlockEnabled) throws IOException {
    BlockInputStreamFactory factory = new BlockInputStreamFactoryImpl();
    ReplicationConfig repConfig =
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);

    BlockLocationInfo blockInfo = createKeyLocationInfo(repConfig, 3,
        1024 * 1024 * 10);
    Pipeline pipeline = Mockito.spy(blockInfo.getPipeline());
    blockInfo.setPipeline(pipeline);
    Mockito.when(pipeline.getReplicaIndex(any(DatanodeDetails.class))).thenReturn(1);
    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    clientConfig.setStreamReadBlock(streamReadBlockEnabled);
    BlockExtendedInputStream stream =
        factory.create(repConfig, blockInfo, blockInfo.getPipeline(),
            blockInfo.getToken(), null, null,
            clientConfig);
    if (streamReadBlockEnabled) {
      assertInstanceOf(StreamBlockInputStream.class, stream);
    } else {
      assertInstanceOf(BlockInputStream.class, stream);
    }
    assertEquals(stream.getBlockID(), blockInfo.getBlockID());
    assertEquals(stream.getLength(), blockInfo.getLength());
  }

  @Test
  public void testECGivesECBlockInputStream() throws IOException {
    BlockInputStreamFactory factory = new BlockInputStreamFactoryImpl();
    ReplicationConfig repConfig =
        new ECReplicationConfig(3, 2);

    BlockLocationInfo blockInfo =
        createKeyLocationInfo(repConfig, 5, 1024 * 1024 * 10);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    BlockExtendedInputStream stream =
        factory.create(repConfig, blockInfo, blockInfo.getPipeline(),
            blockInfo.getToken(), null, null,
            clientConfig);
    assertInstanceOf(ECBlockInputStreamProxy.class, stream);
    assertEquals(stream.getBlockID(), blockInfo.getBlockID());
    assertEquals(stream.getLength(), blockInfo.getLength());
  }

  private BlockLocationInfo createKeyLocationInfo(ReplicationConfig repConf,
      long blockLength, Map<DatanodeDetails, Integer> dnMap) {

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setReplicationConfig(repConf)
        .build();

    BlockLocationInfo keyInfo = new BlockLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setLength(blockLength)
        .setOffset(0)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build();
    return keyInfo;
  }

  private BlockLocationInfo createKeyLocationInfo(ReplicationConfig repConf,
      int nodeCount, long blockLength) {
    Map<DatanodeDetails, Integer> datanodes = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      datanodes.put(MockDatanodeDetails.randomDatanodeDetails(), i + 1);
    }
    return createKeyLocationInfo(repConf, blockLength, datanodes);
  }

}
