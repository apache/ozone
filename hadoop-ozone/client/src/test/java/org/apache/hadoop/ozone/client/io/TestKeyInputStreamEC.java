/**
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
package org.apache.hadoop.ozone.client.io;

import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test KeyInputStream with underlying ECBlockInputStream.
 */
public class TestKeyInputStreamEC {

  @Test
  public void testReadAgainstLargeBlockGroup() throws IOException {
    int dataBlocks = 10;
    int parityBlocks = 4;
    ECReplicationConfig ec10And4RepConfig = new ECReplicationConfig(dataBlocks,
        parityBlocks, ECReplicationConfig.EcCodec.RS, (int)(1 * MB));
    // default blockSize of 256MB with EC 10+4 makes a large block group
    long blockSize = 256 * MB;
    OmKeyLocationInfo blockInfo = createBlockInfo(ec10And4RepConfig,
        dataBlocks + parityBlocks, dataBlocks * blockSize);

    ECBlockInputStream ecBlockInputStream = new ECBlockInputStream(
        ec10And4RepConfig, blockInfo, false, null, null, null);

    ECBlockInputStreamFactory mockStreamFactory =
        mock(ECBlockInputStreamFactory.class);
    when(mockStreamFactory.create(anyBoolean(), anyList(), any(), any(),
        anyBoolean(), any(), any())).thenReturn(ecBlockInputStream);

    try (KeyInputStream kis = new KeyInputStream()) {
      ECBlockInputStreamProxy streamProxy = new ECBlockInputStreamProxy(
          ec10And4RepConfig, blockInfo, true, null, null, mockStreamFactory);
      ECBlockInputStreamProxy spyEcBlockInputStream = spy(streamProxy);
      byte[] buf = new byte[100];
      // spy the read(ByteBuffer) method is ok since issue HDDS-6319
      // happens in read(byte[], off, len)
      doReturn(buf.length).when(spyEcBlockInputStream)
          .read(any(ByteBuffer.class));

      kis.addStream(spyEcBlockInputStream);

      int readBytes = kis.read(buf, 0, 100);
      Assert.assertEquals(100, readBytes);
    }
  }

  private OmKeyLocationInfo createBlockInfo(ReplicationConfig repConf,
      int nodeCount, long blockLength) {
    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      dnMap.put(randomDatanodeDetails(), i + 1);
    }

    Pipeline pipeline = Pipeline.newBuilder()
        .setState(Pipeline.PipelineState.CLOSED)
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>(dnMap.keySet()))
        .setReplicaIndexes(dnMap)
        .setReplicationConfig(repConf)
        .build();

    OmKeyLocationInfo blockInfo = new OmKeyLocationInfo.Builder()
        .setBlockID(new BlockID(1, 1))
        .setLength(blockLength)
        .setOffset(0)
        .setPipeline(pipeline)
        .setPartNumber(0)
        .build();
    return blockInfo;
  }

  private DatanodeDetails randomDatanodeDetails() {
    String ipAddress = "127.0.0.1";
    DatanodeDetails.Port containerPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.STANDALONE, 0);
    DatanodeDetails.Port ratisPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.RATIS, 0);
    DatanodeDetails.Port restPort = DatanodeDetails.newPort(
        DatanodeDetails.Port.Name.REST, 0);
    DatanodeDetails.Builder builder = DatanodeDetails.newBuilder();
    builder.setUuid(UUID.randomUUID())
        .setHostName("localhost")
        .setIpAddress(ipAddress)
        .addPort(containerPort)
        .addPort(ratisPort)
        .addPort(restPort);
    return builder.build();
  }
}
