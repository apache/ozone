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

import static org.apache.hadoop.ozone.OzoneConsts.MB;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.BlockID;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.BlockExtendedInputStream;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfoGroup;
import org.junit.jupiter.api.Test;

/**
 * Test KeyInputStream with EC keys.
 */
public class TestKeyInputStreamEC {

  private OzoneConfiguration conf = new OzoneConfiguration();

  @Test
  public void testReadAgainstLargeBlockGroup() throws IOException {
    int dataBlocks = 10;
    int parityBlocks = 4;
    ECReplicationConfig ec10And4RepConfig = new ECReplicationConfig(dataBlocks,
        parityBlocks, ECReplicationConfig.EcCodec.RS, (int)(1 * MB));
    // default blockSize of 256MB with EC 10+4 makes a large block group
    long blockSize = 256 * MB;
    long blockLength = dataBlocks * blockSize;
    OmKeyInfo keyInfo = createOmKeyInfo(ec10And4RepConfig,
        dataBlocks + parityBlocks, blockLength);

    BlockExtendedInputStream blockInputStream =
        new ECStreamTestUtil.TestBlockInputStream(new BlockID(1, 1),
        blockLength, ByteBuffer.allocate(100));

    BlockInputStreamFactory mockStreamFactory =
        mock(BlockInputStreamFactory.class);
    when(mockStreamFactory.create(any(), any(), any(), any(),
        any(), any(), any())).thenReturn(blockInputStream);

    OzoneClientConfig clientConfig = conf.getObject(OzoneClientConfig.class);
    clientConfig.setChecksumVerify(true);
    try (LengthInputStream kis = KeyInputStream.getFromOmKeyInfo(keyInfo,
        null,  null, mockStreamFactory,
        clientConfig)) {
      byte[] buf = new byte[100];
      int readBytes = kis.read(buf, 0, 100);
      assertEquals(100, readBytes);
    }
  }

  private OmKeyInfo createOmKeyInfo(ReplicationConfig repConf,
      int nodeCount, long blockLength) {
    Map<DatanodeDetails, Integer> dnMap = new HashMap<>();
    for (int i = 0; i < nodeCount; i++) {
      dnMap.put(MockDatanodeDetails.randomDatanodeDetails(), i + 1);
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

    List<OmKeyLocationInfo> locations = new ArrayList<>();
    locations.add(blockInfo);
    return new OmKeyInfo.Builder()
        .setBucketName("bucket")
        .setVolumeName("volume")
        .setDataSize(blockLength)
        .setKeyName("someKey")
        .setReplicationConfig(repConf)
        .addOmKeyLocationInfoGroup(new OmKeyLocationInfoGroup(0, locations))
        .build();
  }
}
