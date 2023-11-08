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
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.OzoneClientConfig;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerClientProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.THREE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Test KeyOutputStream with RATIS keys.
 */
public class TestKeyOutputStream {

  private static String testKeyString = "test";

  @Test
  public void testRATISKeyOutputStreamExpiryTime() throws Exception {
    KeyOutputStream keyOutputStream =
        createRATISKeyOutputStream();
    byte[] data = testKeyString.getBytes(UTF_8);
    keyOutputStream.write(data);

    Assert.assertEquals(3,
        keyOutputStream.getExcludeList().getDatanodes().size());

    ExcludeList excludeList = spy(keyOutputStream.getExcludeList());
    when(excludeList.getExpiryTime()).thenReturn(300 * 1000L);
    doReturn(true).when(excludeList)
        .isExpired(anyLong()); // mock DN in exclude list expires
    keyOutputStream.getBlockOutputStreamEntryPool().setExcludeList(excludeList);
    Assert.assertEquals(0,
        keyOutputStream.getExcludeList().getDatanodes().size());
  }

  private KeyOutputStream createRATISKeyOutputStream() throws Exception {
    OpenKeySession openKeySession = mock(OpenKeySession.class);
    OmKeyInfo omKeyInfo =  new OmKeyInfo.Builder()
        .setVolumeName("testvolume")
        .setBucketName("testbucket")
        .setKeyName("testKey")
        .build();
    when(openKeySession.getKeyInfo()).thenReturn(omKeyInfo);

    XceiverClientFactory xceiverClientManager
        = mock(XceiverClientFactory.class);

    OzoneManagerClientProtocol ozoneManagerClientProtocol
        = mock(OzoneManagerClientProtocol.class);

    OzoneClientConfig clientConfig = spy(new OzoneClientConfig());
    when(clientConfig.getStreamBufferSize()).thenReturn(1);
    when(clientConfig.getExcludeNodesExpiryTime()).thenReturn(300 * 1000L);

    KeyOutputStream.Builder builder;

    ReplicationConfig repConfig =
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.THREE);
    builder = new KeyOutputStream.Builder()
        .setReplicationConfig(repConfig);

    KeyOutputStream keyOutputStream =
        spy(builder.setHandler(openKeySession)
            .setXceiverClientManager(xceiverClientManager)
            .setOmClient(ozoneManagerClientProtocol)
            .setConfig(clientConfig)
            .build());

    BlockOutputStreamEntryPool blockOutputStreamEntryPool
        = spy(keyOutputStream.getBlockOutputStreamEntryPool());
    BlockOutputStreamEntry blockOutputStreamEntry
        = mock(BlockOutputStreamEntry.class);
    Pipeline pipeline = new Pipeline.Builder()
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(RatisReplicationConfig.getInstance(THREE))
        .setId(PipelineID.randomId())
        .setNodes(new ArrayList<>())
        .build();

    doThrow(IOException.class).when(blockOutputStreamEntry)
        .write(any(byte[].class), anyInt(), anyInt());
    when(blockOutputStreamEntry.getPipeline()).thenReturn(pipeline);
    when(blockOutputStreamEntry.getBlockID()).thenReturn(new BlockID(1, 1));

    // mock the datanodes for getFailedServers()
    List<DatanodeDetails> datanodeDetails = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      datanodeDetails.add(
          DatanodeDetails.getFromProtoBuf(
              HddsProtos.DatanodeDetailsProto.newBuilder().setUuid128(
                  HddsProtos.UUID.newBuilder()
                      .setLeastSigBits(i).setMostSigBits(i)
                      .build()).setHostName("localhost").setIpAddress("0.0.0.0")
              .addPorts(HddsProtos.Port.newBuilder().setName("test").setValue(i)
                  .build()).build())
      );
    }

    doReturn(datanodeDetails).when(blockOutputStreamEntry).getFailedServers();
    doReturn(blockOutputStreamEntry)
        .when(blockOutputStreamEntryPool).allocateBlockIfNeeded();
    doReturn(Long.valueOf(testKeyString.length()))
        .when(blockOutputStreamEntryPool).getKeyLength();
    keyOutputStream.setBlockOutputStreamEntryPool(blockOutputStreamEntryPool);
    doReturn(testKeyString.length())
        .when(keyOutputStream)
        .getCurrentBlockOutputStreamDataWritten(any(BlockOutputStreamEntry.class), anyLong());
    return keyOutputStream;
  }

}
