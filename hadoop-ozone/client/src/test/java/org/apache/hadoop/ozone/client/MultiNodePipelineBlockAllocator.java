/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.ArrayList;
import java.util.List;

/**
 * Allocates the block with required number of nodes in the pipeline.
 */
public class MultiNodePipelineBlockAllocator implements MockBlockAllocator {
  private long blockId;
  private HddsProtos.Pipeline pipeline;
  private int requiredNodes;

  public MultiNodePipelineBlockAllocator(int requiredNodes) {
    this.requiredNodes = requiredNodes;
  }

  @Override
  public Iterable<? extends OzoneManagerProtocolProtos.KeyLocation>
      allocateBlock(OzoneManagerProtocolProtos.KeyArgs keyArgs) {
    if (pipeline == null) {
      HddsProtos.Pipeline.Builder builder =
          HddsProtos.Pipeline.newBuilder().setFactor(keyArgs.getFactor())
              .setType(keyArgs.getType()).setId(
              HddsProtos.PipelineID.newBuilder().setUuid128(
                  HddsProtos.UUID.newBuilder().setLeastSigBits(1L)
                      .setMostSigBits(1L).build()).build());

      for (int i = 1; i <= requiredNodes; i++) {
        builder.addMembers(HddsProtos.DatanodeDetailsProto.newBuilder()
            .setUuid128(HddsProtos.UUID.newBuilder().setLeastSigBits(i)
                .setMostSigBits(i).build()).setHostName("localhost")
            .setIpAddress("1.2.3.4").addPorts(
                HddsProtos.Port.newBuilder().setName("RATIS").setValue(1234 + i)
                    .build()).build());
      }
      pipeline = builder.build();
    }

    List<OzoneManagerProtocolProtos.KeyLocation> results = new ArrayList<>();
    results.add(OzoneManagerProtocolProtos.KeyLocation.newBuilder()
        .setPipeline(pipeline).setBlockID(
            HddsProtos.BlockID.newBuilder().setBlockCommitSequenceId(1L)
                .setContainerBlockID(
                    HddsProtos.ContainerBlockID.newBuilder().setContainerID(1L)
                        .setLocalID(blockId++).build()).build()).setOffset(0L)
        .setLength(keyArgs.getDataSize()).build());
    return results;
  }
}
