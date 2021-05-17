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
package org.apache.hadoop.ozone.client;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Port;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UUID;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;

import java.util.ArrayList;
import java.util.List;

/**
 * Allocate incremental blocks in a single one node pipeline.
 */
public class SinglePipelineBlockAllocator
    implements MockBlockAllocator {

  private long blockId;
  private Pipeline pipeline;

  public SinglePipelineBlockAllocator() {

  }

  @Override
  public Iterable<? extends KeyLocation> allocateBlock(
      KeyArgs keyArgs) {

    if (pipeline == null) {
      pipeline = Pipeline.newBuilder()
          .setFactor(keyArgs.getFactor())
          .setType(keyArgs.getType())
          .setId(PipelineID.newBuilder()
              .setUuid128(UUID.newBuilder()
                  .setLeastSigBits(1L)
                  .setMostSigBits(1L)
                  .build())
              .build())
          .addMembers(DatanodeDetailsProto.newBuilder()
              .setUuid128(UUID.newBuilder()
                  .setLeastSigBits(1L)
                  .setMostSigBits(1L)
                  .build())
              .setHostName("localhost")
              .setIpAddress("1.2.3.4")
              .addPorts(Port.newBuilder()
                  .setName("RATIS")
                  .setValue(1234)
                  .build())
              .build())
          .build();
    }

    List<KeyLocation> results = new ArrayList<>();
    results.add(KeyLocation.newBuilder()
        .setPipeline(pipeline)
        .setBlockID(BlockID.newBuilder()
            .setBlockCommitSequenceId(1L)
            .setContainerBlockID(ContainerBlockID.newBuilder()
                .setContainerID(1L)
                .setLocalID(blockId++)
                .build())
            .build())
        .setOffset(0L)
        .setLength(keyArgs.getDataSize())
        .build());
    return results;
  }
}
