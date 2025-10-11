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

package org.apache.hadoop.ozone.client;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.BlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ContainerBlockID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DatanodeDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Pipeline;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.PipelineID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.Port;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.UUID;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;

/**
 * Allocate incremental blocks in a single one node pipeline.
 */
public class SinglePipelineBlockAllocator
    implements MockBlockAllocator {

  private long blockId;
  private Pipeline pipeline;
  private OzoneConfiguration conf;

  public SinglePipelineBlockAllocator(OzoneConfiguration conf) {
    this.conf = conf;
  }

  @Override
  public Iterable<? extends KeyLocation> allocateBlock(KeyArgs keyArgs,
      ExcludeList excludeList) {

    if (pipeline == null) {
      Pipeline.Builder bldr = Pipeline.newBuilder()
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
              .build());
      if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
        bldr.setEcReplicationConfig(keyArgs.getEcReplicationConfig());
      }
      pipeline = bldr.build();
    }

    long blockSize =  (long)conf.getStorageSize(
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
        OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT,
        StorageUnit.BYTES);

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
        .setLength(blockSize)
        .build());
    return results;
  }
}
