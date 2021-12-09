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

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * Allocates the block with required number of nodes in the pipeline.
 */
public class MultiNodePipelineBlockAllocator implements MockBlockAllocator {
  public static final Random RANDOM = new Random();
  private long blockId;
  private int requiredNodes;
  private final ConfigurationSource conf;

  public MultiNodePipelineBlockAllocator(OzoneConfiguration conf,
      int requiredNodes) {
    this.requiredNodes = requiredNodes;
    this.conf = conf;
  }

  @Override
  public Iterable<? extends OzoneManagerProtocolProtos.KeyLocation>
      allocateBlock(OzoneManagerProtocolProtos.KeyArgs keyArgs) {
    HddsProtos.Pipeline.Builder builder =
        HddsProtos.Pipeline.newBuilder().setFactor(keyArgs.getFactor())
            .setType(keyArgs.getType()).setId(HddsProtos.PipelineID.newBuilder()
            .setUuid128(HddsProtos.UUID.newBuilder().setLeastSigBits(1L)
                .setMostSigBits(1L).build()).build());
    Set<Integer> usedPorts = new HashSet<>();
    int rand = RANDOM.nextInt(); // used for port and UUID combination.
    // It's ok here for port number limit as don't really create any socket
    // connection, but it has to be unique.
    while (!usedPorts.contains(rand)) {
      rand = RANDOM.nextInt();
    }
    usedPorts.add(rand);
    for (int i = 1; i <= requiredNodes; i++) {
      builder.addMembers(HddsProtos.DatanodeDetailsProto.newBuilder()
          .setUuid128(HddsProtos.UUID.newBuilder().setLeastSigBits(rand)
              .setMostSigBits(i).build()).setHostName("localhost")
          .setIpAddress("1.2.3.4").addPorts(
              HddsProtos.Port.newBuilder().setName("RATIS").setValue(rand)
                  .build()).build());
      if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
        builder.addMemberReplicaIndexes(i);
      }
    }
    if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
      builder.setEcReplicationConfig(keyArgs.getEcReplicationConfig());
    }
    final HddsProtos.Pipeline pipeline = builder.build();

    long blockSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
            OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);

    List<OzoneManagerProtocolProtos.KeyLocation> results = new ArrayList<>();
    results.add(OzoneManagerProtocolProtos.KeyLocation.newBuilder()
        .setPipeline(pipeline).setBlockID(
            HddsProtos.BlockID.newBuilder().setBlockCommitSequenceId(1L)
                .setContainerBlockID(
                    HddsProtos.ContainerBlockID.newBuilder().setContainerID(1L)
                        .setLocalID(blockId++).build()).build()).setOffset(0L)
        .setLength(blockSize).build());
    return results;
  }
}
