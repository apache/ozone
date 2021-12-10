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
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

import java.util.ArrayList;
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
  private List<HddsProtos.DatanodeDetailsProto> clusterDns = new ArrayList<>();
  private int start = 0;

  public MultiNodePipelineBlockAllocator(OzoneConfiguration conf,
      int requiredNodes, int clusterSize) {
    this.requiredNodes = requiredNodes;
    this.conf = conf;
    for (int i = 0; i < clusterSize; i++) {
      clusterDns.add(HddsProtos.DatanodeDetailsProto.newBuilder().setUuid128(
          HddsProtos.UUID.newBuilder().setLeastSigBits(i).setMostSigBits(i)
              .build()).setHostName("localhost").setIpAddress("1.2.3.4")
          .addPorts(HddsProtos.Port.newBuilder().setName("RATIS").setValue(i)
              .build()).build());
    }
  }

  public List<HddsProtos.DatanodeDetailsProto> getClusterDns(){
    return this.clusterDns;
  }

  @Override
  public Iterable<? extends OzoneManagerProtocolProtos.KeyLocation>
      allocateBlock(OzoneManagerProtocolProtos.KeyArgs keyArgs,
      ExcludeList excludeList) {
    long blockSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
            OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    long blockGroupLen = keyArgs.getEcReplicationConfig().getData() * blockSize;
    long dataSize = keyArgs.getDataSize();
    List<OzoneManagerProtocolProtos.KeyLocation> results = new ArrayList<>();
    long numbBlkGroups = dataSize / blockGroupLen + 1;
    for (int i = 0; i < numbBlkGroups; i++) {
      HddsProtos.Pipeline.Builder builder =
          HddsProtos.Pipeline.newBuilder().setFactor(keyArgs.getFactor())
              .setType(keyArgs.getType()).setId(
              HddsProtos.PipelineID.newBuilder().setUuid128(
                  HddsProtos.UUID.newBuilder().setLeastSigBits(1L)
                      .setMostSigBits(1L).build()).build());
      addMembers(builder, requiredNodes, excludeList.getDatanodes(), keyArgs);
      if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
        builder.setEcReplicationConfig(keyArgs.getEcReplicationConfig());
      }
      final HddsProtos.Pipeline pipeline = builder.build();
      results.add(OzoneManagerProtocolProtos.KeyLocation.newBuilder()
          .setPipeline(pipeline).setBlockID(
              HddsProtos.BlockID.newBuilder().setBlockCommitSequenceId(1L)
                  .setContainerBlockID(HddsProtos.ContainerBlockID.newBuilder()
                      .setContainerID(1L).setLocalID(blockId++).build())
                  .build()).setOffset(0L).setLength(blockSize).build());
    }
    return results;
  }

  private void addMembers(HddsProtos.Pipeline.Builder builder,
      int nodesNeeded, Set<DatanodeDetails> datanodes,
      OzoneManagerProtocolProtos.KeyArgs keyArgs) {
    int clusterSize = clusterDns.size();
    int counter = nodesNeeded;
    int j = 1;
    for (int i = 0; i < clusterDns.size(); i++) {
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto =
          clusterDns.get(start % clusterSize);
      start++;
      if (DatanodeDetails.getFromProtoBuf(datanodeDetailsProto)
          .equals(datanodes)) {
        continue;
      } else {
        builder.addMembers(datanodeDetailsProto);
        if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
          builder.addMemberReplicaIndexes(j++);
        }
        counter--;
        if (counter == 0) {
          break;
        }
      }
    }
    if (j - 1 == counter) {
      throw new IllegalStateException(
          "MockedImpl: Could not find enough nodes.");
    }
  }

}
