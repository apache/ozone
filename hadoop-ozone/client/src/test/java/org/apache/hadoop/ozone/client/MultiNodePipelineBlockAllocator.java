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
import java.util.Set;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;

/**
 * Allocates the block with required number of nodes in the pipeline.
 * The nodes are pre-created with port numbers starting from 0 to
 * ( given cluster size -1).
 */
public class MultiNodePipelineBlockAllocator implements MockBlockAllocator {
  private long blockId;
  private int requiredNodes;
  private final ConfigurationSource conf;
  private List<HddsProtos.DatanodeDetailsProto> clusterDns = new ArrayList<>();
  private int start = 0;

  public MultiNodePipelineBlockAllocator(OzoneConfiguration conf,
      int requiredNodes, int clusterSize) {
    this.requiredNodes = requiredNodes;
    this.conf = conf;
    // Pre-initializing the datanodes. Later allocateBlock API will use this
    // nodes to add the required number of nodes in the block pipelines.
    for (int i = 0; i < clusterSize; i++) {
      clusterDns.add(HddsProtos.DatanodeDetailsProto.newBuilder().setUuid128(
          HddsProtos.UUID.newBuilder().setLeastSigBits(i).setMostSigBits(i)
              .build()).setHostName("localhost").setIpAddress("1.2.3.4")
          .addPorts(HddsProtos.Port.newBuilder().setName("RATIS").setValue(i)
              .build()).build());
    }
  }

  public List<HddsProtos.DatanodeDetailsProto> getClusterDns() {
    return this.clusterDns;
  }

  /**
   * This method selects the block pipeline nodes from the pre-created cluster
   * nodes(clusterDns). It will use requiredNodes field to decide how many nodes
   * to be chosen for the pipeline. To make the tests easy prediction of the
   * node allocations, it will choose block pipeline nodes in a sliding window
   * fashion starting from 0th index in clusterDns in incrementing order until
   * given requireNodes number. Similarly for the next block pipeline, it will
   * start from the index location of previous chosen pipeline's last node index
   * + 1. Let's say cluster size was initialized with 10 and required nodes are
   * 5, the first block pipeline will have nodes from 0 to 4 and the second
   * block will be assigned with the index locations of 5th to 9th nodes. Once
   * we finish round of allocations, then it will start from 0 again for next
   * block. It will also support exclude list. If client passes exclude list, it
   * will simply skip the node if it presents in exclude list, instead it will
   * simply take the next node. If not enough nodes left due to the grown
   * exclude list, it will throw IllegalStateException.
   *
   * @param keyArgs
   * @param excludeList
   * @return KeyLocation
   */
  @Override
  public Iterable<? extends OzoneManagerProtocolProtos.KeyLocation>
      allocateBlock(OzoneManagerProtocolProtos.KeyArgs keyArgs,
      ExcludeList excludeList) {
    HddsProtos.Pipeline.Builder builder =
        HddsProtos.Pipeline.newBuilder().setFactor(keyArgs.getFactor())
            .setType(keyArgs.getType()).setId(HddsProtos.PipelineID.newBuilder()
            .setUuid128(HddsProtos.UUID.newBuilder().setLeastSigBits(1L)
                .setMostSigBits(1L).build()).build());
    addMembers(builder, requiredNodes, excludeList.getDatanodes(), keyArgs);
    if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
      builder.setEcReplicationConfig(keyArgs.getEcReplicationConfig());
    }
    final HddsProtos.Pipeline pipeline = builder.build();
    List<OzoneManagerProtocolProtos.KeyLocation> results = new ArrayList<>();
    long blockSize = (long) conf
        .getStorageSize(OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE,
            OzoneConfigKeys.OZONE_SCM_BLOCK_SIZE_DEFAULT, StorageUnit.BYTES);
    results.add(OzoneManagerProtocolProtos.KeyLocation.newBuilder()
        .setPipeline(pipeline).setBlockID(
            HddsProtos.BlockID.newBuilder().setBlockCommitSequenceId(1L)
                .setContainerBlockID(
                    HddsProtos.ContainerBlockID.newBuilder().setContainerID(1L)
                        .setLocalID(blockId++).build()).build()).setOffset(0L)
        .setLength(blockSize).build());
    return results;
  }

  private void addMembers(HddsProtos.Pipeline.Builder builder, int nodesNeeded,
      Set<DatanodeDetails> excludedDataNodes,
      OzoneManagerProtocolProtos.KeyArgs keyArgs) {
    int clusterSize = clusterDns.size();
    int counter = nodesNeeded;
    int j = 0;

    for (int i = 0; i < clusterSize; i++) {
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto =
          clusterDns.get(start % clusterSize);
      start++;
      if (excludedDataNodes
          .contains(DatanodeDetails.getFromProtoBuf(datanodeDetailsProto))) {
        continue;
      } else {
        builder.addMembers(datanodeDetailsProto);
        if (keyArgs.getType() == HddsProtos.ReplicationType.EC) {
          builder.addMemberReplicaIndexes(++j);
        }
        if (--counter == 0) {
          break;
        }
      }
    }
    if (counter > 0) {
      throw new IllegalStateException(
          "MockedImpl: Could not find enough nodes.");
    }
  }

}
