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
package org.apache.hadoop.hdds.scm.pipeline;

import static org.apache.hadoop.hdds.protocol.proto.StorageContainerDatanodeProtocolProtos.StorageTypeProto.SSD;

import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.StorageUtils;
import org.apache.hadoop.hdds.scm.node.NodeManager;

/**
 * Filter out datanodes with available SSD volumes.
 */
public class SsdStorageAwarePipelinePlacementPolicy
    extends PipelinePlacementPolicy {

  /**
   * Constructs a pipeline placement with considering network topology,
   * load balancing and rack awareness.
   *
   * @param nodeManager  NodeManager
   * @param stateManager PipelineStateManagerImpl
   * @param conf         Configuration
   */
  public SsdStorageAwarePipelinePlacementPolicy(
      NodeManager nodeManager, PipelineStateManager stateManager,
      ConfigurationSource conf) {
    super(nodeManager, stateManager, conf);
  }

  @Override
  protected boolean hasEnoughSpace(DatanodeDetails datanodeDetails,
                                   long metadataSizeRequired,
                                   long dataSizeRequired) {
    return StorageUtils.hasEnoughSpace(datanodeDetails,
        dataVolume -> dataVolume.getStorageType().equals(SSD), null,
        metadataSizeRequired, dataSizeRequired);
  }
}
