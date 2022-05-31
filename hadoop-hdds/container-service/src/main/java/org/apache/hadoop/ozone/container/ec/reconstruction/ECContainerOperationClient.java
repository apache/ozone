/*
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
package org.apache.hadoop.ozone.container.ec.reconstruction;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * This class wraps necessary container-level rpc calls
 * during ec offline reconstruction.
 *   - ListBlock
 *   - CloseContainer
 */
public class ECContainerOperationClient implements Closeable {

  private final XceiverClientManager xceiverClientManager;

  public ECContainerOperationClient(XceiverClientManager clientManager) {
    this.xceiverClientManager = clientManager;
  }

  public ECContainerOperationClient(ConfigurationSource conf)
      throws IOException {
    this(new XceiverClientManager(conf));
  }

  public BlockData[] listBlock(long containerId, int replicaIndex,
      DatanodeDetails dn, ECReplicationConfig repConfig) {
    return null;
  }

  public void closeContainer(long containerId, int replicaIndex,
      DatanodeDetails dn) {
  }

  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  private Pipeline createSingleNodePipeline(DatanodeDetails dn,
      int replicaIndex, ECReplicationConfig repConfig) {

    Map<DatanodeDetails, Integer> indicesForSinglePipeline = new HashMap<>();
    indicesForSinglePipeline.put(dn, replicaIndex);

    return Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setReplicationConfig(repConfig)
        .setState(Pipeline.PipelineState.OPEN)
        .setNodes(ImmutableList.of(dn))
        .setReplicaIndexes(indicesForSinglePipeline)
        .build();
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
  }
}
