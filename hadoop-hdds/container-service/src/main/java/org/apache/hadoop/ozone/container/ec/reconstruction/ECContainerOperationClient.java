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
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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
    this(new XceiverClientManager(conf,
        new XceiverClientManager.XceiverClientManagerConfigBuilder()
            .setMaxCacheSize(256).setStaleThresholdMs(10 * 1000).build(),
        null));
  }

  public BlockData[] listBlock(long containerId, DatanodeDetails dn,
      ECReplicationConfig repConfig, Token<? extends TokenIdentifier> token)
      throws IOException {
    List<ContainerProtos.BlockData> blockDataList = ContainerProtocolCalls
        .listBlock(this.xceiverClientManager.acquireClient(
            Pipeline.newBuilder().setId(PipelineID.randomId())
                .setReplicationConfig(repConfig).setNodes(ImmutableList.of(dn))
                .setState(Pipeline.PipelineState.CLOSED).build()), containerId,
            null, Integer.MAX_VALUE, token).getBlockDataList();
    return blockDataList.stream().map(i -> {
      try {
        return BlockData.getFromProtoBuf(i);
      } catch (IOException e) {
        // TODO: revisit here.
        return null;
      }
    }).collect(Collectors.toList())
        .toArray(new BlockData[blockDataList.size()]);
  }

  public void closeContainer(XceiverClientSpi client, long containerID,
      String encodedToken) throws IOException {
    ContainerProtocolCalls.closeContainer(client, containerID, null);
    client.close();
  }

  public XceiverClientManager getXceiverClientManager() {
    return xceiverClientManager;
  }

  @Override
  public void close() throws IOException {
    if (xceiverClientManager != null) {
      xceiverClientManager.close();
    }
  }
}