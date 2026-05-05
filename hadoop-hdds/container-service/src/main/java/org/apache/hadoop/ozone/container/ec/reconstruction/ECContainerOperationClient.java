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

package org.apache.hadoop.ozone.container.ec.reconstruction;

import com.google.common.collect.ImmutableList;
import jakarta.annotation.Nonnull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto.State;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.client.ClientTrustManager;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.ozone.OzoneSecurityUtil;
import org.apache.hadoop.ozone.container.common.helpers.BlockData;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class wraps necessary container-level rpc calls
 * during ec offline reconstruction.
 *   - ListBlock
 *   - CloseContainer
 */
public class ECContainerOperationClient implements Closeable {
  private static final Logger LOG =
      LoggerFactory.getLogger(ECContainerOperationClient.class);
  private final XceiverClientManager xceiverClientManager;

  public ECContainerOperationClient(XceiverClientManager clientManager) {
    this.xceiverClientManager = clientManager;
  }

  public ECContainerOperationClient(ConfigurationSource conf,
      CertificateClient certificateClient) throws IOException {
    this(createClientManager(conf, certificateClient));
  }

  @Nonnull
  private static XceiverClientManager createClientManager(ConfigurationSource conf, CertificateClient certificateClient)
      throws IOException {
    ClientTrustManager trustManager = null;
    if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
      trustManager = certificateClient.createClientTrustManager();
    }
    XceiverClientManager.ScmClientConfig scmClientConfig = new XceiverClientManager.XceiverClientManagerConfigBuilder()
        .setMaxCacheSize(256)
        .setStaleThresholdMs(10 * 1000)
        .build();
    return new XceiverClientManager(conf, scmClientConfig, trustManager);
  }

  public BlockData[] listBlock(long containerId, DatanodeDetails dn,
      ECReplicationConfig repConfig, Token<? extends TokenIdentifier> token)
      throws IOException {
    XceiverClientSpi xceiverClient = this.xceiverClientManager
        .acquireClient(singleNodePipeline(dn, repConfig));
    try {
      List<ContainerProtos.BlockData> blockDataList = ContainerProtocolCalls
          .listBlock(xceiverClient, containerId, null, Integer.MAX_VALUE, token)
          .getBlockDataList();
      return blockDataList.stream().map(i -> {
        try {
          return BlockData.getFromProtoBuf(i);
        } catch (IOException e) {
          LOG.debug("Failed while converting to protobuf BlockData. Returning null for listBlock from DN: {}", dn, e);
          // TODO: revisit here.
          return null;
        }
      }).toArray(BlockData[]::new);
    } finally {
      this.xceiverClientManager.releaseClient(xceiverClient, false);
    }
  }

  public void closeContainer(long containerID, DatanodeDetails dn,
      ECReplicationConfig repConfig, String encodedToken) throws IOException {
    XceiverClientSpi xceiverClient = this.xceiverClientManager
        .acquireClient(singleNodePipeline(dn, repConfig));
    try {
      ContainerProtocolCalls
          .closeContainer(xceiverClient, containerID, encodedToken);
    } finally {
      this.xceiverClientManager.releaseClient(xceiverClient, false);
    }
  }

  /**
   * Deletes the container at the given DN. Before deleting it will pre-check
   * whether the container is in RECOVERING state. As this is not an atomic
   * check for RECOVERING container, there is a chance non recovering containers
   * could get deleted if they just created in the window time of RECOVERING
   * container exist check and delete op. So, user of the API needs to keep this
   * scenario in mind and use this API if it is still safe.
   *
   * TODO: Alternatively we can extend this API to pass the flag to perform the
   *       check at server side. So, that it will become atomic op.
   * @param containerID - Container ID.
   * @param dn - Datanode details.
   * @param repConfig - Replication config.
   * @param encodedToken - Token
   */
  public void deleteContainerInState(long containerID, DatanodeDetails dn,
         ECReplicationConfig repConfig, String encodedToken,
         Set<State> acceptableStates) throws IOException {
    XceiverClientSpi xceiverClient = this.xceiverClientManager
        .acquireClient(singleNodePipeline(dn, repConfig));
    try {
      // Before deleting the recovering container, just make sure that state is
      // Recovering & Unhealthy. There will be still race condition,
      // but this will avoid most usual case.
      ContainerProtos.ReadContainerResponseProto readContainerResponseProto =
          ContainerProtocolCalls
              .readContainer(xceiverClient, containerID, encodedToken);
      State currentState =
              readContainerResponseProto.getContainerData().getState();
      if (!Objects.isNull(acceptableStates)
              && acceptableStates.contains(currentState)) {
        ContainerProtocolCalls.deleteContainer(xceiverClient, containerID,
                        true, encodedToken);
      } else {
        LOG.warn("Container {} will not be deleted as current state " +
                "not in acceptable states. Current state: {}, " +
                "Acceptable States: {}", containerID, currentState,
                acceptableStates);
      }
    } finally {
      this.xceiverClientManager.releaseClient(xceiverClient, false);
    }
  }

  public void createRecoveringContainer(long containerID, DatanodeDetails dn,
      ECReplicationConfig repConfig, String encodedToken, int replicaIndex)
      throws IOException {
    XceiverClientSpi xceiverClient = this.xceiverClientManager.acquireClient(
        singleNodePipeline(dn, repConfig));
    try {
      ContainerProtocolCalls
          .createRecoveringContainer(xceiverClient, containerID, encodedToken,
              replicaIndex);
    } finally {
      this.xceiverClientManager.releaseClient(xceiverClient, false);
    }
  }

  Pipeline singleNodePipeline(DatanodeDetails dn,
      ECReplicationConfig repConfig) {
    return singleNodePipeline(dn, repConfig, 0);
  }

  Pipeline singleNodePipeline(DatanodeDetails dn,
       ECReplicationConfig repConfig, int replicaIndex) {
    // To get the same client from cache, we try to use the DN UUID as
    // pipelineID for uniqueness. Please note, pipeline does not have any
    // significance after it's close. So, we are ok to use any ID.
    return Pipeline.newBuilder().setId(dn.getID())
            .setReplicationConfig(repConfig).setNodes(ImmutableList.of(dn))
            .setState(Pipeline.PipelineState.CLOSED)
            .setReplicaIndexes(Collections.singletonMap(dn, replicaIndex)).build();
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
