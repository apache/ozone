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

package org.apache.hadoop.ozone.debug.replicas;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;

/**
 * Verifies the state of a replica from the DN.
 * [DELETED, UNHEALTHY, INVALID] are considered bad states.
 */
public class ContainerStateVerifier implements ReplicaVerifier {
  private static final String CHECK_TYPE = "containerState";
  private static final long DEFAULT_CONTAINER_CACHE_SIZE = 1000000;
  private final ContainerOperationClient containerOperationClient;
  private final XceiverClientManager xceiverClientManager;
  // cache for container info and encodedToken from the SCM
  private final Cache<Long, ContainerInfoToken> encodedTokenCache;

  public ContainerStateVerifier(OzoneConfiguration conf, long containerCacheSize) throws IOException {
    containerOperationClient = new ContainerOperationClient(conf);
    xceiverClientManager = containerOperationClient.getXceiverClientManager();

    if (containerCacheSize < 1) {
      System.err.println("Invalid cache size provided: " + containerCacheSize +
              ". Falling back to default: " + DEFAULT_CONTAINER_CACHE_SIZE);
      containerCacheSize = DEFAULT_CONTAINER_CACHE_SIZE;
    }
    encodedTokenCache = CacheBuilder.newBuilder().maximumSize(containerCacheSize).build();
  }

  @Override
  public String getType() {
    return CHECK_TYPE;
  }

  @Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OmKeyLocationInfo keyLocation) {
    try {
      StringBuilder replicaCheckMsg = new StringBuilder().append("Replica state is ");
      boolean pass = false;

      ContainerInfoToken containerInfoToken = getContainerInfoToken(keyLocation.getContainerID());
      ContainerDataProto containerData = fetchContainerDataFromDatanode(datanode, keyLocation.getContainerID(),
          keyLocation, containerInfoToken);

      if (containerData == null) {
        return BlockVerificationResult.failIncomplete("No container data returned from DN.");
      }
      ContainerDataProto.State state = containerData.getState();
      replicaCheckMsg.append(state.name());
      if (areContainerAndReplicasInGoodState(state, containerInfoToken.getContainerState())) {
        pass = true;
      }
      replicaCheckMsg.append(", Container state in SCM is ").append(containerInfoToken.getContainerState());

      if (pass) {
        return BlockVerificationResult.pass();
      } else {
        return BlockVerificationResult.failCheck(replicaCheckMsg.toString());
      }
    } catch (IOException e) {
      if (e.getMessage().contains("ContainerID") && e.getMessage().contains("does not exist")) {
        // if container "does not exist", mark it as failed instead of incomplete
        return BlockVerificationResult.failCheck(e.getMessage());
      }
      return BlockVerificationResult.failIncomplete(e.getMessage());
    }
  }

  private boolean areContainerAndReplicasInGoodState(ContainerDataProto.State replicaState,
      HddsProtos.LifeCycleState containerState) {
    return (replicaState != ContainerDataProto.State.UNHEALTHY &&
        replicaState != ContainerDataProto.State.INVALID &&
        replicaState != ContainerDataProto.State.DELETED &&
        containerState != HddsProtos.LifeCycleState.DELETING &&
        containerState != HddsProtos.LifeCycleState.DELETED);
  }

  private ContainerDataProto fetchContainerDataFromDatanode(DatanodeDetails dn, long containerId,
                                                            OmKeyLocationInfo keyLocation,
                                                            ContainerInfoToken containerInfoToken)
      throws IOException {
    XceiverClientSpi client = null;
    ReadContainerResponseProto response;
    try {
      Pipeline pipeline = keyLocation.getPipeline().copyForReadFromNode(dn);
      String encodedToken = containerInfoToken.getEncodedToken();

      client = xceiverClientManager.acquireClientForReadData(pipeline);
      response = ContainerProtocolCalls
          .readContainer(client, containerId, encodedToken);
    } finally {
      if (client != null) {
        xceiverClientManager.releaseClient(client, false);
      }
    }

    if (!response.hasContainerData()) {
      return null;
    }
    return response.getContainerData();
  }

  private ContainerInfoToken getContainerInfoToken(long containerId)
      throws IOException {
    ContainerInfoToken cachedData = encodedTokenCache.getIfPresent(containerId);
    if (cachedData != null) {
      return cachedData;
    }
    // Cache miss - fetch and store
    ContainerInfo info = containerOperationClient.getContainer(containerId);
    String encodeToken = containerOperationClient.getEncodedContainerToken(containerId);
    cachedData = new ContainerInfoToken(info.getState(), encodeToken);
    encodedTokenCache.put(containerId, cachedData);
    return cachedData;
  }

  private static class ContainerInfoToken {
    private HddsProtos.LifeCycleState state;
    private final String encodedToken;

    ContainerInfoToken(HddsProtos.LifeCycleState lifeState, String token) {
      this.state = lifeState;
      this.encodedToken = token;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ContainerInfoToken)) {
        return false;
      }
      ContainerInfoToken key = (ContainerInfoToken) o;
      return Objects.equals(state, key.state) &&
          Objects.equals(encodedToken, key.encodedToken);
    }

    @Override
    public int hashCode() {
      return Objects.hash(state, encodedToken);
    }

    public HddsProtos.LifeCycleState getContainerState() {
      return state;
    }

    public String getEncodedToken() {
      return encodedToken;
    }
  }

}
