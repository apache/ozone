package org.apache.hadoop.ozone.debug.replicas;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ReadContainerResponseProto;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos.ContainerDataProto;
import org.apache.hadoop.hdds.scm.XceiverClientManager;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ContainerOperationClient;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor.ONE;

/**
 * Verifies the state of a replica from the DN.
 * [DELETED, UNHEALTHY, INVALID] are considered bad states.
 */
public class ReplicaStateVerifier implements ReplicaVerifier {
  private final ContainerOperationClient containerOperationClient;
  private final XceiverClientManager xceiverClientManager;
  private final Cache<ReplicaKey, ContainerDataProto> containerDataCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .build();
  private final Cache<Long, ContainerInfoToken> encodedTokenCache = CacheBuilder.newBuilder()
      .maximumSize(1000)
      .build();

  public ReplicaStateVerifier(OzoneConfiguration conf) throws IOException {
    containerOperationClient = new ContainerOperationClient(conf);
    xceiverClientManager = containerOperationClient.getXceiverClientManager();
  }
  @Override
  public void verifyKey(OzoneKeyDetails keyDetails) {

  }

  //@Override
  public BlockVerificationResult verifyBlock(DatanodeDetails datanode, OmKeyLocationInfo keyLocation,
                                             int replicaIndex) {
    try {
      StringBuilder replicaCheckMsg = new StringBuilder().append("Replica state is ");
      boolean pass = false;

      ContainerInfoToken containerInfoToken = getContainerInfoToken(keyLocation.getContainerID());
      ContainerInfo containerInfo = containerInfoToken.getContainerInfo();

      ContainerDataProto containerData = getContainerData(datanode, keyLocation, replicaIndex, containerInfoToken);
      if (containerData == null) {
        return BlockVerificationResult.failCheck("ContainerData is missing in the datanode.");
      }
      if (containerData.getState().equals(ContainerDataProto.State.UNHEALTHY)) {
        replicaCheckMsg.append("UNHEALTHY");
      } else if (containerData.getState().equals(ContainerDataProto.State.INVALID)) {
        replicaCheckMsg.append("INVALID");
      } else if (containerData.getState().equals(ContainerDataProto.State.DELETED)) {
        replicaCheckMsg.append("DELETED");
      } else {
        pass = true;
      }
      replicaCheckMsg.append(", Container state in SCM is ").append(containerInfo.getState());

      if (pass) {
        return BlockVerificationResult.pass();
      } else {
        return BlockVerificationResult.failCheck(replicaCheckMsg.toString());
      }
    } catch (IOException e) {
      return BlockVerificationResult.failIncomplete(e.getMessage());
    }
  }

  public ContainerDataProto getContainerData(DatanodeDetails dn, OmKeyLocationInfo keyLocation,
                                             int replicaIndex, ContainerInfoToken containerInfoToken)
      throws IOException {
    long containerId = containerInfoToken.getContainerInfo().getContainerID();
    ReplicaKey key = new ReplicaKey(dn, containerId);
    ContainerDataProto cachedData = containerDataCache.getIfPresent(key);
    if (cachedData != null) {
      return cachedData;
    }
    // Cache miss - fetch and store
    ContainerDataProto data = fetchContainerDataFromDatanode(dn, containerId, keyLocation,
        replicaIndex, containerInfoToken);
    containerDataCache.put(key, data);
    return data;
  }

  private ContainerDataProto fetchContainerDataFromDatanode(DatanodeDetails dn, long containerId,
                                                            OmKeyLocationInfo keyLocation, int replicaIndex,
                                                            ContainerInfoToken containerInfoToken)
      throws IOException {
    XceiverClientSpi client = null;
    ReadContainerResponseProto response;
    try {
      Pipeline pipeline = Pipeline.newBuilder(keyLocation.getPipeline())
          .setReplicationConfig(StandaloneReplicationConfig.getInstance(ONE))
          .setNodes(Collections.singletonList(dn))
          .setReplicaIndexes(Collections.singletonMap(dn, replicaIndex))
          .build();
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

  public ContainerInfoToken getContainerInfoToken(long containerId)
      throws IOException {
    ContainerInfoToken cachedData = encodedTokenCache.getIfPresent(containerId);
    if (cachedData != null) {
      return cachedData;
    }
    // Cache miss - fetch and store
    ContainerInfo info = containerOperationClient.getContainer(containerId);
    String encodeToken = containerOperationClient.getEncodedContainerToken(containerId);
    cachedData = new ContainerInfoToken(info, encodeToken);
    encodedTokenCache.put(containerId, cachedData);
    return cachedData;
  }


  private class ReplicaKey {
    private final DatanodeDetails datanode;
    private final long containerID;

    ReplicaKey(DatanodeDetails datanode, long containerID) {
      this.datanode = datanode;
      this.containerID = containerID;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ReplicaKey)) {
        return false;
      }
      ReplicaKey key = (ReplicaKey) o;
      return Objects.equals(datanode, key.datanode) &&
          Objects.equals(containerID, key.containerID);
    }

    @Override
    public int hashCode() {
      return Objects.hash(datanode, containerID);
    }
  }

  private class ContainerInfoToken {
    private final ContainerInfo containerInfo;
    private final String encodedToken;

    ContainerInfoToken(ContainerInfo info, String token) {
      this.containerInfo = info;
      this.encodedToken = token;
    }

    @Override
    public int hashCode() {
      return Objects.hash(containerInfo, encodedToken);
    }

    public ContainerInfo getContainerInfo() {
      return containerInfo;
    }

    public String getEncodedToken() {
      return encodedToken;
    }
  }

}
