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
package org.apache.hadoop.ozone.debug;

import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.ozone.shell.Shell;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import org.kohsuke.MetaInfServices;
import picocli.CommandLine;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static java.util.Comparator.comparing;

/**
 * Find EC keys affected by missing padding blocks (HDDS-10681).
 */
@CommandLine.Command(name = "find-missing-padding",
    aliases = { "fmp" },
    description = "List all keys with any missing padding, optionally limited to a volume/bucket/key URI.")
@MetaInfServices(SubcommandWithParent.class)
public class FindMissingPadding extends Handler implements SubcommandWithParent {

  @CommandLine.ParentCommand
  private OzoneDebug parent;

  @CommandLine.Mixin
  private ScmOption scmOption;

  @CommandLine.Parameters(arity = "0..1",
      description = Shell.OZONE_URI_DESCRIPTION)
  private String uri;

  /**
   * Keys possibly affected (those with any block under threshold size),
   * grouped by container ID and block (local) ID.
   */
  private final Map<Long, Map<Long, Set<OzoneKey>>> candidateKeys = new HashMap<>();

  private final Set<OzoneKey> affectedKeys = new HashSet<>();

  @Override
  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(uri);
  }

  @Override
  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  @Override
  protected void execute(OzoneClient ozoneClient, OzoneAddress address) throws IOException {
    findCandidateKeys(ozoneClient, address);
    checkContainers(ozoneClient);
    handleAffectedKeys();
  }

  private void findCandidateKeys(OzoneClient ozoneClient, OzoneAddress address) throws IOException {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    ClientProtocol rpcClient = objectStore.getClientProxy();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    if (!keyName.isEmpty()) {
      checkKey(rpcClient, volumeName, bucketName, keyName);
    } else if (!bucketName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      OzoneBucket bucket = volume.getBucket(bucketName);
      checkBucket(bucket, rpcClient);
    } else if (!volumeName.isEmpty()) {
      OzoneVolume volume = objectStore.getVolume(volumeName);
      checkVolume(volume, rpcClient);
    } else {
      for (Iterator<? extends OzoneVolume> it = objectStore.listVolumes(null); it.hasNext();) {
        checkVolume(it.next(), rpcClient);
      }
    }
  }

  private void checkVolume(OzoneVolume volume, ClientProtocol rpcClient) throws IOException {
    for (Iterator<? extends OzoneBucket> it = volume.listBuckets(null); it.hasNext();) {
      OzoneBucket bucket = it.next();
      checkBucket(bucket, rpcClient);
    }
  }

  private void checkBucket(OzoneBucket bucket, ClientProtocol rpcClient) throws IOException {
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    for (Iterator<? extends OzoneKey> it = bucket.listKeys(null); it.hasNext();) {
      OzoneKey key = it.next();
      if (isEC(key)) {
        checkKey(rpcClient, volumeName, bucketName, key.getName());
      } else {
        LOG.trace("Key {}/{}/{} is not EC", volumeName, bucketName, key.getName());
      }
    }
  }

  private void checkKey(ClientProtocol rpcClient, String volumeName, String bucketName, String keyName)
      throws IOException {
    OzoneKeyDetails keyDetails = rpcClient.getKeyDetails(volumeName, bucketName, keyName);
    if (isEC(keyDetails)) {
      checkECKey(keyDetails);
    }
  }

  private void checkECKey(OzoneKeyDetails keyDetails) {
    List<OzoneKeyLocation> locations = keyDetails.getOzoneKeyLocations();
    if (!locations.isEmpty()) {
      ECReplicationConfig ecConfig = (ECReplicationConfig) keyDetails.getReplicationConfig();
      long sizeThreshold = (ecConfig.getData() - 1) * (long) ecConfig.getEcChunkSize();
      for (OzoneKeyLocation loc : locations) {
        long size = loc.getLength();
        if (size <= sizeThreshold) {
          candidateKeys.computeIfAbsent(loc.getContainerID(), k -> new HashMap<>())
              .computeIfAbsent(loc.getLocalID(), k -> new HashSet<>())
              .add(keyDetails);
        }
      }
    } else {
      LOG.trace("Key {}/{}/{} has no locations",
          keyDetails.getVolumeName(), keyDetails.getBucketName(), keyDetails.getName());
    }
  }

  private static boolean isEC(OzoneKey key) {
    return key.getReplicationConfig().getReplicationType() == HddsProtos.ReplicationType.EC;
  }

  private void checkContainers(OzoneClient ozoneClient) throws IOException {
    if (candidateKeys.isEmpty()) {
      return;
    }

    SecurityConfig securityConfig = new SecurityConfig(getConf());
    final boolean tokenEnabled = securityConfig.isSecurityEnabled() && securityConfig.isContainerTokenEnabled();
    StorageContainerLocationProtocol scmContainerClient = HAUtils.getScmContainerClient(getConf());
    RpcClient rpcClient = (RpcClient) ozoneClient.getProxy();
    XceiverClientFactory xceiverClientManager = rpcClient.getXceiverClientManager();
    Pipeline.Builder pipelineBuilder = Pipeline.newBuilder()
        .setId(PipelineID.randomId())
        .setState(Pipeline.PipelineState.OPEN)
        .setReplicationConfig(StandaloneReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE));

    try (ScmClient scmClient = scmOption.createScmClient()) {
      for (Map.Entry<Long, Map<Long, Set<OzoneKey>>> entry : candidateKeys.entrySet()) {
        long containerID = entry.getKey();
        Map<Long, Set<OzoneKey>> blockToKeysMap = entry.getValue();

        ContainerInfo container = scmClient.getContainer(containerID);
        if (container.getState() != HddsProtos.LifeCycleState.CLOSED) {
          LOG.trace("Skip container {} as it is not CLOSED, rather {}", containerID, container.getState());
          continue;
        }

        Token<? extends TokenIdentifier> token = tokenEnabled
            ? scmContainerClient.getContainerToken(ContainerID.valueOf(containerID))
            : null;

        List<ContainerReplicaInfo> containerReplicas = scmClient.getContainerReplicas(containerID);

        LOG.debug("Container {} replicas: {}", containerID, containerReplicas.stream()
            .sorted(comparing(ContainerReplicaInfo::getReplicaIndex)
                .thenComparing(ContainerReplicaInfo::getState)
                .thenComparing(r -> r.getDatanodeDetails().getUuidString()))
            .map(r -> "index=" + r.getReplicaIndex() + " keys=" + r.getKeyCount()
                + " state=" + r.getState() + " dn=" + r.getDatanodeDetails())
            .collect(Collectors.joining(", "))
        );

        for (ContainerReplicaInfo replica : containerReplicas) {
          if (!HddsProtos.LifeCycleState.CLOSED.name().equals(replica.getState())) {
            LOG.trace("Ignore container {} replica {} at {} in {} state",
                replica.getContainerID(), replica.getReplicaIndex(), replica.getDatanodeDetails(), replica.getState());
            continue;
          }

          final Set<Long> missingBlocks = new HashSet<>(blockToKeysMap.keySet());
          Pipeline pipeline = pipelineBuilder
              .setNodes(Collections.singletonList(replica.getDatanodeDetails()))
              .build();
          XceiverClientSpi datanodeClient = xceiverClientManager.acquireClientForReadData(pipeline);
          try {
            ContainerProtos.ListBlockResponseProto listBlockResponse = ContainerProtocolCalls.listBlock(
                datanodeClient, containerID, null, Integer.MAX_VALUE, token);
            for (ContainerProtos.BlockData blockData : listBlockResponse.getBlockDataList()) {
              missingBlocks.remove(blockData.getBlockID().getLocalID());
            }
            if (missingBlocks.isEmpty()) {
              LOG.debug("All {} blocks in container {} found on replica {} at {}",
                  blockToKeysMap.keySet().size(), containerID, replica.getReplicaIndex(), replica.getDatanodeDetails());
            } else {
              LOG.info("Found {} blocks missing from container {} on replica {} at {}",
                  missingBlocks.size(), containerID, replica.getReplicaIndex(), replica.getDatanodeDetails());
              missingBlocks.forEach(b -> affectedKeys.addAll(blockToKeysMap.getOrDefault(b, emptySet())));
            }
          } finally {
            xceiverClientManager.releaseClientForReadData(datanodeClient, false);
          }
        }
      }
    }
  }

  private void handleAffectedKeys() {
    if (!affectedKeys.isEmpty()) {
      out().println(StringUtils.join("\t", Arrays.asList(
          "Key", "Size", "Replication"
      )));
      for (OzoneKey key : affectedKeys) {
        out().println(StringUtils.join("\t", Arrays.asList(
            key.getVolumeName() + "/" + key.getBucketName() + "/" + key.getName(),
            key.getDataSize(),
            key.getReplicationConfig().getReplication()
        )));
      }
    }
  }
}
