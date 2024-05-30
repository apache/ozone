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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.cli.SubcommandWithParent;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.StandaloneReplicationConfig;
import org.apache.hadoop.hdds.protocol.datanode.proto.ContainerProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.LifeCycleState;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationFactor;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ReplicationType;
import org.apache.hadoop.hdds.scm.XceiverClientFactory;
import org.apache.hadoop.hdds.scm.XceiverClientSpi;
import org.apache.hadoop.hdds.scm.cli.ScmOption;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerReplicaInfo;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline;
import org.apache.hadoop.hdds.scm.pipeline.PipelineID;
import org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState;
import org.apache.hadoop.hdds.scm.protocol.StorageContainerLocationProtocol;
import org.apache.hadoop.hdds.scm.storage.ContainerProtocolCalls;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientException;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.rpc.RpcClient;
import org.apache.hadoop.ozone.om.helpers.ListKeysResult;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.shell.Handler;
import org.apache.hadoop.ozone.shell.OzoneAddress;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.util.StringUtils;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

@Command(
    name = "find-missing-padding",
    aliases = {"fmp"},
    description = {"List all keys with any missing padding, optionally limited to a volume/bucket/key URI."}
)
public class FindMissingPadding extends Handler implements SubcommandWithParent {
  @ParentCommand
  private OzoneDebug parent;
  @Mixin
  private ScmOption scmOption;
  @Parameters(
      arity = "0..1",
      description = {"Ozone URI could either be a full URI or short URI.\nFull URI should start with o3://, in case of non-HA\nclusters it should be followed by the host name and\noptionally the port number. In case of HA clusters\nthe service id should be used. Service id provides a\nlogical name for multiple hosts and it is defined\nin the property ozone.om.service.ids.\nExample of a full URI with host name and port number\nfor a key:\no3://omhostname:9862/vol1/bucket1/key1\nWith a service id for a volume:\no3://omserviceid/vol1/\nShort URI should start from the volume.\nExample of a short URI for a bucket:\nvol1/bucket1\nAny unspecified information will be identified from\nthe config files.\n"}
  )
  private String uri;
  private static final int LIST_KEY_SIZE = 10000;
  private static final int BATCH_SIZE = 100000;
  private static final int THREAD_NUM = 10;
  private final ExecutorService executorService = Executors.newFixedThreadPool(10);
  private SecurityConfig securityConfig;
  private boolean tokenEnabled;
  private StorageContainerLocationProtocol scmContainerClient;
  private ScmClient scmClient;
  private RpcClient rpcClient;
  private XceiverClientFactory xceiverClientManager;
  private Pipeline.Builder pipelineBuilder;
  private volatile boolean exception = false;
  private final Map<Long, Map<Long, Set<OmKeyInfo>>> candidateKeys = new HashMap();
  private final Set<OmKeyInfo> affectedKeys = new HashSet();

  public FindMissingPadding() {
  }

  public void init(OzoneClient ozoneClient) throws IOException {
    this.securityConfig = new SecurityConfig(this.getConf());
    this.tokenEnabled = this.securityConfig.isSecurityEnabled() && this.securityConfig.isContainerTokenEnabled();
    this.scmContainerClient = HAUtils.getScmContainerClient(this.getConf());
    this.scmClient = this.scmOption.createScmClient();
    this.rpcClient = (RpcClient)ozoneClient.getProxy();
    this.xceiverClientManager = this.rpcClient.getXceiverClientManager();
    this.pipelineBuilder = Pipeline.newBuilder().setId(PipelineID.randomId()).setState(PipelineState.OPEN).setReplicationConfig(StandaloneReplicationConfig.getInstance(ReplicationFactor.ONE));
  }

  protected OzoneAddress getAddress() throws OzoneClientException {
    return new OzoneAddress(this.uri);
  }

  public Class<?> getParentType() {
    return OzoneDebug.class;
  }

  protected void execute(OzoneClient ozoneClient, OzoneAddress address) throws IOException {
    this.init(ozoneClient);
    this.findCandidateKeys(ozoneClient, address);
    this.handleAffectedKeys();
  }

  private void findCandidateKeys(OzoneClient ozoneClient, OzoneAddress address) throws IOException {
    ObjectStore objectStore = ozoneClient.getObjectStore();
    String volumeName = address.getVolumeName();
    String bucketName = address.getBucketName();
    String keyName = address.getKeyName();
    if (!keyName.isEmpty()) {
      this.checkKey(volumeName, bucketName, keyName);
    } else {
      OzoneVolume volume;
      if (!bucketName.isEmpty()) {
        volume = objectStore.getVolume(volumeName);
        OzoneBucket bucket = volume.getBucket(bucketName);
        this.checkBucket(bucket);
      } else if (!volumeName.isEmpty()) {
        volume = objectStore.getVolume(volumeName);
        this.checkVolume(volume);
      } else {
        Iterator<? extends OzoneVolume> it = objectStore.listVolumes((String)null);

        while(it.hasNext()) {
          this.checkVolume((OzoneVolume)it.next());
        }
      }
    }

  }

  private void checkVolume(OzoneVolume volume) throws IOException {
    Iterator<? extends OzoneBucket> it = volume.listBuckets((String)null);

    while(it.hasNext()) {
      OzoneBucket bucket = (OzoneBucket)it.next();
      this.checkBucket(bucket);
    }

  }

  private void checkBucket(OzoneBucket bucket) throws IOException {
    String volumeName = bucket.getVolumeName();
    String bucketName = bucket.getName();
    List<OmKeyInfo> keyBatch = new ArrayList();
    List<Future<Void>> futures = new ArrayList();
    boolean hasMoreKeys = true;
    String prevKey = "";

    while(hasMoreKeys) {
      ListKeysResult result = this.rpcClient.getOzoneManagerClient().listKeys(volumeName, bucketName, "", prevKey, 10000);
      List<OmKeyInfo> keys = result.getKeys();
      if (keys.size() < 10000) {
        hasMoreKeys = false;
      } else {
        prevKey = ((OmKeyInfo)keys.get(keys.size() - 1)).getKeyName();
      }

      keyBatch.addAll(keys);
      if (keyBatch.size() >= 100000) {
        futures.add(this.executorService.submit(() -> {
          return this.processKeyBatch(volumeName, bucketName, keyBatch);
        }));
        keyBatch = new ArrayList();
      }
    }

    if (!keyBatch.isEmpty()) {
      futures.add(this.executorService.submit(() -> {
        return this.processKeyBatch(volumeName, bucketName, keyBatch);
      }));
    }

    Iterator var13 = futures.iterator();

    while(var13.hasNext()) {
      Future<Void> future = (Future)var13.next();

      try {
        future.get();
      } catch (ExecutionException var11) {
        throw new RuntimeException(var11);
      } catch (InterruptedException var12) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(var12);
      }
    }

  }

  private void checkKey(String volumeName, String bucketName, String keyName) throws IOException {
    OmKeyInfo omKeyInfo = this.rpcClient.getKeyInfo(volumeName, bucketName, keyName, false);
    this.processKeyBatch(volumeName, bucketName, Collections.singletonList(omKeyInfo));
  }

  private void checkECKey(OmKeyInfo omKeyInfo, Map<Long, Map<Long, Set<OmKeyInfo>>> candidateKeyBatch) {
    ECReplicationConfig ecConfig = (ECReplicationConfig)omKeyInfo.getReplicationConfig();
    long sizeThreshold = (long)(ecConfig.getData() - 1) * (long)ecConfig.getEcChunkSize();
    Iterator var6 = omKeyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().iterator();

    while(var6.hasNext()) {
      OmKeyLocationInfo info = (OmKeyLocationInfo)var6.next();
      long size = info.getLength();
      if (size <= sizeThreshold) {
        ((Set)((Map)candidateKeyBatch.computeIfAbsent(info.getContainerID(), (k) -> {
          return new HashMap();
        })).computeIfAbsent(info.getLocalID(), (k) -> {
          return new HashSet();
        })).add(omKeyInfo);
      }
    }

  }

  private static boolean isEC(OzoneKey key) {
    return key.getReplicationConfig().getReplicationType() == ReplicationType.EC;
  }

  private static boolean isEC(OmKeyInfo key) {
    return key.getReplicationConfig().getReplicationType() == ReplicationType.EC;
  }

  private void handleAffectedKeys() {
    if (!this.affectedKeys.isEmpty()) {
      this.out().println(StringUtils.join("\t", Arrays.asList("Key", "Size", "Replication")));
      Iterator var1 = this.affectedKeys.iterator();

      while(var1.hasNext()) {
        OmKeyInfo key = (OmKeyInfo)var1.next();
        this.out().println(StringUtils.join("\t", Arrays.asList(key.getVolumeName() + "/" + key.getBucketName() + "/" + key.getKeyName(), key.getDataSize(), key.getReplicationConfig().getReplication())));
      }
    } else {
      this.out().println("not found");
    }

  }

  private Void processKeyBatch(String volumeName, String bucketName, List<OmKeyInfo> keyBatch) {
    new HashMap();
    Map<Long, Map<Long, Set<OmKeyInfo>>> candidateKeyBatch = new HashMap();
    Iterator var6 = keyBatch.iterator();

    while(true) {
      while(var6.hasNext()) {
        OmKeyInfo omKeyInfo = (OmKeyInfo)var6.next();
        if (isEC(omKeyInfo)) {
          this.checkECKey(omKeyInfo, candidateKeyBatch);
        } else {
          Iterator var8 = omKeyInfo.getLatestVersionLocations().getBlocksLatestVersionOnly().iterator();

          while(var8.hasNext()) {
            OmKeyLocationInfo info = (OmKeyLocationInfo)var8.next();
            ((Set)((Map)candidateKeyBatch.computeIfAbsent(info.getContainerID(), (k) -> {
              return new HashMap();
            })).computeIfAbsent(info.getLocalID(), (k) -> {
              return new HashSet();
            })).add(omKeyInfo);
          }
        }
      }

      try {
        var6 = candidateKeyBatch.keySet().iterator();

        label133:
        while(var6.hasNext()) {
          Long containerID = (Long)var6.next();
          Token<? extends TokenIdentifier> token = this.tokenEnabled ? this.scmContainerClient.getContainerToken(ContainerID.valueOf(containerID)) : null;
          List<ContainerReplicaInfo> containerReplicas = this.scmClient.getContainerReplicas(containerID);
          LOG.debug("Container {} replicas: {}", containerID, containerReplicas.stream().sorted(Comparator.comparing(ContainerReplicaInfo::getReplicaIndex).thenComparing(ContainerReplicaInfo::getState).thenComparing((r) -> {
            return r.getDatanodeDetails().getUuidString();
          })).map((r) -> {
            return "index=" + r.getReplicaIndex() + " keys=" + r.getKeyCount() + " state=" + r.getState() + " dn=" + r.getDatanodeDetails();
          }).collect(Collectors.joining(", ")));
          Iterator var10 = containerReplicas.iterator();

          while(true) {
            while(true) {
              if (!var10.hasNext()) {
                continue label133;
              }

              ContainerReplicaInfo replica = (ContainerReplicaInfo)var10.next();
              if (!LifeCycleState.CLOSED.name().equals(replica.getState())) {
                LOG.trace("Ignore container {} replica {} at {} in {} state", new Object[]{replica.getContainerID(), replica.getReplicaIndex(), replica.getDatanodeDetails(), replica.getState()});
              } else {
                Pipeline pipeline = this.pipelineBuilder.setNodes(Collections.singletonList(replica.getDatanodeDetails())).build();
                XceiverClientSpi datanodeClient = this.xceiverClientManager.acquireClientForReadData(pipeline);

                try {
                  ContainerProtos.HeadBlocksResponseProto missingBlockResponse = ContainerProtocolCalls.headBlocks(datanodeClient, containerID, ((Map)candidateKeyBatch.get(containerID)).keySet(), token);
                  Iterator var15 = missingBlockResponse.getMissingBlockList().iterator();

                  while(var15.hasNext()) {
                    ContainerProtos.MissingBlock missingBlock = (ContainerProtos.MissingBlock)var15.next();
                    LOG.info("Found {} block missing from container {} on replica {} at {}, block on Container DB {}, block on Container disk {}", new Object[]{missingBlock.getLocalID(), containerID, replica.getReplicaIndex(), replica.getDatanodeDetails(), missingBlock.getOnDB(), missingBlock.getOnDisk()});
                    this.affectedKeys.addAll((Collection)((Map)candidateKeyBatch.get(containerID)).get(missingBlock.getLocalID()));
                  }
                } catch (IOException var21) {
                  this.exception = true;
                  LOG.error("123");
                } finally {
                  this.xceiverClientManager.releaseClientForReadData(datanodeClient, false);
                }
              }
            }
          }
        }

        return null;
      } catch (IOException var23) {
        LOG.error("123");
        throw new RuntimeException(var23);
      }
    }
  }
}
