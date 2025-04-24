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

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.om.protocolPB.OmTransport;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocationList;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListOpenFilesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListOpenFilesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse.Builder;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OM transport for testing with in-memory state.
 */
public class MockOmTransport implements OmTransport {
  private static final Logger LOG =
      LoggerFactory.getLogger(MockOmTransport.class);

  private final MockBlockAllocator blockAllocator;
  //volumename -> volumeinfo
  private Map<String, VolumeInfo> volumes = new HashMap<>();
  //volumename -> bucketname -> bucketInfo
  private Map<String, Map<String, BucketInfo>> buckets = new HashMap<>();
  //volumename -> bucketname -> keyName -> keys
  private Map<String, Map<String, Map<String, KeyInfo>>> openKeys =
      new HashMap<>();
  //volumename -> bucketname -> keyName -> keys
  private Map<String, Map<String, Map<String, KeyInfo>>> keys =
      new HashMap<>();

  public MockOmTransport(MockBlockAllocator allocator) {
    this.blockAllocator = allocator;
  }

  public MockOmTransport() {
    this(new SinglePipelineBlockAllocator(new OzoneConfiguration()));
  }

  @Override
  public OMResponse submitRequest(OMRequest payload) throws IOException {
    switch (payload.getCmdType()) {
    case CreateVolume:
      return response(payload,
          r -> r.setCreateVolumeResponse(
              createVolume(payload.getCreateVolumeRequest())));
    case InfoVolume:
      return response(payload,
          r -> r.setInfoVolumeResponse(
              infoVolume(payload.getInfoVolumeRequest())));
    case DeleteVolume:
      return response(payload,
          r -> r.setDeleteVolumeResponse(
              deleteVolume(payload.getDeleteVolumeRequest())));
    case CreateBucket:
      return response(payload,
          r -> r.setCreateBucketResponse(
              createBucket(payload.getCreateBucketRequest())));
    case InfoBucket:
      return response(payload,
          r -> r.setInfoBucketResponse(
              infoBucket(payload.getInfoBucketRequest())));
    case CreateKey:
      return response(payload,
          r -> r.setCreateKeyResponse(
              createKey(payload.getCreateKeyRequest())));
    case CommitKey:
      return response(payload,
          r -> r.setCommitKeyResponse(
              commitKey(payload.getCommitKeyRequest())));
    case LookupKey:
      return response(payload,
          r -> r.setLookupKeyResponse(
              lookupKey(payload.getLookupKeyRequest())));
    case ServiceList:
      return response(payload,
          r -> r.setServiceListResponse(
              serviceList(payload.getServiceListRequest())));
    case ListOpenFiles:
      return response(payload,
          r -> r.setListOpenFilesResponse(
              listOpenFiles(payload.getListOpenFilesRequest())));
    case AllocateBlock:
      return response(payload, r -> r.setAllocateBlockResponse(
          allocateBlock(payload.getAllocateBlockRequest())));
    case GetKeyInfo:
      return response(payload, r -> r.setGetKeyInfoResponse(
          getKeyInfo(payload.getGetKeyInfoRequest())));
    default:
      throw new IllegalArgumentException(
          "Mock version of om call " + payload.getCmdType()
              + " is not yet implemented");
    }
  }

  private OzoneManagerProtocolProtos.AllocateBlockResponse allocateBlock(
      OzoneManagerProtocolProtos.AllocateBlockRequest allocateBlockRequest) {
    Iterator<? extends OzoneManagerProtocolProtos.KeyLocation> iterator =
        blockAllocator.allocateBlock(allocateBlockRequest.getKeyArgs(),
            ExcludeList.getFromProtoBuf(allocateBlockRequest.getExcludeList()))
            .iterator();
    OzoneManagerProtocolProtos.AllocateBlockResponse.Builder builder =
        OzoneManagerProtocolProtos.AllocateBlockResponse.newBuilder()
            .setKeyLocation(iterator.next());
    while (iterator.hasNext()) {
      builder.mergeKeyLocation(iterator.next());
    }
    return builder.build();

  }

  private DeleteVolumeResponse deleteVolume(
      DeleteVolumeRequest deleteVolumeRequest) {
    volumes.remove(deleteVolumeRequest.getVolumeName());
    return DeleteVolumeResponse.newBuilder()
        .build();
  }

  private LookupKeyResponse lookupKey(LookupKeyRequest lookupKeyRequest) {
    final KeyArgs keyArgs = lookupKeyRequest.getKeyArgs();
    return LookupKeyResponse.newBuilder()
        .setKeyInfo(
            keys.get(keyArgs.getVolumeName()).get(keyArgs.getBucketName())
                .get(keyArgs.getKeyName()))
        .build();
  }

  private GetKeyInfoResponse getKeyInfo(GetKeyInfoRequest request) {
    final KeyArgs keyArgs = request.getKeyArgs();
    return GetKeyInfoResponse.newBuilder()
        .setKeyInfo(
            keys.get(keyArgs.getVolumeName()).get(keyArgs.getBucketName())
                .get(keyArgs.getKeyName()))
        .build();
  }

  private boolean isHSync(CommitKeyRequest commitKeyRequest) {
    return commitKeyRequest.hasHsync() && commitKeyRequest.getHsync();
  }

  private boolean isRecovery(CommitKeyRequest commitKeyRequest) {
    return commitKeyRequest.hasRecovery() && commitKeyRequest.getRecovery();
  }

  private String toOperationString(CommitKeyRequest commitKeyRequest) {
    boolean hsync = isHSync(commitKeyRequest);
    boolean recovery = isRecovery(commitKeyRequest);
    if (hsync) {
      return "hsync";
    }
    if (recovery) {
      return "recover";
    }
    return "commit";
  }

  private CommitKeyResponse commitKey(CommitKeyRequest commitKeyRequest) {
    final KeyArgs keyArgs = commitKeyRequest.getKeyArgs();
    final KeyInfo openKey =
        openKeys.get(keyArgs.getVolumeName()).get(keyArgs.getBucketName())
            .get(keyArgs.getKeyName());
    LOG.debug("{} open key vol: {} bucket: {} key: {}",
        toOperationString(commitKeyRequest),
        keyArgs.getVolumeName(),
        keyArgs.getBucketName(),
        keyArgs.getKeyName());
    boolean hsync = isHSync(commitKeyRequest);
    if (!hsync) {
      KeyInfo deleteKey = openKeys.get(keyArgs.getVolumeName())
          .get(keyArgs.getBucketName())
          .remove(keyArgs.getKeyName());
      assert deleteKey != null;
    }
    final KeyInfo.Builder committedKeyInfoWithLocations =
        KeyInfo.newBuilder().setVolumeName(keyArgs.getVolumeName())
            .setBucketName(keyArgs.getBucketName())
            .setKeyName(keyArgs.getKeyName())
            .setCreationTime(openKey.getCreationTime())
            .setModificationTime(openKey.getModificationTime())
            .setDataSize(keyArgs.getDataSize()).setLatestVersion(0L)
            .addKeyLocationList(KeyLocationList.newBuilder()
                .addAllKeyLocations(keyArgs.getKeyLocationsList()));
    // Just inherit replication config details from open Key
    if (openKey.hasEcReplicationConfig()) {
      committedKeyInfoWithLocations
          .setEcReplicationConfig(openKey.getEcReplicationConfig());
    } else if (openKey.hasFactor()) {
      committedKeyInfoWithLocations.setFactor(openKey.getFactor());
    }
    committedKeyInfoWithLocations.setType(openKey.getType());
    keys.get(keyArgs.getVolumeName()).get(keyArgs.getBucketName())
        .put(keyArgs.getKeyName(), committedKeyInfoWithLocations.build());
    return CommitKeyResponse.newBuilder()
        .build();
  }

  private CreateKeyResponse createKey(CreateKeyRequest createKeyRequest) {
    final KeyArgs keyArgs = createKeyRequest.getKeyArgs();
    final long now = System.currentTimeMillis();
    final BucketInfo bucketInfo =
        buckets.get(keyArgs.getVolumeName()).get(keyArgs.getBucketName());

    final KeyInfo.Builder keyInfoBuilder =
        KeyInfo.newBuilder().setVolumeName(keyArgs.getVolumeName())
            .setBucketName(keyArgs.getBucketName())
            .setKeyName(keyArgs.getKeyName()).setCreationTime(now)
            .setModificationTime(now).setDataSize(keyArgs.getDataSize())
            .setLatestVersion(0L).addKeyLocationList(
            KeyLocationList.newBuilder().addAllKeyLocations(
                blockAllocator.allocateBlock(createKeyRequest.getKeyArgs(),
                    new ExcludeList()))
                .build());

    if (keyArgs.getType() == HddsProtos.ReplicationType.NONE) {
      // 1. Client did not pass replication config.
      // Now lets try bucket defaults
      if (bucketInfo.getDefaultReplicationConfig() != null) {
        // Since Bucket defaults are available, let's inherit
        final HddsProtos.ReplicationType type =
            bucketInfo.getDefaultReplicationConfig().getType();
        keyInfoBuilder
            .setType(bucketInfo.getDefaultReplicationConfig().getType());
        switch (type) {
        case EC:
          keyInfoBuilder.setEcReplicationConfig(
              bucketInfo.getDefaultReplicationConfig()
                  .getEcReplicationConfig());
          break;
        case RATIS:
        case STAND_ALONE:
          keyInfoBuilder
              .setFactor(bucketInfo.getDefaultReplicationConfig().getFactor());
          break;
        default:
          throw new UnsupportedOperationException(
              "Unknown replication type: " + type);
        }
      } else {
        keyInfoBuilder.setType(HddsProtos.ReplicationType.RATIS);
        keyInfoBuilder.setFactor(HddsProtos.ReplicationFactor.THREE);
      }
    } else {
      // 1. Client passed the replication config.
      // Let's use it.
      final HddsProtos.ReplicationType type = keyArgs.getType();
      keyInfoBuilder.setType(type);
      switch (type) {
      case EC:
        keyInfoBuilder.setEcReplicationConfig(keyArgs.getEcReplicationConfig());
        break;
      case RATIS:
      case STAND_ALONE:
        keyInfoBuilder.setFactor(keyArgs.getFactor());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown replication type: " + type);
      }
    }

    final KeyInfo keyInfo = keyInfoBuilder.build();
    openKeys.get(keyInfo.getVolumeName()).get(keyInfo.getBucketName())
        .put(keyInfo.getKeyName(), keyInfo);
    return CreateKeyResponse.newBuilder().setOpenVersion(0L).setKeyInfo(keyInfo)
        .build();
  }

  private InfoBucketResponse infoBucket(InfoBucketRequest infoBucketRequest) {
    BucketInfo bucketInfo = buckets.get(infoBucketRequest.getVolumeName())
        .get(infoBucketRequest.getBucketName());
    if (!bucketInfo.hasDefaultReplicationConfig()) {
      bucketInfo = bucketInfo.toBuilder()
          .setDefaultReplicationConfig(new DefaultReplicationConfig(
              ReplicationConfig.getDefault(new OzoneConfiguration())).toProto()
          )
          .build();
    }
    return InfoBucketResponse.newBuilder()
        .setBucketInfo(bucketInfo)
        .build();
  }

  private InfoVolumeResponse infoVolume(InfoVolumeRequest infoVolumeRequest) {
    final VolumeInfo volumeInfo =
        volumes.get(infoVolumeRequest.getVolumeName());
    if (volumeInfo == null) {
      throw new MockOmException(Status.VOLUME_NOT_FOUND);
    }
    return InfoVolumeResponse.newBuilder()
        .setVolumeInfo(volumeInfo)
        .build();
  }

  private CreateVolumeResponse createVolume(
      CreateVolumeRequest createVolumeRequest) {
    volumes.put(createVolumeRequest.getVolumeInfo().getVolume(),
        createVolumeRequest.getVolumeInfo());
    buckets
        .put(createVolumeRequest.getVolumeInfo().getVolume(), new HashMap<>());
    openKeys
        .put(createVolumeRequest.getVolumeInfo().getVolume(), new HashMap<>());
    keys
        .put(createVolumeRequest.getVolumeInfo().getVolume(), new HashMap<>());
    return CreateVolumeResponse.newBuilder()
        .build();
  }

  private ServiceListResponse serviceList(
      ServiceListRequest serviceListRequest) {
    return ServiceListResponse.newBuilder()
        .build();
  }

  private ListOpenFilesResponse listOpenFiles(
      ListOpenFilesRequest listOpenFilesRequest) {
    return ListOpenFilesResponse.newBuilder().build();
  }

  private OMResponse response(OMRequest payload,
      Function<OMResponse.Builder, OMResponse.Builder> function) {
    Builder builder = OMResponse.newBuilder();
    try {
      builder = function.apply(builder);
      builder.setSuccess(true);
      builder.setStatus(Status.OK);
    } catch (MockOmException e) {
      builder.setSuccess(false);
      builder.setStatus(e.getStatus());
    }

    builder.setCmdType(payload.getCmdType());
    return builder.build();
  }

  private CreateBucketResponse createBucket(
      CreateBucketRequest createBucketRequest) {
    final BucketInfo bucketInfo =
        BucketInfo.newBuilder(createBucketRequest.getBucketInfo())
            .setCreationTime(System.currentTimeMillis())
            .build();

    buckets.get(bucketInfo.getVolumeName())
        .put(bucketInfo.getBucketName(), bucketInfo);
    openKeys.get(bucketInfo.getVolumeName())
        .put(bucketInfo.getBucketName(), new HashMap<>());
    keys.get(bucketInfo.getVolumeName())
        .put(bucketInfo.getBucketName(), new HashMap<>());
    return CreateBucketResponse.newBuilder().build();
  }

  public Map<String, Map<String, Map<String, KeyInfo>>> getKeys() {
    return this.keys;
  }

  @Override
  public Text getDelegationTokenService() {
    return null;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Error from mock OM API.
   */
  public static class MockOmException extends RuntimeException {

    private Status status;

    public MockOmException(
        Status status) {
      this.status = status;
    }

    public Status getStatus() {
      return status;
    }
  }

}
