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
package org.apache.hadoop.ozone.om.protocolPB;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos
    .UpgradeFinalizationStatus;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.DBUpdates;
import org.apache.hadoop.ozone.om.helpers.DeleteTenantState;
import org.apache.hadoop.ozone.om.helpers.KeyInfoWithVolumeContext;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.helpers.OmBucketArgs;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDeleteKeys;
import org.apache.hadoop.ozone.om.helpers.OmKeyArgs;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyLocationInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUpload;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadList;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadListParts;
import org.apache.hadoop.ozone.om.helpers.OmRenameKeys;
import org.apache.hadoop.ozone.om.helpers.OmTenantArgs;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.om.helpers.OpenKeySession;
import org.apache.hadoop.ozone.om.helpers.OzoneFileStatus;
import org.apache.hadoop.ozone.om.helpers.RepeatedOmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.S3SecretValue;
import org.apache.hadoop.ozone.om.helpers.S3VolumeContext;
import org.apache.hadoop.ozone.om.helpers.ServiceInfo;
import org.apache.hadoop.ozone.om.helpers.ServiceInfoEx;
import org.apache.hadoop.ozone.om.helpers.TenantStateList;
import org.apache.hadoop.ozone.om.helpers.TenantUserInfoValue;
import org.apache.hadoop.ozone.om.helpers.TenantUserList;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AddAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.AllocateBlockResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.BucketInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CheckVolumeAccessRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CommitKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateDirectoryRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CreateVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DBUpdatesResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.DeleteVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeProgressRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeProgressResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.FinalizeUpgradeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetFileStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetKeyInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeContextRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.GetS3VolumeContextResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoBucketResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.InfoVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListBucketsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListMultipartUploadsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListMultipartUploadsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListStatusResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTenantRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTenantResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTrashRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListTrashResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ListVolumeResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupFileResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LookupKeyResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartCommitUploadPartResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartInfoInitiateResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadCompleteResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.MultipartUploadListPartsResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneFileStatusProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerBGSyncRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RangerBGSyncResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverTrashRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RecoverTrashResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RemoveAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysMap;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenameKeysRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RenewDelegationTokenResponseProto;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.RevokeS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Secret;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServiceListResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetAclResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetS3SecretRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetS3SecretResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantAssignUserAccessIdResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantGetUserInfoResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantListUserRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantListUserResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeAdminRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.TenantRevokeUserAccessIdRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.VolumeInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.EchoRPCResponse;
import org.apache.hadoop.ozone.protocolPB.OMPBHelper;
import org.apache.hadoop.ozone.security.OzoneTokenIdentifier;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.CancelDelegationTokenRequestProto;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.GetDelegationTokenRequestProto;
import org.apache.hadoop.ozone.security.proto.SecurityProtos.RenewDelegationTokenRequestProto;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer;
import org.apache.hadoop.ozone.upgrade.UpgradeFinalizer.StatusAndMessages;
import org.apache.hadoop.security.token.Token;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ByteString;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.TOKEN_ERROR_OTHER;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelPrepareRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.CancelPrepareResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareRequestArgs;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusRequest;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.PrepareStatusResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.S3Authentication;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetBucketPropertyResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.SetVolumePropertyResponse;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.ACCESS_DENIED;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.DIRECTORY_ALREADY_EXISTS;
import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.OK;

/**
 * The client side implementation of OzoneManagerProtocol.
 */

@InterfaceAudience.Private
public final class OzoneManagerProtocolClientSideTranslatorPB
    implements OzoneManagerClientProtocol {

  private final String clientID;
  private OmTransport transport;
  private ThreadLocal<S3Auth> threadLocalS3Auth
      = new ThreadLocal<>();
    
  private boolean s3AuthCheck;
  public OzoneManagerProtocolClientSideTranslatorPB(OmTransport omTransport,
      String clientId) {
    this.clientID = clientId;
    this.transport = omTransport;
    this.s3AuthCheck = false;
  }

  /**
   * Closes this stream and releases any system resources associated
   * with it. If the stream is already closed then invoking this
   * method has no effect.
   * <p>
   * <p> As noted in {@link AutoCloseable#close()}, cases where the
   * close may fail require careful attention. It is strongly advised
   * to relinquish the underlying resources and to internally
   * <em>mark</em> the {@code Closeable} as closed, prior to throwing
   * the {@code IOException}.
   *
   * @throws IOException if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
    //transport is not reusable
    transport.close();
  }

  /**
   * Returns a OMRequest builder with specified type.
   * @param cmdType type of the request
   */
  private OMRequest.Builder createOMRequest(Type cmdType) {
    return OMRequest.newBuilder()
        .setCmdType(cmdType)
        .setVersion(ClientVersion.CURRENT_VERSION)
        .setClientId(clientID);
  }

  /**
   * Submits client request to OM server.
   * @param omRequest client request
   * @return response from OM
   * @throws IOException thrown if any Protobuf service exception occurs
   */
  private OMResponse submitRequest(OMRequest omRequest)
      throws IOException {
    OMRequest.Builder  builder = OMRequest.newBuilder(omRequest);
    // Insert S3 Authentication information for each request.
    if (getThreadLocalS3Auth() != null) {
      builder.setS3Authentication(
          S3Authentication.newBuilder()
              .setSignature(
                  threadLocalS3Auth.get().getSignature())
              .setStringToSign(
                  threadLocalS3Auth.get().getStringTosSign())
              .setAccessId(
                  threadLocalS3Auth.get().getAccessID())
              .build());
    }
    if (s3AuthCheck && getThreadLocalS3Auth() == null) {
      throw new IllegalArgumentException("S3 Auth expected to " +
          "be set but is null " + omRequest.toString());
    }
    OMResponse response =
        transport.submitRequest(
            builder.setTraceID(TracingUtil.exportCurrentSpan()).build());
    return response;
  }

  /**
   * Creates a volume.
   *
   * @param args - Arguments to create Volume.
   * @throws IOException
   */
  @Override
  public void createVolume(OmVolumeArgs args) throws IOException {
    CreateVolumeRequest.Builder req =
        CreateVolumeRequest.newBuilder();
    VolumeInfo volumeInfo = args.getProtobuf();
    req.setVolumeInfo(volumeInfo);

    OMRequest omRequest = createOMRequest(Type.CreateVolume)
        .setCreateVolumeRequest(req)
        .build();

    OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean setOwner(String volume, String owner) throws IOException {
    SetVolumePropertyRequest.Builder req =
        SetVolumePropertyRequest.newBuilder();
    req.setVolumeName(volume).setOwnerName(owner);

    OMRequest omRequest = createOMRequest(Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(req)
        .build();

    OMResponse omResponse = submitRequest(omRequest);
    SetVolumePropertyResponse response =
        handleError(omResponse).getSetVolumePropertyResponse();

    return response.getResponse();
  }

  /**
   * Changes the Quota on a volume.
   *
   * @param volume - Name of the volume.
   * @param quotaInNamespace - Volume quota in counts.
   * @param quotaInBytes - Volume quota in bytes.
   * @throws IOException
   */
  @Override
  public void setQuota(String volume, long quotaInNamespace,
      long quotaInBytes) throws IOException {
    SetVolumePropertyRequest.Builder req =
        SetVolumePropertyRequest.newBuilder();
    req.setVolumeName(volume)
        .setQuotaInBytes(quotaInBytes)
        .setQuotaInNamespace(quotaInNamespace);

    OMRequest omRequest = createOMRequest(Type.SetVolumeProperty)
        .setSetVolumePropertyRequest(req)
        .build();

    OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
  }

  /**
   * Checks if the specified user can access this volume.
   *
   * @param volume - volume
   * @param userAcl - user acls which needs to be checked for access
   * @return true if the user has required access for the volume,
   *         false otherwise
   * @throws IOException
   */
  @Override
  public boolean checkVolumeAccess(String volume, OzoneAclInfo userAcl) throws
      IOException {
    CheckVolumeAccessRequest.Builder req =
        CheckVolumeAccessRequest.newBuilder();
    req.setVolumeName(volume).setUserAcl(userAcl);

    OMRequest omRequest = createOMRequest(Type.CheckVolumeAccess)
        .setCheckVolumeAccessRequest(req)
        .build();

    OMResponse omResponse = submitRequest(omRequest);

    if (omResponse.getStatus() == ACCESS_DENIED) {
      return false;
    } else if (omResponse.getStatus() == OK) {
      return true;
    } else {
      handleError(omResponse);
      return false;
    }
  }

  /**
   * Gets the volume information.
   *
   * @param volume - Volume name.
   * @return OmVolumeArgs or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmVolumeArgs getVolumeInfo(String volume) throws IOException {
    InfoVolumeRequest.Builder req = InfoVolumeRequest.newBuilder();
    req.setVolumeName(volume);

    OMRequest omRequest = createOMRequest(Type.InfoVolume)
        .setInfoVolumeRequest(req)
        .build();

    InfoVolumeResponse resp =
        handleError(submitRequest(omRequest)).getInfoVolumeResponse();


    return OmVolumeArgs.getFromProtobuf(resp.getVolumeInfo());
  }

  /**
   * Deletes an existing empty volume.
   *
   * @param volume - Name of the volume.
   * @throws IOException
   */
  @Override
  public void deleteVolume(String volume) throws IOException {
    DeleteVolumeRequest.Builder req = DeleteVolumeRequest.newBuilder();
    req.setVolumeName(volume);

    OMRequest omRequest = createOMRequest(Type.DeleteVolume)
        .setDeleteVolumeRequest(req)
        .build();

    handleError(submitRequest(omRequest));

  }

  /**
   * Lists volumes accessible by a specific user.
   *
   * @param userName - user name
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<OmVolumeArgs> listVolumeByUser(String userName, String prefix,
                                             String prevKey, int maxKeys)
      throws IOException {
    ListVolumeRequest.Builder builder = ListVolumeRequest.newBuilder();
    if (!Strings.isNullOrEmpty(prefix)) {
      builder.setPrefix(prefix);
    }
    if (!Strings.isNullOrEmpty(prevKey)) {
      builder.setPrevKey(prevKey);
    }
    builder.setMaxKeys(maxKeys);
    builder.setUserName(userName);
    builder.setScope(ListVolumeRequest.Scope.VOLUMES_BY_USER);
    return listVolume(builder.build());
  }

  /**
   * Lists volume all volumes in the cluster.
   *
   * @param prefix - Filter prefix -- Return only entries that match this.
   * @param prevKey - Previous key -- List starts from the next from the
   * prevkey
   * @param maxKeys - Max number of keys to return.
   * @return List of Volumes.
   * @throws IOException
   */
  @Override
  public List<OmVolumeArgs> listAllVolumes(String prefix, String prevKey,
                                           int maxKeys) throws IOException {
    ListVolumeRequest.Builder builder = ListVolumeRequest.newBuilder();
    if (!Strings.isNullOrEmpty(prefix)) {
      builder.setPrefix(prefix);
    }
    if (!Strings.isNullOrEmpty(prevKey)) {
      builder.setPrevKey(prevKey);
    }
    builder.setMaxKeys(maxKeys);
    builder.setScope(ListVolumeRequest.Scope.VOLUMES_BY_CLUSTER);
    return listVolume(builder.build());
  }

  private List<OmVolumeArgs> listVolume(ListVolumeRequest request)
      throws IOException {

    OMRequest omRequest = createOMRequest(Type.ListVolume)
        .setListVolumeRequest(request)
        .build();

    ListVolumeResponse resp =
        handleError(submitRequest(omRequest)).getListVolumeResponse();
    List<OmVolumeArgs> list = new ArrayList<>(resp.getVolumeInfoList().size());
    for (VolumeInfo info : resp.getVolumeInfoList()) {
      list.add(OmVolumeArgs.getFromProtobuf(info));
    }
    return list;
  }

  /**
   * Creates a bucket.
   *
   * @param bucketInfo - BucketInfo to create bucket.
   * @throws IOException
   */
  @Override
  public void createBucket(OmBucketInfo bucketInfo) throws IOException {
    CreateBucketRequest.Builder req =
        CreateBucketRequest.newBuilder();
    BucketInfo bucketInfoProtobuf = bucketInfo.getProtobuf();
    req.setBucketInfo(bucketInfoProtobuf);

    OMRequest omRequest = createOMRequest(Type.CreateBucket)
        .setCreateBucketRequest(req)
        .build();

    handleError(submitRequest(omRequest));

  }

  /**
   * Gets the bucket information.
   *
   * @param volume - Volume name.
   * @param bucket - Bucket name.
   * @return OmBucketInfo or exception is thrown.
   * @throws IOException
   */
  @Override
  public OmBucketInfo getBucketInfo(String volume, String bucket)
      throws IOException {
    InfoBucketRequest.Builder req =
        InfoBucketRequest.newBuilder();
    req.setVolumeName(volume);
    req.setBucketName(bucket);

    OMRequest omRequest = createOMRequest(Type.InfoBucket)
        .setInfoBucketRequest(req)
        .build();

    InfoBucketResponse resp =
        handleError(submitRequest(omRequest)).getInfoBucketResponse();

    return OmBucketInfo.getFromProtobuf(resp.getBucketInfo());
  }

  /**
   * Sets bucket property from args.
   * @param args - BucketArgs.
   * @throws IOException
   */
  @Override
  public void setBucketProperty(OmBucketArgs args)
      throws IOException {
    SetBucketPropertyRequest.Builder req =
        SetBucketPropertyRequest.newBuilder();
    BucketArgs bucketArgs = args.getProtobuf();
    req.setBucketArgs(bucketArgs);

    OMRequest omRequest = createOMRequest(Type.SetBucketProperty)
        .setSetBucketPropertyRequest(req)
        .build();

    handleError(submitRequest(omRequest));

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean setBucketOwner(OmBucketArgs args)
      throws IOException {
    SetBucketPropertyRequest.Builder req =
        SetBucketPropertyRequest.newBuilder();
    BucketArgs bucketArgs = args.getProtobuf();
    req.setBucketArgs(bucketArgs);

    OMRequest omRequest = createOMRequest(Type.SetBucketProperty)
        .setSetBucketPropertyRequest(req)
        .build();

    OMResponse omResponse = submitRequest(omRequest);
    SetBucketPropertyResponse response =
        handleError(omResponse).getSetBucketPropertyResponse();

    return response.getResponse();
  }

  /**
   * List buckets in a volume.
   *
   * @param volumeName
   * @param startKey
   * @param prefix
   * @param count
   * @return
   * @throws IOException
   */
  @Override
  public List<OmBucketInfo> listBuckets(String volumeName,
      String startKey, String prefix, int count) throws IOException {
    List<OmBucketInfo> buckets = new ArrayList<>();
    ListBucketsRequest.Builder reqBuilder = ListBucketsRequest.newBuilder();
    reqBuilder.setVolumeName(volumeName);
    reqBuilder.setCount(count);
    if (startKey != null) {
      reqBuilder.setStartKey(startKey);
    }
    if (prefix != null) {
      reqBuilder.setPrefix(prefix);
    }
    ListBucketsRequest request = reqBuilder.build();

    OMRequest omRequest = createOMRequest(Type.ListBuckets)
        .setListBucketsRequest(request)
        .build();

    ListBucketsResponse resp = handleError(submitRequest(omRequest))
        .getListBucketsResponse();

    buckets.addAll(
          resp.getBucketInfoList().stream()
              .map(OmBucketInfo::getFromProtobuf)
              .collect(Collectors.toList()));
    return buckets;

  }

  /**
   * Create a new open session of the key, then use the returned meta info to
   * talk to data node to actually write the key.
   * @param args the args for the key to be allocated
   * @return a handler to the key, returned client
   * @throws IOException
   */
  @Override
  public OpenKeySession openKey(OmKeyArgs args) throws IOException {
    CreateKeyRequest.Builder req = CreateKeyRequest.newBuilder();
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName());

    if (args.getAcls() != null) {
      keyArgs.addAllAcls(args.getAcls().stream().distinct().map(a ->
          OzoneAcl.toProtobuf(a)).collect(Collectors.toList()));
    }

    if (args.getReplicationConfig() != null) {
      if (args.getReplicationConfig() instanceof ECReplicationConfig) {
        keyArgs.setEcReplicationConfig(
            ((ECReplicationConfig) args.getReplicationConfig()).toProto());
      } else {
        keyArgs.setFactor(
            ReplicationConfig.getLegacyFactor(args.getReplicationConfig()));
      }
      keyArgs.setType(args.getReplicationConfig().getReplicationType());
    }


    if (args.getDataSize() > 0) {
      keyArgs.setDataSize(args.getDataSize());
    }

    if (args.getMetadata() != null && args.getMetadata().size() > 0) {
      keyArgs.addAllMetadata(KeyValueUtil.toProtobuf(args.getMetadata()));
    }
    req.setKeyArgs(keyArgs.build());

    if (args.getMultipartUploadID() != null) {
      keyArgs.setMultipartUploadID(args.getMultipartUploadID());
    }

    if (args.getMultipartUploadPartNumber() > 0) {
      keyArgs.setMultipartNumber(args.getMultipartUploadPartNumber());
    }

    keyArgs.setIsMultipartKey(args.getIsMultipartKey());


    req.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(Type.CreateKey)
        .setCreateKeyRequest(req)
        .build();

    CreateKeyResponse keyResponse =
        handleError(submitRequest(omRequest)).getCreateKeyResponse();
    return new OpenKeySession(keyResponse.getID(),
        OmKeyInfo.getFromProtobuf(keyResponse.getKeyInfo()),
        keyResponse.getOpenVersion());
  }

  private OMResponse handleError(OMResponse resp) throws OMException {
    if (resp.getStatus() != OK) {
      throw new OMException(resp.getMessage(),
          ResultCodes.values()[resp.getStatus().ordinal()]);
    }
    return resp;
  }

  @Override
  public OmKeyLocationInfo allocateBlock(OmKeyArgs args, long clientId,
      ExcludeList excludeList) throws IOException {
    AllocateBlockRequest.Builder req = AllocateBlockRequest.newBuilder();
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize());

    if (args.getReplicationConfig() != null) {
      if (args.getReplicationConfig() instanceof ECReplicationConfig) {
        keyArgs.setEcReplicationConfig(
            ((ECReplicationConfig) args.getReplicationConfig()).toProto());
      } else {
        keyArgs.setFactor(
            ReplicationConfig.getLegacyFactor(args.getReplicationConfig()));
      }
      keyArgs.setType(args.getReplicationConfig().getReplicationType());
    }

    req.setKeyArgs(keyArgs);
    req.setClientID(clientId);
    req.setExcludeList(excludeList.getProtoBuf());


    OMRequest omRequest = createOMRequest(Type.AllocateBlock)
        .setAllocateBlockRequest(req)
        .build();

    AllocateBlockResponse resp = handleError(submitRequest(omRequest))
        .getAllocateBlockResponse();
    return OmKeyLocationInfo.getFromProtobuf(resp.getKeyLocation());
  }
  @Override
  public void commitKey(OmKeyArgs args, long clientId)
      throws IOException {
    CommitKeyRequest.Builder req = CommitKeyRequest.newBuilder();
    List<OmKeyLocationInfo> locationInfoList = args.getLocationInfoList();
    Preconditions.checkNotNull(locationInfoList);
    KeyArgs.Builder keyArgsBuilder = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize())
        .addAllKeyLocations(locationInfoList.stream()
            // TODO use OM version?
            .map(info -> info.getProtobuf(ClientVersion.CURRENT_VERSION))
            .collect(Collectors.toList()));

    if (args.getReplicationConfig() != null) {
      if (args.getReplicationConfig() instanceof ECReplicationConfig) {
        keyArgsBuilder.setEcReplicationConfig(
            ((ECReplicationConfig) args.getReplicationConfig()).toProto());
      } else {
        keyArgsBuilder.setFactor(
            ReplicationConfig.getLegacyFactor(args.getReplicationConfig()));
      }
      keyArgsBuilder.setType(args.getReplicationConfig().getReplicationType());
    }

    req.setKeyArgs(keyArgsBuilder.build());
    req.setClientID(clientId);


    OMRequest omRequest = createOMRequest(Type.CommitKey)
        .setCommitKeyRequest(req)
        .build();

    handleError(submitRequest(omRequest));


  }

  @Override
  public OmKeyInfo lookupKey(OmKeyArgs args) throws IOException {
    LookupKeyRequest.Builder req = LookupKeyRequest.newBuilder();
    req.setKeyArgs(args.toProtobuf());

    OMRequest omRequest = createOMRequest(Type.LookupKey)
        .setLookupKeyRequest(req)
        .build();

    LookupKeyResponse resp =
        handleError(submitRequest(omRequest)).getLookupKeyResponse();

    return OmKeyInfo.getFromProtobuf(resp.getKeyInfo());
  }

  @Override
  public KeyInfoWithVolumeContext getKeyInfo(OmKeyArgs args,
                                             boolean assumeS3Context)
      throws IOException {
    GetKeyInfoRequest.Builder req = GetKeyInfoRequest.newBuilder();
    req.setKeyArgs(args.toProtobuf());
    req.setAssumeS3Context(assumeS3Context);

    OMRequest omRequest = createOMRequest(Type.GetKeyInfo)
        .setGetKeyInfoRequest(req)
        .build();

    GetKeyInfoResponse resp =
        handleError(submitRequest(omRequest)).getGetKeyInfoResponse();
    return KeyInfoWithVolumeContext.fromProtobuf(resp);
  }

  @Override
  @Deprecated
  public void renameKeys(OmRenameKeys omRenameKeys) throws IOException {

    List<RenameKeysMap> renameKeyList  = new ArrayList<>();
    for (Map.Entry< String, String> entry :
        omRenameKeys.getFromAndToKey().entrySet()) {
      RenameKeysMap.Builder renameKey = RenameKeysMap.newBuilder()
          .setFromKeyName(entry.getKey())
          .setToKeyName(entry.getValue());
      renameKeyList.add(renameKey.build());
    }

    RenameKeysArgs.Builder renameKeyArgs = RenameKeysArgs.newBuilder()
        .setVolumeName(omRenameKeys.getVolume())
        .setBucketName(omRenameKeys.getBucket())
        .addAllRenameKeysMap(renameKeyList);

    RenameKeysRequest.Builder reqKeys = RenameKeysRequest.newBuilder()
        .setRenameKeysArgs(renameKeyArgs.build());

    OMRequest omRequest = createOMRequest(Type.RenameKeys)
        .setRenameKeysRequest(reqKeys.build())
        .build();

    handleError(submitRequest(omRequest));
  }

  @Override
  public void renameKey(OmKeyArgs args, String toKeyName) throws IOException {
    RenameKeyRequest.Builder req = RenameKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize()).build();
    req.setKeyArgs(keyArgs);
    req.setToKeyName(toKeyName);

    OMRequest omRequest = createOMRequest(Type.RenameKey)
        .setRenameKeyRequest(req)
        .build();

    handleError(submitRequest(omRequest));
  }

  /**
   * Deletes an existing key.
   *
   * @param args the args of the key.
   * @throws IOException
   */
  @Override
  public void deleteKey(OmKeyArgs args) throws IOException {
    DeleteKeyRequest.Builder req = DeleteKeyRequest.newBuilder();
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setRecursive(args.isRecursive()).build();
    req.setKeyArgs(keyArgs);

    OMRequest omRequest = createOMRequest(Type.DeleteKey)
        .setDeleteKeyRequest(req)
        .build();

    handleError(submitRequest(omRequest));

  }

  /**
   * Deletes existing key/keys. This interface supports delete
   * multiple keys and a single key.
   *
   * @param deleteKeys
   * @throws IOException
   */
  @Override
  public void deleteKeys(OmDeleteKeys deleteKeys) throws IOException {
    DeleteKeysRequest.Builder req = DeleteKeysRequest.newBuilder();
    DeleteKeyArgs deletedKeys = DeleteKeyArgs.newBuilder()
        .setBucketName(deleteKeys.getBucket())
        .setVolumeName(deleteKeys.getVolume())
        .addAllKeys(deleteKeys.getKeyNames()).build();
    req.setDeleteKeys(deletedKeys);
    OMRequest omRequest = createOMRequest(Type.DeleteKeys)
        .setDeleteKeysRequest(req)
        .build();

    handleError(submitRequest(omRequest));

  }

  /**
   * Deletes an existing empty bucket from volume.
   * @param volume - Name of the volume.
   * @param bucket - Name of the bucket.
   * @throws IOException
   */
  @Override
  public void deleteBucket(String volume, String bucket) throws IOException {
    DeleteBucketRequest.Builder req = DeleteBucketRequest.newBuilder();
    req.setVolumeName(volume);
    req.setBucketName(bucket);

    OMRequest omRequest = createOMRequest(Type.DeleteBucket)
        .setDeleteBucketRequest(req)
        .build();

    handleError(submitRequest(omRequest));

  }

  /**
   * List keys in a bucket.
   */
  @Override
  public List<OmKeyInfo> listKeys(String volumeName, String bucketName,
      String startKey, String prefix, int maxKeys) throws IOException {
    List<OmKeyInfo> keys = new ArrayList<>();
    ListKeysRequest.Builder reqBuilder = ListKeysRequest.newBuilder();
    reqBuilder.setVolumeName(volumeName);
    reqBuilder.setBucketName(bucketName);
    reqBuilder.setCount(maxKeys);

    if (startKey != null) {
      reqBuilder.setStartKey(startKey);
    }

    if (prefix != null) {
      reqBuilder.setPrefix(prefix);
    }

    ListKeysRequest req = reqBuilder.build();

    OMRequest omRequest = createOMRequest(Type.ListKeys)
        .setListKeysRequest(req)
        .build();

    ListKeysResponse resp =
        handleError(submitRequest(omRequest)).getListKeysResponse();
    List<OmKeyInfo> list = new ArrayList<>();
    for (OzoneManagerProtocolProtos.KeyInfo keyInfo : resp.getKeyInfoList()) {
      OmKeyInfo fromProtobuf = OmKeyInfo.getFromProtobuf(keyInfo);
      list.add(fromProtobuf);
    }
    keys.addAll(list);
    return keys;

  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID) throws IOException {
    GetS3SecretRequest request = GetS3SecretRequest.newBuilder()
        .setKerberosID(kerberosID)
        .build();
    OMRequest omRequest = createOMRequest(Type.GetS3Secret)
        .setGetS3SecretRequest(request)
        .build();
    final GetS3SecretResponse resp = handleError(submitRequest(omRequest))
        .getGetS3SecretResponse();

    return S3SecretValue.fromProtobuf(resp.getS3Secret());
  }

  @Override
  public S3SecretValue getS3Secret(String kerberosID, boolean createIfNotExist)
          throws IOException {
    GetS3SecretRequest request = GetS3SecretRequest.newBuilder()
            .setKerberosID(kerberosID)
            .setCreateIfNotExist(createIfNotExist)
            .build();
    OMRequest omRequest = createOMRequest(Type.GetS3Secret)
            .setGetS3SecretRequest(request)
            .build();
    final GetS3SecretResponse resp = handleError(submitRequest(omRequest))
            .getGetS3SecretResponse();

    return S3SecretValue.fromProtobuf(resp.getS3Secret());
  }

  @Override
  public S3SecretValue setS3Secret(String accessId, String secretKey)
          throws IOException {
    final SetS3SecretRequest request = SetS3SecretRequest.newBuilder()
        .setAccessId(accessId)
        .setSecretKey(secretKey)
        .build();
    OMRequest omRequest = createOMRequest(Type.SetS3Secret)
        .setSetS3SecretRequest(request)
        .build();
    final SetS3SecretResponse resp = handleError(submitRequest(omRequest))
        .getSetS3SecretResponse();

    final S3Secret accessIdSecretKeyPair = S3Secret.newBuilder()
        .setKerberosID(resp.getAccessId())
        .setAwsSecret(resp.getSecretKey())
        .build();

    return S3SecretValue.fromProtobuf(accessIdSecretKeyPair);
  }

  @Override
  public void revokeS3Secret(String kerberosID) throws IOException {
    RevokeS3SecretRequest request = RevokeS3SecretRequest.newBuilder()
            .setKerberosID(kerberosID)
            .build();
    OMRequest omRequest = createOMRequest(Type.RevokeS3Secret)
            .setRevokeS3SecretRequest(request)
            .build();
    handleError(submitRequest(omRequest));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createTenant(OmTenantArgs omTenantArgs) throws IOException {
    final CreateTenantRequest.Builder requestBuilder =
        CreateTenantRequest.newBuilder()
            .setTenantId(omTenantArgs.getTenantId())
            .setVolumeName(omTenantArgs.getVolumeName());
            // Can add more args (like policy names) later if needed
    final OMRequest omRequest = createOMRequest(Type.CreateTenant)
        .setCreateTenantRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DeleteTenantState deleteTenant(String tenantId) throws IOException {
    final DeleteTenantRequest.Builder requestBuilder =
        DeleteTenantRequest.newBuilder()
            .setTenantId(tenantId);
    final OMRequest omRequest = createOMRequest(Type.DeleteTenant)
        .setDeleteTenantRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    final DeleteTenantResponse resp =
        handleError(omResponse).getDeleteTenantResponse();
    return DeleteTenantState.fromProtobuf(resp);
  }

  /**
   * {@inheritDoc}
   *
   * TODO: Add a variant that uses OmTenantUserArgs?
   */
  @Override
  public S3SecretValue tenantAssignUserAccessId(
      String username, String tenantId, String accessId) throws IOException {

    final TenantAssignUserAccessIdRequest.Builder requestBuilder =
        TenantAssignUserAccessIdRequest.newBuilder()
            .setUserPrincipal(username)
            .setTenantId(tenantId)
            .setAccessId(accessId);
    final OMRequest omRequest = createOMRequest(Type.TenantAssignUserAccessId)
        .setTenantAssignUserAccessIdRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    final TenantAssignUserAccessIdResponse resp = handleError(omResponse)
        .getTenantAssignUserAccessIdResponse();

    return S3SecretValue.fromProtobuf(resp.getS3Secret());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tenantRevokeUserAccessId(String accessId)
      throws IOException {

    final TenantRevokeUserAccessIdRequest.Builder requestBuilder =
        TenantRevokeUserAccessIdRequest.newBuilder()
            .setAccessId(accessId);
    final OMRequest omRequest = createOMRequest(Type.TenantRevokeUserAccessId)
        .setTenantRevokeUserAccessIdRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tenantAssignAdmin(String accessId, String tenantId,
      boolean delegated) throws IOException {

    final TenantAssignAdminRequest.Builder requestBuilder =
        TenantAssignAdminRequest.newBuilder()
            .setAccessId(accessId)
            .setDelegated(delegated);
    if (tenantId != null) {
      requestBuilder.setTenantId(tenantId);
    }
    final TenantAssignAdminRequest request = requestBuilder.build();
    final OMRequest omRequest = createOMRequest(Type.TenantAssignAdmin)
        .setTenantAssignAdminRequest(request)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void tenantRevokeAdmin(String accessId, String tenantId)
      throws IOException {

    final TenantRevokeAdminRequest.Builder requestBuilder =
        TenantRevokeAdminRequest.newBuilder()
            .setAccessId(accessId);
    if (tenantId != null) {
      requestBuilder.setTenantId(tenantId);
    }
    final TenantRevokeAdminRequest request = requestBuilder.build();
    final OMRequest omRequest = createOMRequest(Type.TenantRevokeAdmin)
        .setTenantRevokeAdminRequest(request)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    handleError(omResponse);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TenantUserInfoValue tenantGetUserInfo(String userPrincipal)
      throws IOException {

    final TenantGetUserInfoRequest.Builder requestBuilder =
        TenantGetUserInfoRequest.newBuilder()
            .setUserPrincipal(userPrincipal);
    final OMRequest omRequest = createOMRequest(Type.TenantGetUserInfo)
        .setTenantGetUserInfoRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    final TenantGetUserInfoResponse resp = handleError(omResponse)
        .getTenantGetUserInfoResponse();

    return TenantUserInfoValue.fromProtobuf(resp);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TenantUserList listUsersInTenant(String tenantId, String prefix)
      throws IOException {

    final TenantListUserRequest.Builder requestBuilder =
        TenantListUserRequest.newBuilder()
            .setTenantId(tenantId);
    if (prefix != null) {
      requestBuilder.setPrefix(prefix);
    }
    TenantListUserRequest request = requestBuilder.build();

    final OMRequest omRequest = createOMRequest(Type.TenantListUser)
        .setTenantListUserRequest(request).build();
    final OMResponse response = submitRequest(omRequest);
    final TenantListUserResponse resp =
        handleError(response).getTenantListUserResponse();
    return TenantUserList.fromProtobuf(resp);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public TenantStateList listTenant() throws IOException {

    final ListTenantRequest.Builder requestBuilder =
        ListTenantRequest.newBuilder();
    final OMRequest omRequest = createOMRequest(Type.ListTenant)
        .setListTenantRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    final ListTenantResponse resp = handleError(omResponse)
        .getListTenantResponse();

    return TenantStateList.fromProtobuf(resp.getTenantStateList());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public S3VolumeContext getS3VolumeContext() throws IOException {

    final GetS3VolumeContextRequest.Builder requestBuilder =
        GetS3VolumeContextRequest.newBuilder();
    final OMRequest omRequest = createOMRequest(Type.GetS3VolumeContext)
        .setGetS3VolumeContextRequest(requestBuilder)
        .build();
    final OMResponse omResponse = submitRequest(omRequest);
    final GetS3VolumeContextResponse resp =
        handleError(omResponse).getGetS3VolumeContextResponse();
    return S3VolumeContext.fromProtobuf(resp);
  }

  /**
   * Return the proxy object underlying this protocol translator.
   *
   * @return the proxy object underlying this protocol translator.
   */
  @Override
  public OmMultipartInfo initiateMultipartUpload(OmKeyArgs omKeyArgs) throws
      IOException {

    MultipartInfoInitiateRequest.Builder multipartInfoInitiateRequest =
        MultipartInfoInitiateRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .addAllAcls(omKeyArgs.getAcls().stream().map(a ->
            OzoneAcl.toProtobuf(a)).collect(Collectors.toList()));

    if (omKeyArgs.getReplicationConfig() != null) {
      if (omKeyArgs.getReplicationConfig() instanceof ECReplicationConfig) {
        keyArgs.setEcReplicationConfig(
            ((ECReplicationConfig) omKeyArgs.getReplicationConfig()).toProto());
      } else {
        keyArgs.setFactor(ReplicationConfig
            .getLegacyFactor(omKeyArgs.getReplicationConfig()));
      }
      keyArgs.setType(omKeyArgs.getReplicationConfig().getReplicationType());
    }

    multipartInfoInitiateRequest.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(
        Type.InitiateMultiPartUpload)
        .setInitiateMultiPartUploadRequest(multipartInfoInitiateRequest.build())
        .build();

    MultipartInfoInitiateResponse resp = handleError(submitRequest(omRequest))
        .getInitiateMultiPartUploadResponse();

    return new OmMultipartInfo(resp.getVolumeName(), resp.getBucketName(), resp
        .getKeyName(), resp.getMultipartUploadID());
  }

  @Override
  public OmMultipartCommitUploadPartInfo commitMultipartUploadPart(
      OmKeyArgs omKeyArgs, long clientId) throws IOException {

    List<OmKeyLocationInfo> locationInfoList = omKeyArgs.getLocationInfoList();
    Preconditions.checkNotNull(locationInfoList);


    MultipartCommitUploadPartRequest.Builder multipartCommitUploadPartRequest
        = MultipartCommitUploadPartRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID())
        .setIsMultipartKey(omKeyArgs.getIsMultipartKey())
        .setMultipartNumber(omKeyArgs.getMultipartUploadPartNumber())
        .setDataSize(omKeyArgs.getDataSize())
        .addAllKeyLocations(locationInfoList.stream()
            // TODO use OM version?
            .map(info -> info.getProtobuf(ClientVersion.CURRENT_VERSION))
            .collect(Collectors.toList()));
    multipartCommitUploadPartRequest.setClientID(clientId);
    multipartCommitUploadPartRequest.setKeyArgs(keyArgs.build());

    OMRequest omRequest = createOMRequest(
        Type.CommitMultiPartUpload)
        .setCommitMultiPartUploadRequest(multipartCommitUploadPartRequest
            .build())
        .build();

    MultipartCommitUploadPartResponse response =
        handleError(submitRequest(omRequest))
        .getCommitMultiPartUploadResponse();

    OmMultipartCommitUploadPartInfo info = new
        OmMultipartCommitUploadPartInfo(response.getPartName());
    return info;
  }

  @Override
  public OmMultipartUploadCompleteInfo completeMultipartUpload(
      OmKeyArgs omKeyArgs, OmMultipartUploadCompleteList multipartUploadList)
      throws IOException {
    MultipartUploadCompleteRequest.Builder multipartUploadCompleteRequest =
        MultipartUploadCompleteRequest.newBuilder();

    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .addAllAcls(omKeyArgs.getAcls().stream().map(a ->
            OzoneAcl.toProtobuf(a)).collect(Collectors.toList()))
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID());

    multipartUploadCompleteRequest.setKeyArgs(keyArgs.build());
    multipartUploadCompleteRequest.addAllPartsList(multipartUploadList
        .getPartsList());

    OMRequest omRequest = createOMRequest(
        Type.CompleteMultiPartUpload)
        .setCompleteMultiPartUploadRequest(
            multipartUploadCompleteRequest.build()).build();

    MultipartUploadCompleteResponse response =
        handleError(submitRequest(omRequest))
        .getCompleteMultiPartUploadResponse();

    OmMultipartUploadCompleteInfo info = new
        OmMultipartUploadCompleteInfo(response.getVolume(), response
        .getBucket(), response.getKey(), response.getHash());
    return info;
  }

  @Override
  public void abortMultipartUpload(OmKeyArgs omKeyArgs) throws IOException {
    KeyArgs.Builder keyArgs = KeyArgs.newBuilder()
        .setVolumeName(omKeyArgs.getVolumeName())
        .setBucketName(omKeyArgs.getBucketName())
        .setKeyName(omKeyArgs.getKeyName())
        .setMultipartUploadID(omKeyArgs.getMultipartUploadID());

    MultipartUploadAbortRequest.Builder multipartUploadAbortRequest =
        MultipartUploadAbortRequest.newBuilder();
    multipartUploadAbortRequest.setKeyArgs(keyArgs);

    OMRequest omRequest = createOMRequest(
        Type.AbortMultiPartUpload)
        .setAbortMultiPartUploadRequest(multipartUploadAbortRequest.build())
        .build();

    handleError(submitRequest(omRequest));

  }

  @Override
  public OmMultipartUploadListParts listParts(String volumeName,
      String bucketName, String keyName, String uploadID,
      int partNumberMarker, int maxParts) throws IOException {
    MultipartUploadListPartsRequest.Builder multipartUploadListPartsRequest =
        MultipartUploadListPartsRequest.newBuilder();
    multipartUploadListPartsRequest.setVolume(volumeName)
        .setBucket(bucketName).setKey(keyName).setUploadID(uploadID)
        .setPartNumbermarker(partNumberMarker).setMaxParts(maxParts);

    OMRequest omRequest = createOMRequest(Type.ListMultiPartUploadParts)
        .setListMultipartUploadPartsRequest(
            multipartUploadListPartsRequest.build()).build();

    MultipartUploadListPartsResponse response =
        handleError(submitRequest(omRequest))
            .getListMultipartUploadPartsResponse();


    OmMultipartUploadListParts omMultipartUploadListParts =
        new OmMultipartUploadListParts(
            ReplicationConfig.fromProto(
                response.getType(), response.getFactor(),
                response.getEcReplicationConfig()),
            response.getNextPartNumberMarker(), response.getIsTruncated());
    omMultipartUploadListParts.addProtoPartList(response.getPartsListList());

    return omMultipartUploadListParts;

  }

  @Override
  public OmMultipartUploadList listMultipartUploads(String volumeName,
      String bucketName,
      String prefix) throws IOException {
    ListMultipartUploadsRequest request = ListMultipartUploadsRequest
        .newBuilder()
        .setVolume(volumeName)
        .setBucket(bucketName)
        .setPrefix(prefix == null ? "" : prefix)
        .build();

    OMRequest omRequest = createOMRequest(Type.ListMultipartUploads)
        .setListMultipartUploadsRequest(request)
        .build();

    ListMultipartUploadsResponse listMultipartUploadsResponse =
        handleError(submitRequest(omRequest)).getListMultipartUploadsResponse();

    List<OmMultipartUpload> uploadList =
        listMultipartUploadsResponse.getUploadsListList()
            .stream()
            .map(proto -> new OmMultipartUpload(
                proto.getVolumeName(),
                proto.getBucketName(),
                proto.getKeyName(),
                proto.getUploadId(),
                Instant.ofEpochMilli(proto.getCreationTime()),
                ReplicationConfig.fromProto(proto.getType(), proto.getFactor(),
                    proto.getEcReplicationConfig())
            ))
            .collect(Collectors.toList());

    OmMultipartUploadList response = new OmMultipartUploadList(uploadList);

    return response;
  }

  @Override
  public List<ServiceInfo> getServiceList() throws IOException {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    OMRequest omRequest = createOMRequest(Type.ServiceList)
        .setServiceListRequest(req)
        .build();

    final ServiceListResponse resp = handleError(submitRequest(omRequest))
        .getServiceListResponse();

    return resp.getServiceInfoList().stream()
          .map(ServiceInfo::getFromProtobuf)
          .collect(Collectors.toList());

  }

  @Override
  public ServiceInfoEx getServiceInfo() throws IOException {
    ServiceListRequest req = ServiceListRequest.newBuilder().build();

    OMRequest omRequest = createOMRequest(Type.ServiceList)
        .setServiceListRequest(req)
        .build();

    final ServiceListResponse resp = handleError(submitRequest(omRequest))
        .getServiceListResponse();

    return new ServiceInfoEx(
        resp.getServiceInfoList().stream()
            .map(ServiceInfo::getFromProtobuf)
            .collect(Collectors.toList()),
        resp.getCaCertificate(), resp.getCaCertsList());
  }

  @Override
  public boolean triggerRangerBGSync(boolean noWait) throws IOException {
    RangerBGSyncRequest req = RangerBGSyncRequest.newBuilder()
        .setNoWait(noWait)
        .build();

    OMRequest omRequest = createOMRequest(Type.RangerBGSync)
        .setRangerBGSyncRequest(req)
        .build();

    RangerBGSyncResponse resp = handleError(submitRequest(omRequest))
        .getRangerBGSyncResponse();

    return resp.getRunSuccess();
  }

  @Override
  public StatusAndMessages finalizeUpgrade(String upgradeClientID)
      throws IOException {
    FinalizeUpgradeRequest req = FinalizeUpgradeRequest.newBuilder()
        .setUpgradeClientId(upgradeClientID)
        .build();

    OMRequest omRequest = createOMRequest(Type.FinalizeUpgrade)
        .setFinalizeUpgradeRequest(req)
        .build();

    FinalizeUpgradeResponse response =
        handleError(submitRequest(omRequest)).getFinalizeUpgradeResponse();

    UpgradeFinalizationStatus status = response.getStatus();
    return new StatusAndMessages(
        UpgradeFinalizer.Status.valueOf(status.getStatus().name()),
        status.getMessagesList()
    );
  }

  @Override
  public StatusAndMessages queryUpgradeFinalizationProgress(
      String upgradeClientID, boolean takeover, boolean readonly
  ) throws IOException {
    FinalizeUpgradeProgressRequest req = FinalizeUpgradeProgressRequest
        .newBuilder()
        .setUpgradeClientId(upgradeClientID)
        .setTakeover(takeover)
        .setReadonly(readonly)
        .build();

    OMRequest omRequest = createOMRequest(Type.FinalizeUpgradeProgress)
        .setFinalizeUpgradeProgressRequest(req)
        .build();

    FinalizeUpgradeProgressResponse response =
        handleError(submitRequest(omRequest))
            .getFinalizeUpgradeProgressResponse();

    UpgradeFinalizationStatus status = response.getStatus();

    return new StatusAndMessages(
        UpgradeFinalizer.Status.valueOf(status.getStatus().name()),
        status.getMessagesList()
    );
  }

  /**
   * Get a valid Delegation Token.
   *
   * @param renewer the designated renewer for the token
   * @return Token<OzoneDelegationTokenSelector>
   * @throws OMException
   */
  @Override
  public Token<OzoneTokenIdentifier> getDelegationToken(Text renewer)
      throws OMException {
    GetDelegationTokenRequestProto req = GetDelegationTokenRequestProto
        .newBuilder()
        .setRenewer(renewer == null ? "" : renewer.toString())
        .build();

    OMRequest omRequest = createOMRequest(Type.GetDelegationToken)
        .setGetDelegationTokenRequest(req)
        .build();

    final GetDelegationTokenResponseProto resp;
    try {
      resp =
          handleError(submitRequest(omRequest)).getGetDelegationTokenResponse();
      return resp.getResponse().hasToken() ?
          OMPBHelper.convertToDelegationToken(resp.getResponse().getToken())
          : null;
    } catch (IOException e) {
      if (e instanceof OMException) {
        throw (OMException)e;
      }
      throw new OMException("Get delegation token failed.", e,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Renew an existing delegation token.
   *
   * @param token delegation token obtained earlier
   * @return the new expiration time
   */
  @Override
  public long renewDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    RenewDelegationTokenRequestProto req =
        RenewDelegationTokenRequestProto.newBuilder().
            setToken(OMPBHelper.convertToTokenProto(token)).
            build();

    OMRequest omRequest = createOMRequest(Type.RenewDelegationToken)
        .setRenewDelegationTokenRequest(req)
        .build();

    final RenewDelegationTokenResponseProto resp;
    try {
      resp = handleError(submitRequest(omRequest))
          .getRenewDelegationTokenResponse();
      return resp.getResponse().getNewExpiryTime();
    } catch (IOException e) {
      if (e instanceof OMException) {
        throw (OMException)e;
      }
      throw new OMException("Renew delegation token failed.", e,
          TOKEN_ERROR_OTHER);
    }
  }

  /**
   * Cancel an existing delegation token.
   *
   * @param token delegation token
   */
  @Override
  public void cancelDelegationToken(Token<OzoneTokenIdentifier> token)
      throws OMException {
    CancelDelegationTokenRequestProto req = CancelDelegationTokenRequestProto
        .newBuilder()
        .setToken(OMPBHelper.convertToTokenProto(token))
        .build();

    OMRequest omRequest = createOMRequest(Type.CancelDelegationToken)
        .setCancelDelegationTokenRequest(req)
        .build();

    final CancelDelegationTokenResponseProto resp;
    try {
      handleError(submitRequest(omRequest));
    } catch (IOException e) {
      if (e instanceof OMException) {
        throw (OMException)e;
      }
      throw new OMException("Cancel delegation token failed.", e,
          TOKEN_ERROR_OTHER);
    }
  }

  @Override
  public void setThreadLocalS3Auth(
      S3Auth s3Auth) {
    this.threadLocalS3Auth.set(s3Auth);
  }
  @Override
  public void clearThreadLocalS3Auth() {
    this.threadLocalS3Auth.remove();
  }
  @Override
  public S3Auth getThreadLocalS3Auth() {
    return this.threadLocalS3Auth.get();
  }

  /**
   * Get File Status for an Ozone key.
   *
   * @param args
   * @return OzoneFileStatus for the key.
   * @throws IOException
   */
  @Override
  public OzoneFileStatus getFileStatus(OmKeyArgs args) throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setSortDatanodes(args.getSortDatanodes())
        .setLatestVersionLocation(args.getLatestVersionLocation())
        .build();
    GetFileStatusRequest req =
        GetFileStatusRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .build();

    OMRequest omRequest = createOMRequest(Type.GetFileStatus)
        .setGetFileStatusRequest(req)
        .build();

    final GetFileStatusResponse resp;
    try {
      resp = handleError(submitRequest(omRequest)).getGetFileStatusResponse();
    } catch (IOException e) {
      throw e;
    }
    return OzoneFileStatus.getFromProtobuf(resp.getStatus());
  }

  @Override
  public void createDirectory(OmKeyArgs args) throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .addAllAcls(args.getAcls().stream().map(a ->
            OzoneAcl.toProtobuf(a)).collect(Collectors.toList()))
        .build();
    CreateDirectoryRequest request = CreateDirectoryRequest.newBuilder()
        .setKeyArgs(keyArgs)
        .build();

    OMRequest omRequest = createOMRequest(Type.CreateDirectory)
        .setCreateDirectoryRequest(request)
        .build();

    OMResponse omResponse = submitRequest(omRequest);
    if (!omResponse.getStatus().equals(DIRECTORY_ALREADY_EXISTS)) {
      // TODO: If the directory already exists, we should return false to
      //  client. For this, the client createDirectory API needs to be
      //  changed to return a boolean.
      handleError(omResponse);
    }
  }

  @Override
  public OmKeyInfo lookupFile(OmKeyArgs args)
      throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setSortDatanodes(args.getSortDatanodes())
        .setLatestVersionLocation(args.getLatestVersionLocation())
        .build();
    LookupFileRequest lookupFileRequest = LookupFileRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .build();
    OMRequest omRequest = createOMRequest(Type.LookupFile)
        .setLookupFileRequest(lookupFileRequest)
        .build();
    LookupFileResponse resp =
        handleError(submitRequest(omRequest)).getLookupFileResponse();
    return OmKeyInfo.getFromProtobuf(resp.getKeyInfo());
  }

  /**
   * Add acl for Ozone object. Return true if acl is added successfully else
   * false.
   *
   * @param obj Ozone object for which acl should be added.
   * @param acl ozone acl to be added.
   * @throws IOException if there is error.
   */
  @Override
  public boolean addAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    AddAclRequest req = AddAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj))
        .setAcl(OzoneAcl.toProtobuf(acl))
        .build();

    OMRequest omRequest = createOMRequest(Type.AddAcl)
        .setAddAclRequest(req)
        .build();
    AddAclResponse addAclResponse =
        handleError(submitRequest(omRequest)).getAddAclResponse();

    return addAclResponse.getResponse();
  }

  /**
   * Remove acl for Ozone object. Return true if acl is removed successfully
   * else false.
   *
   * @param obj Ozone object.
   * @param acl Ozone acl to be removed.
   * @throws IOException if there is error.
   */
  @Override
  public boolean removeAcl(OzoneObj obj, OzoneAcl acl) throws IOException {
    RemoveAclRequest req = RemoveAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj))
        .setAcl(OzoneAcl.toProtobuf(acl))
        .build();

    OMRequest omRequest = createOMRequest(Type.RemoveAcl)
        .setRemoveAclRequest(req)
        .build();
    RemoveAclResponse response =
        handleError(submitRequest(omRequest)).getRemoveAclResponse();

    return response.getResponse();
  }

  /**
   * Acls to be set for given Ozone object. This operations reset ACL for given
   * object to list of ACLs provided in argument.
   *
   * @param obj Ozone object.
   * @param acls List of acls.
   * @throws IOException if there is error.
   */
  @Override
  public boolean setAcl(OzoneObj obj, List<OzoneAcl> acls) throws IOException {
    SetAclRequest.Builder builder = SetAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj));

    acls.forEach(a -> builder.addAcl(OzoneAcl.toProtobuf(a)));

    OMRequest omRequest = createOMRequest(Type.SetAcl)
        .setSetAclRequest(builder.build())
        .build();
    SetAclResponse response =
        handleError(submitRequest(omRequest)).getSetAclResponse();

    return response.getResponse();
  }

  /**
   * Returns list of ACLs for given Ozone object.
   *
   * @param obj Ozone object.
   * @throws IOException if there is error.
   */
  @Override
  public List<OzoneAcl> getAcl(OzoneObj obj) throws IOException {
    GetAclRequest req = GetAclRequest.newBuilder()
        .setObj(OzoneObj.toProtobuf(obj))
        .build();

    OMRequest omRequest = createOMRequest(Type.GetAcl)
        .setGetAclRequest(req)
        .build();
    GetAclResponse response =
        handleError(submitRequest(omRequest)).getGetAclResponse();
    List<OzoneAcl> acls = new ArrayList<>();
    response.getAclsList().stream().forEach(a ->
        acls.add(OzoneAcl.fromProtobuf(a)));
    return acls;
  }

  @Override
  public DBUpdates getDBUpdates(DBUpdatesRequest dbUpdatesRequest)
      throws IOException {
    OMRequest omRequest = createOMRequest(Type.DBUpdates)
        .setDbUpdatesRequest(dbUpdatesRequest)
        .build();

    DBUpdatesResponse dbUpdatesResponse =
        handleError(submitRequest(omRequest)).getDbUpdatesResponse();

    DBUpdates dbUpdatesWrapper = new DBUpdates();
    for (ByteString byteString : dbUpdatesResponse.getDataList()) {
      dbUpdatesWrapper.addWriteBatch(byteString.toByteArray(), 0L);
    }
    dbUpdatesWrapper.setCurrentSequenceNumber(
        dbUpdatesResponse.getSequenceNumber());
    dbUpdatesWrapper.setLatestSequenceNumber(
        dbUpdatesResponse.getLatestSequenceNumber());
    return dbUpdatesWrapper;
  }

  @Override
  public OpenKeySession createFile(OmKeyArgs args,
      boolean overWrite, boolean recursive) throws IOException {
    KeyArgs.Builder keyArgsBuilder = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setDataSize(args.getDataSize())
        .addAllAcls(args.getAcls().stream().map(a ->
            OzoneAcl.toProtobuf(a)).collect(Collectors.toList()));
    if (args.getReplicationConfig() != null) {
      if (args.getReplicationConfig() instanceof ECReplicationConfig) {
        keyArgsBuilder.setEcReplicationConfig(
            ((ECReplicationConfig) args.getReplicationConfig()).toProto());
      } else {
        keyArgsBuilder.setFactor(
            ReplicationConfig.getLegacyFactor(args.getReplicationConfig()));
      }
      keyArgsBuilder.setType(args.getReplicationConfig().getReplicationType());
    }
    CreateFileRequest createFileRequest = CreateFileRequest.newBuilder()
            .setKeyArgs(keyArgsBuilder.build())
            .setIsOverwrite(overWrite)
            .setIsRecursive(recursive)
            .build();
    OMRequest omRequest = createOMRequest(Type.CreateFile)
        .setCreateFileRequest(createFileRequest)
        .build();
    CreateFileResponse resp =
        handleError(submitRequest(omRequest)).getCreateFileResponse();
    return new OpenKeySession(resp.getID(),
        OmKeyInfo.getFromProtobuf(resp.getKeyInfo()), resp.getOpenVersion());
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries, boolean allowPartialPrefixes)
      throws IOException {
    KeyArgs keyArgs = KeyArgs.newBuilder()
        .setVolumeName(args.getVolumeName())
        .setBucketName(args.getBucketName())
        .setKeyName(args.getKeyName())
        .setSortDatanodes(args.getSortDatanodes())
        .setLatestVersionLocation(args.getLatestVersionLocation())
        .build();
    ListStatusRequest.Builder listStatusRequestBuilder =
        ListStatusRequest.newBuilder()
            .setKeyArgs(keyArgs)
            .setRecursive(recursive)
            .setStartKey(startKey)
            .setNumEntries(numEntries);

    if (allowPartialPrefixes) {
      listStatusRequestBuilder.setAllowPartialPrefix(allowPartialPrefixes);
    }

    OMRequest omRequest = createOMRequest(Type.ListStatus)
        .setListStatusRequest(listStatusRequestBuilder.build())
        .build();
    ListStatusResponse listStatusResponse =
        handleError(submitRequest(omRequest)).getListStatusResponse();
    List<OzoneFileStatus> statusList =
        new ArrayList<>(listStatusResponse.getStatusesCount());
    for (OzoneFileStatusProto fileStatus : listStatusResponse
        .getStatusesList()) {
      statusList.add(OzoneFileStatus.getFromProtobuf(fileStatus));
    }
    return statusList;
  }

  @Override
  public List<OzoneFileStatus> listStatus(OmKeyArgs args, boolean recursive,
      String startKey, long numEntries) throws IOException {
    return listStatus(args, recursive, startKey, numEntries, false);
  }

  @Override
  public List<RepeatedOmKeyInfo> listTrash(String volumeName,
      String bucketName, String startKeyName, String keyPrefix, int maxKeys)
      throws IOException {

    Preconditions.checkArgument(Strings.isNullOrEmpty(volumeName),
        "The volume name cannot be null or " +
        "empty.  Please enter a valid volume name or use '*' as a wild card");

    Preconditions.checkArgument(Strings.isNullOrEmpty(bucketName),
        "The bucket name cannot be null or " +
        "empty.  Please enter a valid bucket name or use '*' as a wild card");

    ListTrashRequest trashRequest = ListTrashRequest.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStartKeyName(startKeyName)
        .setKeyPrefix(keyPrefix)
        .setMaxKeys(maxKeys)
        .build();

    OMRequest omRequest = createOMRequest(Type.ListTrash)
        .setListTrashRequest(trashRequest)
        .build();

    ListTrashResponse trashResponse =
        handleError(submitRequest(omRequest)).getListTrashResponse();

    List<RepeatedOmKeyInfo> deletedKeyList =
        new ArrayList<>(trashResponse.getDeletedKeysCount());

    List<RepeatedOmKeyInfo> list = new ArrayList<>();
    for (OzoneManagerProtocolProtos.RepeatedKeyInfo
        repeatedKeyInfo : trashResponse.getDeletedKeysList()) {
      RepeatedOmKeyInfo fromProto =
          RepeatedOmKeyInfo.getFromProto(repeatedKeyInfo);
      list.add(fromProto);
    }
    deletedKeyList.addAll(list);

    return deletedKeyList;
  }

  @Override
  public boolean recoverTrash(String volumeName, String bucketName,
      String keyName, String destinationBucket) throws IOException {

    Preconditions.checkArgument(Strings.isNullOrEmpty(volumeName),
        "The volume name cannot be null or empty. " +
        "Please enter a valid volume name.");

    Preconditions.checkArgument(Strings.isNullOrEmpty(bucketName),
        "The bucket name cannot be null or empty. " +
        "Please enter a valid bucket name.");

    Preconditions.checkArgument(Strings.isNullOrEmpty(keyName),
        "The key name cannot be null or empty. " +
        "Please enter a valid key name.");

    Preconditions.checkArgument(Strings.isNullOrEmpty(destinationBucket),
        "The destination bucket name cannot be null or empty. " +
        "Please enter a valid destination bucket name.");

    RecoverTrashRequest.Builder req = RecoverTrashRequest.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setDestinationBucket(destinationBucket);

    OMRequest omRequest = createOMRequest(Type.RecoverTrash)
        .setRecoverTrashRequest(req)
        .build();

    RecoverTrashResponse recoverResponse =
        handleError(submitRequest(omRequest)).getRecoverTrashResponse();

    return recoverResponse.getResponse();
  }

  @Override
  public long prepareOzoneManager(
      long txnApplyWaitTimeoutSeconds, long txnApplyCheckIntervalSeconds)
      throws IOException {
    Preconditions.checkArgument(txnApplyWaitTimeoutSeconds > 0,
        "txnApplyWaitTimeoutSeconds has to be > zero");

    Preconditions.checkArgument(txnApplyCheckIntervalSeconds > 0 &&
            txnApplyCheckIntervalSeconds < txnApplyWaitTimeoutSeconds / 2,
        "txnApplyCheckIntervalSeconds has to be > zero and < half "
            + "of txnApplyWaitTimeoutSeconds to make sense.");

    PrepareRequest prepareRequest =
        PrepareRequest.newBuilder().setArgs(
            PrepareRequestArgs.newBuilder()
                .setTxnApplyWaitTimeoutSeconds(txnApplyWaitTimeoutSeconds)
                .setTxnApplyCheckIntervalSeconds(txnApplyCheckIntervalSeconds)
                .build()).build();

    OMRequest omRequest = createOMRequest(Type.Prepare)
        .setPrepareRequest(prepareRequest).build();

    PrepareResponse prepareResponse =
        handleError(submitRequest(omRequest)).getPrepareResponse();
    return prepareResponse.getTxnID();
  }

  @Override
  public PrepareStatusResponse getOzoneManagerPrepareStatus(long txnId)
      throws IOException {
    PrepareStatusRequest prepareStatusRequest =
        PrepareStatusRequest.newBuilder().setTxnID(txnId).build();
    OMRequest omRequest = createOMRequest(Type.PrepareStatus)
        .setPrepareStatusRequest(prepareStatusRequest).build();
    PrepareStatusResponse prepareStatusResponse =
        handleError(submitRequest(omRequest)).getPrepareStatusResponse();
    return prepareStatusResponse;
  }

  @Override
  public CancelPrepareResponse cancelOzoneManagerPrepare() throws IOException {
    CancelPrepareRequest cancelPrepareRequest =
        CancelPrepareRequest.newBuilder().build();

    OMRequest omRequest = createOMRequest(Type.CancelPrepare)
        .setCancelPrepareRequest(cancelPrepareRequest).build();

    return handleError(submitRequest(omRequest)).getCancelPrepareResponse();
  }

  @Override
  public EchoRPCResponse echoRPCReq(byte[] payloadReq,
                                    int payloadSizeResp)
          throws IOException {
    EchoRPCRequest echoRPCRequest =
            EchoRPCRequest.newBuilder()
                    .setPayloadReq(ByteString.copyFrom(payloadReq))
                    .setPayloadSizeResp(payloadSizeResp)
                    .build();

    OMRequest omRequest = createOMRequest(Type.EchoRPC)
            .setEchoRPCRequest(echoRPCRequest).build();

    EchoRPCResponse echoRPCResponse =
            handleError(submitRequest(omRequest)).getEchoRPCResponse();
    return echoRPCResponse;
  }

  @VisibleForTesting
  public OmTransport getTransport() {
    return transport;
  }

  public boolean isS3AuthCheck() {
    return s3AuthCheck;
  }

  public void setS3AuthCheck(boolean s3AuthCheck) {
    this.s3AuthCheck = s3AuthCheck;
  }
}
