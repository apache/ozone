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

package org.apache.hadoop.ozone.om.ratis.utils;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.OZONE_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OzoneManagerUtils.getBucketLayout;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ServiceException;
import java.io.File;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.security.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.BucketLayoutAwareOMKeyRequestFactory;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketAddAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketSetAclRequest;
import org.apache.hadoop.ozone.om.request.file.OMRecoverLeaseRequest;
import org.apache.hadoop.ozone.om.request.key.OMDirectoriesPurgeRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.om.request.key.OMOpenKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixSetAclRequest;
import org.apache.hadoop.ozone.om.request.lifecycle.OMLifecycleConfigurationDeleteRequest;
import org.apache.hadoop.ozone.om.request.lifecycle.OMLifecycleConfigurationSetRequest;
import org.apache.hadoop.ozone.om.request.lifecycle.OMLifecycleSetServiceStatusRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3ExpiredMultipartUploadsAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.security.OMSetSecretRequest;
import org.apache.hadoop.ozone.om.request.s3.security.S3GetSecretRequest;
import org.apache.hadoop.ozone.om.request.s3.security.S3RevokeSecretRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMSetRangerServiceVersionRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantAssignAdminRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantAssignUserAccessIdRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantCreateRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantDeleteRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRevokeAdminRequest;
import org.apache.hadoop.ozone.om.request.s3.tenant.OMTenantRevokeUserAccessIdRequest;
import org.apache.hadoop.ozone.om.request.security.OMCancelDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMGetDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMRenewDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotCreateRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotDeleteRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotMoveDeletedKeysRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotMoveTableKeysRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotPurgeRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotRenameRequest;
import org.apache.hadoop.ozone.om.request.snapshot.OMSnapshotSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMCancelPrepareRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMFinalizeUpgradeRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMPrepareRequest;
import org.apache.hadoop.ozone.om.request.util.OMEchoRPCWriteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMQuotaRepairRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeCreateRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeDeleteRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetOwnerRequest;
import org.apache.hadoop.ozone.om.request.volume.OMVolumeSetQuotaRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeAddAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.volume.acl.OMVolumeSetAclRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneObj.ObjectType;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.apache.ratis.grpc.GrpcTlsConfig;
import org.apache.ratis.protocol.ClientId;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneManagerRatisUtils {
  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerRatisUtils.class);

  private OzoneManagerRatisUtils() {
  }

  /**
   * Create OMClientRequest which encapsulates the OMRequest.
   * @param omRequest
   * @return OMClientRequest
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:methodlength")
  public static OMClientRequest createClientRequest(OMRequest omRequest,
      OzoneManager ozoneManager) throws IOException {

    // Handling of exception by createClientRequest(OMRequest, OzoneManger):
    // Either the code will take FSO or non FSO path, both classes has a
    // validateAndUpdateCache() function which also contains
    // validateBucketAndVolume() function which validates bucket and volume and
    // throws necessary exceptions if required. validateAndUpdateCache()
    // function has catch block which catches the exception if required and
    // handles it appropriately.
    Type cmdType = omRequest.getCmdType();
    OzoneManagerProtocolProtos.KeyArgs keyArgs;
    BucketLayout bucketLayout = BucketLayout.DEFAULT;
    String volumeName = "";
    String bucketName = "";

    switch (cmdType) {
    case CreateVolume:
      return new OMVolumeCreateRequest(omRequest);
    case SetVolumeProperty:
      boolean hasQuota = omRequest.getSetVolumePropertyRequest()
          .hasQuotaInBytes();
      boolean hasOwner = omRequest.getSetVolumePropertyRequest().hasOwnerName();
      Preconditions.checkState(hasOwner || hasQuota, "Either Quota or owner " +
          "should be set in the SetVolumeProperty request");
      Preconditions.checkState(!(hasOwner && hasQuota), "Either Quota or " +
          "owner should be set in the SetVolumeProperty request. Should not " +
          "set both");
      if (hasQuota) {
        return new OMVolumeSetQuotaRequest(omRequest);
      } else {
        return new OMVolumeSetOwnerRequest(omRequest);
      }
    case DeleteVolume:
      return new OMVolumeDeleteRequest(omRequest);
    case CreateBucket:
      return new OMBucketCreateRequest(omRequest);
    case DeleteBucket:
      return new OMBucketDeleteRequest(omRequest);
    case SetBucketProperty:
      boolean hasBucketOwner = omRequest.getSetBucketPropertyRequest()
          .getBucketArgs().hasOwnerName();
      if (hasBucketOwner) {
        return new OMBucketSetOwnerRequest(omRequest);
      } else {
        return new OMBucketSetPropertyRequest(omRequest);
      }
    case AddAcl:
    case RemoveAcl:
    case SetAcl:
      return getOMAclRequest(omRequest, ozoneManager);
    case GetDelegationToken:
      return new OMGetDelegationTokenRequest(omRequest);
    case CancelDelegationToken:
      return new OMCancelDelegationTokenRequest(omRequest);
    case RenewDelegationToken:
      return new OMRenewDelegationTokenRequest(omRequest);
    case GetS3Secret:
      return new S3GetSecretRequest(omRequest);
    case FinalizeUpgrade:
      return new OMFinalizeUpgradeRequest(omRequest);
    case Prepare:
      return new OMPrepareRequest(omRequest);
    case CancelPrepare:
      return new OMCancelPrepareRequest(omRequest);
    case SetS3Secret:
      return new OMSetSecretRequest(omRequest);
    case RevokeS3Secret:
      return new S3RevokeSecretRequest(omRequest);
    case PurgeKeys:
      return new OMKeyPurgeRequest(omRequest);
    case PurgeDirectories:
      return new OMDirectoriesPurgeRequestWithFSO(omRequest);
    case CreateTenant:
      ozoneManager.checkS3MultiTenancyEnabled();
      return new OMTenantCreateRequest(omRequest);
    case DeleteTenant:
      ozoneManager.checkS3MultiTenancyEnabled();
      return new OMTenantDeleteRequest(omRequest);
    case TenantAssignUserAccessId:
      ozoneManager.checkS3MultiTenancyEnabled();
      return new OMTenantAssignUserAccessIdRequest(omRequest);
    case TenantRevokeUserAccessId:
      ozoneManager.checkS3MultiTenancyEnabled();
      return new OMTenantRevokeUserAccessIdRequest(omRequest);
    case TenantAssignAdmin:
      ozoneManager.checkS3MultiTenancyEnabled();
      return new OMTenantAssignAdminRequest(omRequest);
    case TenantRevokeAdmin:
      ozoneManager.checkS3MultiTenancyEnabled();
      return new OMTenantRevokeAdminRequest(omRequest);
    case SetRangerServiceVersion:
      return new OMSetRangerServiceVersionRequest(omRequest);
    case CreateSnapshot:
      return new OMSnapshotCreateRequest(omRequest);
    case DeleteSnapshot:
      return new OMSnapshotDeleteRequest(omRequest);
    case RenameSnapshot:
      return new OMSnapshotRenameRequest(omRequest);
    case SnapshotMoveDeletedKeys:
      return new OMSnapshotMoveDeletedKeysRequest(omRequest);
    case SnapshotMoveTableKeys:
      return new OMSnapshotMoveTableKeysRequest(omRequest);
    case SnapshotPurge:
      return new OMSnapshotPurgeRequest(omRequest);
    case SetSnapshotProperty:
      return new OMSnapshotSetPropertyRequest(omRequest);
    case DeleteOpenKeys:
      BucketLayout bktLayout = BucketLayout.DEFAULT;
      if (omRequest.getDeleteOpenKeysRequest().hasBucketLayout()) {
        bktLayout = BucketLayout.fromProto(
            omRequest.getDeleteOpenKeysRequest().getBucketLayout());
      }
      return new OMOpenKeysDeleteRequest(omRequest, bktLayout);
    case RecoverLease:
      volumeName = omRequest.getRecoverLeaseRequest().getVolumeName();
      bucketName = omRequest.getRecoverLeaseRequest().getBucketName();
      bucketLayout =
        getBucketLayout(ozoneManager.getMetadataManager(), volumeName,
          bucketName);
      if (bucketLayout != BucketLayout.FILE_SYSTEM_OPTIMIZED) {
        throw new IOException("Bucket " + bucketName + " is not FSO layout. " +
                "It does not support lease recovery");
      }
      return new OMRecoverLeaseRequest(omRequest);
    /*
     * Key requests that can have multiple variants based on the bucket layout
     * should be created using {@link BucketLayoutAwareOMKeyRequestFactory}.
     */
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case DeleteKeys:
      OzoneManagerProtocolProtos.DeleteKeyArgs deleteKeyArgs =
          omRequest.getDeleteKeysRequest()
              .getDeleteKeys();
      volumeName = deleteKeyArgs.getVolumeName();
      bucketName = deleteKeyArgs.getBucketName();
      break;
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case RenameKeys:
      OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs =
          omRequest.getRenameKeysRequest().getRenameKeysArgs();
      volumeName = renameKeysArgs.getVolumeName();
      bucketName = renameKeysArgs.getBucketName();
      break;
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case SetTimes:
      keyArgs = omRequest.getSetTimesRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case EchoRPC:
      return new OMEchoRPCWriteRequest(omRequest);
    case AbortExpiredMultiPartUploads:
      return new S3ExpiredMultipartUploadsAbortRequest(omRequest);
    case QuotaRepair:
      return new OMQuotaRepairRequest(omRequest);
    case PutObjectTagging:
      keyArgs = omRequest.getPutObjectTaggingRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case DeleteObjectTagging:
      keyArgs = omRequest.getDeleteObjectTaggingRequest().getKeyArgs();
      volumeName = keyArgs.getVolumeName();
      bucketName = keyArgs.getBucketName();
      break;
    case SetLifecycleConfiguration:
      return new OMLifecycleConfigurationSetRequest(omRequest);
    case DeleteLifecycleConfiguration:
      return new OMLifecycleConfigurationDeleteRequest(omRequest);
    case SetLifecycleServiceStatus:
      return new OMLifecycleSetServiceStatusRequest(omRequest);
    default:
      throw new OMException("Unrecognized write command type request "
          + cmdType, OMException.ResultCodes.INVALID_REQUEST);
    }

    return BucketLayoutAwareOMKeyRequestFactory.createRequest(
        volumeName, bucketName, omRequest, ozoneManager.getMetadataManager());
  }

  private static OMClientRequest getOMAclRequest(OMRequest omRequest,
      OzoneManager ozoneManager) {
    Type cmdType = omRequest.getCmdType();
    if (Type.AddAcl == cmdType) {
      ObjectType type = omRequest.getAddAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        return new OMVolumeAddAclRequest(omRequest);
      } else if (ObjectType.BUCKET == type) {
        return new OMBucketAddAclRequest(omRequest);
      } else if (ObjectType.KEY == type) {
        OMKeyAddAclRequest aclReq =
            new OMKeyAddAclRequest(omRequest, ozoneManager);
        if (aclReq.getBucketLayout().isFileSystemOptimized()) {
          return new OMKeyAddAclRequestWithFSO(omRequest,
              aclReq.getBucketLayout());
        }
        return aclReq;
      } else {
        return new OMPrefixAddAclRequest(omRequest);
      }
    } else if (Type.RemoveAcl == cmdType) {
      ObjectType type = omRequest.getRemoveAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        return new OMVolumeRemoveAclRequest(omRequest);
      } else if (ObjectType.BUCKET == type) {
        return new OMBucketRemoveAclRequest(omRequest);
      } else if (ObjectType.KEY == type) {
        OMKeyRemoveAclRequest aclReq =
            new OMKeyRemoveAclRequest(omRequest, ozoneManager);
        if (aclReq.getBucketLayout().isFileSystemOptimized()) {
          return new OMKeyRemoveAclRequestWithFSO(omRequest,
              aclReq.getBucketLayout());
        }
        return aclReq;
      } else {
        return new OMPrefixRemoveAclRequest(omRequest);
      }
    } else {
      ObjectType type = omRequest.getSetAclRequest().getObj().getResType();
      if (ObjectType.VOLUME == type) {
        return new OMVolumeSetAclRequest(omRequest);
      } else if (ObjectType.BUCKET == type) {
        return new OMBucketSetAclRequest(omRequest);
      } else if (ObjectType.KEY == type) {
        OMKeySetAclRequest aclReq =
            new OMKeySetAclRequest(omRequest, ozoneManager);
        if (aclReq.getBucketLayout().isFileSystemOptimized()) {
          return new OMKeySetAclRequestWithFSO(omRequest,
              aclReq.getBucketLayout());
        }
        return aclReq;
      } else {
        return new OMPrefixSetAclRequest(omRequest);
      }
    }
  }

  /**
   * Convert exception result to {@link org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status}.
   * @param exception
   * @return Status
   */
  public static Status exceptionToResponseStatus(Exception exception) {
    if (exception instanceof OMException) {
      return Status.values()[((OMException) exception).getResult().ordinal()];
    } else if (exception instanceof InvalidPathException) {
      return Status.INVALID_PATH;
    } else {
      // Doing this here, because when DB error happens we need to return
      // correct error code, so that in applyTransaction we can
      // completeExceptionally for DB errors.

      // Currently Table API's are in hdds-common, which are used by SCM, OM
      // currently. So, they return IOException with setting cause to
      // RocksDBException. So, here that is the reason for the instanceof
      // check RocksDBException.
      if (exception.getCause() != null
          && exception.getCause() instanceof RocksDBException) {
        return Status.METADATA_ERROR;
      } else {
        return Status.INTERNAL_ERROR;
      }
    }
  }

  /**
   * Obtain OMTransactionInfo from Checkpoint.
   */
  public static TransactionInfo getTrxnInfoFromCheckpoint(
      OzoneConfiguration conf, Path dbPath) throws Exception {
    return HAUtils.getTrxnInfoFromCheckpoint(conf, dbPath, OMDBDefinition.get());
  }

  /**
   * Verify transaction info with provided lastAppliedIndex.
   *
   * If transaction info transaction Index is less than or equal to
   * lastAppliedIndex, return false, else return true.
   * @param transactionInfo
   * @param lastAppliedIndex
   * @param leaderId
   * @param newDBlocation
   * @return boolean
   */
  public static boolean verifyTransactionInfo(TransactionInfo transactionInfo,
      long lastAppliedIndex, String leaderId, Path newDBlocation) {
    return HAUtils
        .verifyTransactionInfo(transactionInfo, lastAppliedIndex, leaderId,
            newDBlocation, OzoneManager.LOG);
  }

  /**
   * Get the local directory where ratis logs will be stored.
   */
  public static String getOMRatisDirectory(ConfigurationSource conf) {
    String storageDir = conf.get(OMConfigKeys.OZONE_OM_RATIS_STORAGE_DIR);

    if (Strings.isNullOrEmpty(storageDir)) {
      storageDir = ServerUtils.getDefaultRatisDirectory(conf);
    }
    return storageDir;
  }

  /**
   * Get the local directory where ratis snapshots will be stored.
   */
  public static String getOMRatisSnapshotDirectory(ConfigurationSource conf) {
    String snapshotDir = conf.get(OZONE_OM_RATIS_SNAPSHOT_DIR);

    // If ratis snapshot directory is not set, fall back to ozone.metadata.dir.
    if (Strings.isNullOrEmpty(snapshotDir)) {
      LOG.warn("{} is not configured. Falling back to {} config",
          OZONE_OM_RATIS_SNAPSHOT_DIR, OZONE_METADATA_DIRS);
      File metaDirPath = ServerUtils.getOzoneMetaDirPath(conf);
      snapshotDir = Paths.get(metaDirPath.getPath(),
          OZONE_RATIS_SNAPSHOT_DIR).toString();
    }
    return snapshotDir;
  }

  public static void checkLeaderStatus(OzoneManager ozoneManager)
      throws ServiceException {
    try {
      ozoneManager.checkLeaderStatus();
    } catch (OMNotLeaderException | OMLeaderNotReadyException e) {
      LOG.debug(e.getMessage());
      throw new ServiceException(e);
    }
  }

  public static GrpcTlsConfig createServerTlsConfig(SecurityConfig conf,
      CertificateClient caClient) throws IOException {
    if (conf.isSecurityEnabled() && conf.isGrpcTlsEnabled()) {
      return new GrpcTlsConfig(caClient.getKeyManager(), caClient.getTrustManager(), true);
    }

    return null;
  }

  public static OzoneManagerProtocolProtos.OMResponse submitRequest(
      OzoneManager om, OMRequest omRequest, ClientId clientId, long callId) throws ServiceException {
    return om.getOmRatisServer().submitRequest(omRequest, clientId, callId);
  }

  public static OzoneManagerProtocolProtos.OMResponse createErrorResponse(
      OMRequest omRequest, IOException exception) {
    // Added all write command types here, because in future if any of the
    // preExecute is changed to return IOException, we can return the error
    // OMResponse to the client.
    OzoneManagerProtocolProtos.OMResponse.Builder omResponse = OzoneManagerProtocolProtos.OMResponse.newBuilder()
        .setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(exception))
        .setCmdType(omRequest.getCmdType())
        .setTraceID(omRequest.getTraceID())
        .setSuccess(false);
    if (exception.getMessage() != null) {
      omResponse.setMessage(exception.getMessage());
    }
    return omResponse.build();
  }
}
