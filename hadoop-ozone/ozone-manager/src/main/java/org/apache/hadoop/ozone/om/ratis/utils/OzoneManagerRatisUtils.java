/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.ratis.utils;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.protobuf.ServiceException;
import java.io.File;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.server.ServerUtils;
import org.apache.hadoop.hdds.utils.HAUtils;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.hdds.utils.TransactionInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.exceptions.OMLeaderNotReadyException;
import org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException;
import org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.RaftServerStatus;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketCreateRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketDeleteRequest;
import org.apache.hadoop.ozone.om.request.bucket.OMBucketSetPropertyRequest;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketAddAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.bucket.acl.OMBucketSetAclRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMDirectoryCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequest;
import org.apache.hadoop.ozone.om.request.file.OMFileCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeysDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequest;
import org.apache.hadoop.ozone.om.request.key.OMAllocateBlockRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCommitRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyCreateRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyDeleteRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeyPurgeRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMKeyRenameRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMKeysRenameRequest;
import org.apache.hadoop.ozone.om.request.key.OMPathsPurgeRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.OMTrashRecoverRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyAddAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeyRemoveAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.OMKeySetAclRequestWithFSO;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixAddAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixRemoveAclRequest;
import org.apache.hadoop.ozone.om.request.key.acl.prefix.OMPrefixSetAclRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3InitiateMultipartUploadRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadAbortRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCommitPartRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequest;
import org.apache.hadoop.ozone.om.request.s3.multipart.S3MultipartUploadCompleteRequestWithFSO;
import org.apache.hadoop.ozone.om.request.s3.security.S3GetSecretRequest;
import org.apache.hadoop.ozone.om.request.s3.security.S3RevokeSecretRequest;
import org.apache.hadoop.ozone.om.request.security.OMCancelDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMGetDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.security.OMRenewDelegationTokenRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMCancelPrepareRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMFinalizeUpgradeRequest;
import org.apache.hadoop.ozone.om.request.upgrade.OMPrepareRequest;
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
import org.apache.ratis.protocol.RaftPeerId;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.HddsConfigKeys.OZONE_METADATA_DIRS;
import static org.apache.hadoop.ozone.OzoneConsts.OM_RATIS_SNAPSHOT_DIR;
import static org.apache.hadoop.ozone.om.OMConfigKeys.OZONE_OM_RATIS_SNAPSHOT_DIR;

/**
 * Utility class used by OzoneManager HA.
 */
public final class OzoneManagerRatisUtils {

  // TODO: Temporary workaround for OM upgrade path and will be replaced once
  //  upgrade HDDS-3698 story reaches consensus.
  private static boolean isBucketFSOptimized = false;

  private static final Logger LOG = LoggerFactory
      .getLogger(OzoneManagerRatisUtils.class);

  private OzoneManagerRatisUtils() {
  }

  /**
   * Sets enabled/disabled file system optimized path property. A true value
   * represents enabled, false represents disabled.
   *
   * @param enabledFSO enabled/disabled file system optimized
   */
  public static void setBucketFSOptimized(boolean enabledFSO) {
    OzoneManagerRatisUtils.isBucketFSOptimized = enabledFSO;
  }

  /**
   * Create OMClientRequest which encapsulates the OMRequest.
   * @param omRequest
   * @return OMClientRequest
   * @throws IOException
   */
  @SuppressWarnings("checkstyle:methodlength")
  public static OMClientRequest createClientRequest(OMRequest omRequest,
      OzoneManager ozoneManager) {

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
      return new OMBucketSetPropertyRequest(omRequest);
    case AllocateBlock:
      keyArgs = omRequest.getAllocateBlockRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMAllocateBlockRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMAllocateBlockRequest(omRequest, bucketLayout);
    case CreateKey:
      keyArgs = omRequest.getCreateKeyRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMKeyCreateRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMKeyCreateRequest(omRequest, bucketLayout);
    case CommitKey:
      keyArgs = omRequest.getCommitKeyRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMKeyCommitRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMKeyCommitRequest(omRequest, bucketLayout);
    case DeleteKey:
      keyArgs = omRequest.getDeleteKeyRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMKeyDeleteRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMKeyDeleteRequest(omRequest, bucketLayout);
    case DeleteKeys:
      return new OMKeysDeleteRequest(omRequest);
    case RenameKey:
      keyArgs = omRequest.getRenameKeyRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMKeyRenameRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMKeyRenameRequest(omRequest, bucketLayout);
    case RenameKeys:
      return new OMKeysRenameRequest(omRequest);
    case CreateDirectory:
      keyArgs = omRequest.getCreateDirectoryRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMDirectoryCreateRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMDirectoryCreateRequest(omRequest, bucketLayout);
    case CreateFile:
      keyArgs = omRequest.getCreateFileRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new OMFileCreateRequestWithFSO(omRequest, bucketLayout);
      }
      return new OMFileCreateRequest(omRequest);
    case PurgeKeys:
      return new OMKeyPurgeRequest(omRequest);
    case PurgePaths:
      return new OMPathsPurgeRequestWithFSO(omRequest,
          BucketLayout.FILE_SYSTEM_OPTIMIZED);
    case InitiateMultiPartUpload:
      keyArgs = omRequest.getInitiateMultiPartUploadRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new S3InitiateMultipartUploadRequestWithFSO(omRequest,
            bucketLayout);
      }
      return new S3InitiateMultipartUploadRequest(omRequest, bucketLayout);
    case CommitMultiPartUpload:
      keyArgs = omRequest.getCommitMultiPartUploadRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new S3MultipartUploadCommitPartRequestWithFSO(omRequest,
            bucketLayout);
      }
      return new S3MultipartUploadCommitPartRequest(omRequest, bucketLayout);
    case AbortMultiPartUpload:
      keyArgs = omRequest.getAbortMultiPartUploadRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new S3MultipartUploadAbortRequestWithFSO(omRequest,
            bucketLayout);
      }
      return new S3MultipartUploadAbortRequest(omRequest, bucketLayout);
    case CompleteMultiPartUpload:
      keyArgs = omRequest.getCompleteMultiPartUploadRequest().getKeyArgs();
      bucketLayout = getBucketLayout(keyArgs.getVolumeName(),
          keyArgs.getBucketName(), ozoneManager);
      if (bucketLayout.isFileSystemOptimized()) {
        return new S3MultipartUploadCompleteRequestWithFSO(omRequest,
            bucketLayout);
      }
      return new S3MultipartUploadCompleteRequest(omRequest, bucketLayout);
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
    case RecoverTrash:
      return new OMTrashRecoverRequest(omRequest);
    case FinalizeUpgrade:
      return new OMFinalizeUpgradeRequest(omRequest);
    case Prepare:
      return new OMPrepareRequest(omRequest);
    case CancelPrepare:
      return new OMCancelPrepareRequest(omRequest);
    case RevokeS3Secret:
      return new S3RevokeSecretRequest(omRequest);
    default:
      throw new IllegalStateException("Unrecognized write command " +
          "type request" + cmdType);
    }
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
        OMKeyAddAclRequest aclReq = new OMKeyAddAclRequest(omRequest);
        BucketLayout bucketLayout = aclReq.getBucketLayout(ozoneManager);
        if (bucketLayout.isFileSystemOptimized()) {
          return new OMKeyAddAclRequestWithFSO(omRequest);
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
        OMKeyRemoveAclRequest aclReq = new OMKeyRemoveAclRequest(omRequest);
        BucketLayout bucketLayout = aclReq.getBucketLayout(ozoneManager);
        if (bucketLayout.isFileSystemOptimized()) {
          return new OMKeyRemoveAclRequestWithFSO(omRequest);
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
        OMKeySetAclRequest aclReq = new OMKeySetAclRequest(omRequest);
        BucketLayout bucketLayout = aclReq.getBucketLayout(ozoneManager);
        if (bucketLayout.isFileSystemOptimized()) {
          return new OMKeySetAclRequestWithFSO(omRequest);
        }
        return aclReq;
      } else {
        return new OMPrefixSetAclRequest(omRequest);
      }
    }
  }

  /**
   * Convert exception result to {@link OzoneManagerProtocolProtos.Status}.
   * @param exception
   * @return OzoneManagerProtocolProtos.Status
   */
  public static Status exceptionToResponseStatus(IOException exception) {
    if (exception instanceof OMException) {
      return Status.values()[((OMException) exception).getResult().ordinal()];
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
    return HAUtils
        .getTrxnInfoFromCheckpoint(conf, dbPath, new OMDBDefinition());
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
   * Returns enabled/disabled file system optimized path property. A true value
   * represents FSO path is enabled, false represents disabled.
   *
   * @return true or false.
   */
  public static boolean isBucketFSOptimized() {
    return isBucketFSOptimized;
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
          OM_RATIS_SNAPSHOT_DIR).toString();
    }
    return snapshotDir;
  }

  public static void checkLeaderStatus(RaftServerStatus raftServerStatus,
      RaftPeerId raftPeerId) throws ServiceException {
    switch (raftServerStatus) {
    case LEADER_AND_READY: return;

    case LEADER_AND_NOT_READY: throw createLeaderNotReadyException(raftPeerId);

    case NOT_LEADER: throw createNotLeaderException(raftPeerId);

    default: throw new IllegalStateException(
        "Unknown Ratis Server state: " + raftServerStatus);
    }
  }

  private static ServiceException createNotLeaderException(
      RaftPeerId raftPeerId) {

    // TODO: Set suggest leaderID. Right now, client is not using suggest
    // leaderID. Need to fix this.

    OMNotLeaderException notLeaderException =
        new OMNotLeaderException(raftPeerId);

    LOG.debug(notLeaderException.getMessage());

    return new ServiceException(notLeaderException);
  }

  private static ServiceException createLeaderNotReadyException(
      RaftPeerId raftPeerId) {

    OMLeaderNotReadyException leaderNotReadyException =
        new OMLeaderNotReadyException(raftPeerId.toString() + " is Leader " +
            "but not ready to process request yet.");

    LOG.debug(leaderNotReadyException.getMessage());

    return new ServiceException(leaderNotReadyException);
  }

  /**
   * All the client requests are executed through
   * OzoneManagerStateMachine#runCommand function and ensures sequential
   * execution path.
   * Below is the call trace to perform OM client request operation:
   * OzoneManagerStateMachine#applyTransaction ->
   * OzoneManagerStateMachine#runCommand ->
   * OzoneManagerRequestHandler#handleWriteRequest ->
   * OzoneManagerRatisUtils#createClientRequest ->
   * OzoneManagerRatisUtils#getOmBucketInfo ->
   * omMetadataManager().getBucketTable().get(buckKey)
   */

  private static OmBucketInfo getOmBucketInfo(OzoneManager ozoneManager,
      String volName, String buckName) {
    String buckKey =
        ozoneManager.getMetadataManager().getBucketKey(volName, buckName);
    OmBucketInfo buckInfo = null;
    try {
      buckInfo =
          ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    } catch (IOException e) {
      LOG.debug("Failed to get the value for the key: " + buckKey);
    }
    return buckInfo;
  }

  private static BucketLayout getBucketLayout(String volName,
      String buckName, OzoneManager ozoneManager) {
    OmBucketInfo buckInfo = getOmBucketInfo(ozoneManager, volName, buckName);
    if (buckInfo != null) {
      return buckInfo.getBucketLayout();
    } else {
      LOG.error("Bucket not found: {}/{} ", volName, buckName);
      // TODO: Handle bucket validation
    }
    return BucketLayout.LEGACY;
  }
}
