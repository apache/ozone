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
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.om.lock;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmDBTenantState;
import org.apache.hadoop.ozone.om.lock.OmLockOpr.LockType;
import org.apache.hadoop.ozone.om.request.util.ObjectParser;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

/**
 * Map request lock for lock and unlock.
 */
public final class OmRequestLockUtils {
  private static Map<OzoneManagerProtocolProtos.Type,
      BiFunction<OzoneManager, OzoneManagerProtocolProtos.OMRequest, OmLockOpr>> reqLockMap = new HashMap<>();
  private static final OmLockOpr NO_LOCK = new OmLockOpr();
  private static final OmLockOpr EXP_LOCK = new OmLockOpr();

  private OmRequestLockUtils() {
  }

  public static void init() {
    reqLockMap.put(Type.CreateVolume, (om, req) ->
        new OmLockOpr(LockType.W_VOLUME, req.getCreateVolumeRequest().getVolumeInfo().getVolume()));
    reqLockMap.put(Type.SetVolumeProperty, (om, req) ->
        new OmLockOpr(LockType.R_VOLUME, req.getSetVolumePropertyRequest().getVolumeName()));
    reqLockMap.put(Type.DeleteVolume, (om, req) ->
        new OmLockOpr(LockType.W_VOLUME, req.getDeleteVolumeRequest().getVolumeName()));
    reqLockMap.put(Type.CreateBucket, (om, req) -> new OmLockOpr(LockType.RW_VOLUME_BUCKET,
        req.getCreateBucketRequest().getBucketInfo().getVolumeName(),
        req.getCreateBucketRequest().getBucketInfo().getBucketName()));
    reqLockMap.put(Type.DeleteBucket, (om, req) ->
        new OmLockOpr(LockType.W_BUCKET, req.getDeleteBucketRequest().getBucketName()));
    reqLockMap.put(Type.SetBucketProperty, (om, req) ->
        new OmLockOpr(LockType.W_BUCKET, req.getSetBucketPropertyRequest().getBucketArgs().getBucketName()));
    reqLockMap.put(Type.AddAcl, (om, req) -> aclLockInfo(om, req.getAddAclRequest().getObj()));
    reqLockMap.put(Type.RemoveAcl, (om, req) -> aclLockInfo(om, req.getAddAclRequest().getObj()));
    reqLockMap.put(Type.SetAcl, (om, req) -> aclLockInfo(om, req.getAddAclRequest().getObj()));
    reqLockMap.put(Type.GetS3Secret, (om, req) -> new OmLockOpr(LockType.WRITE,
        req.getGetS3SecretRequest().getKerberosID()));
    reqLockMap.put(Type.SetS3Secret, (om, req) -> new OmLockOpr(LockType.WRITE,
        req.getSetS3SecretRequest().getAccessId()));
    reqLockMap.put(Type.RevokeS3Secret, (om, req) -> new OmLockOpr(LockType.WRITE,
        req.getRevokeS3SecretRequest().getKerberosID()));
    reqLockMap.put(Type.GetDelegationToken, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.CancelDelegationToken, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.RenewDelegationToken, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.FinalizeUpgrade, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.Prepare, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.CancelPrepare, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.SetRangerServiceVersion, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.EchoRPC, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.QuotaRepair, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.AbortExpiredMultiPartUploads, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.PurgeKeys, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.PurgeDirectories, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.SnapshotMoveDeletedKeys, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.SnapshotMoveTableKeys, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.SnapshotPurge, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.SetSnapshotProperty, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.DeleteOpenKeys, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.PersistDb, (om, req) -> NO_LOCK);
    reqLockMap.put(Type.CreateTenant, (om, req) ->
        new OmLockOpr(LockType.W_VOLUME, req.getCreateTenantRequest().getVolumeName()));
    reqLockMap.put(Type.DeleteTenant, (om, req) ->
        getTenantLock(om, LockType.W_VOLUME, req.getDeleteTenantRequest().getTenantId()));
    reqLockMap.put(Type.TenantAssignUserAccessId, (om, req) ->
        getTenantLock(om, LockType.R_VOLUME, req.getTenantAssignUserAccessIdRequest().getTenantId()));
    reqLockMap.put(Type.TenantRevokeUserAccessId, (om, req) ->
        getTenantLock(om, LockType.R_VOLUME, req.getTenantRevokeUserAccessIdRequest().getTenantId()));
    reqLockMap.put(Type.TenantAssignAdmin, (om, req) ->
        getTenantLock(om, LockType.R_VOLUME, req.getTenantAssignAdminRequest().getTenantId()));
    reqLockMap.put(Type.TenantRevokeAdmin, (om, req) ->
        getTenantLock(om, LockType.R_VOLUME, req.getTenantRevokeAdminRequest().getTenantId()));
    reqLockMap.put(Type.CreateSnapshot, (om, req) -> new OmLockOpr(LockType.SNAPSHOT,
        req.getCreateSnapshotRequest().getVolumeName(), req.getCreateSnapshotRequest().getBucketName(),
        req.getCreateSnapshotRequest().getSnapshotName()));
    reqLockMap.put(Type.DeleteSnapshot, (om, req) -> new OmLockOpr(LockType.SNAPSHOT,
        req.getDeleteSnapshotRequest().getVolumeName(), req.getDeleteSnapshotRequest().getBucketName(),
        req.getDeleteSnapshotRequest().getSnapshotName()));
    reqLockMap.put(Type.RenameSnapshot, (om, req) -> new OmLockOpr(LockType.SNAPSHOT,
        req.getRenameSnapshotRequest().getVolumeName(), req.getRenameSnapshotRequest().getBucketName(),
        Arrays.asList(req.getRenameSnapshotRequest().getSnapshotOldName(),
            req.getRenameSnapshotRequest().getSnapshotNewName())));
    reqLockMap.put(Type.RecoverLease, (om, req) -> new OmLockOpr(LockType.W_FSO,
        req.getRecoverLeaseRequest().getVolumeName(), req.getRecoverLeaseRequest().getBucketName(),
        req.getRecoverLeaseRequest().getKeyName()));
    reqLockMap.put(Type.CreateDirectory, (om, req) -> getKeyLock(om, req.getCreateDirectoryRequest().getKeyArgs()));
    reqLockMap.put(Type.CreateFile, (om, req) -> getKeyLock(om, req.getCreateFileRequest().getKeyArgs()));
    reqLockMap.put(Type.CreateKey, (om, req) -> getKeyLock(om, req.getCreateKeyRequest().getKeyArgs()));
    reqLockMap.put(Type.AllocateBlock, (om, req) -> getKeyLock(om, req.getAllocateBlockRequest().getKeyArgs()));
    reqLockMap.put(Type.CommitKey, (om, req) -> getKeyLock(om, req.getCommitKeyRequest().getKeyArgs()));
    reqLockMap.put(Type.DeleteKey, (om, req) -> getKeyLock(om, req.getDeleteKeyRequest().getKeyArgs()));
    reqLockMap.put(Type.DeleteKeys, (om, req) -> getDeleteKeyLock(om, req.getDeleteKeysRequest().getDeleteKeys()));
    reqLockMap.put(Type.RenameKey, (om, req) -> getRenameKey(om, req.getRenameKeyRequest()));
    reqLockMap.put(Type.RenameKeys, (om, req) -> {
      OzoneManagerProtocolProtos.RenameKeysArgs renameKeysArgs = req.getRenameKeysRequest().getRenameKeysArgs();
      List<String> lockKeyList = new ArrayList<>();
      renameKeysArgs.getRenameKeysMapList().forEach(e -> {
        lockKeyList.add(e.getFromKeyName());
        lockKeyList.add(e.getToKeyName());
      });
      return new OmLockOpr(LockType.W_OBS, renameKeysArgs.getVolumeName(), renameKeysArgs.getBucketName(), lockKeyList);
    });
    reqLockMap.put(Type.InitiateMultiPartUpload, (om, req) ->
        getKeyLock(om, req.getInitiateMultiPartUploadRequest().getKeyArgs()));
    reqLockMap.put(Type.CommitMultiPartUpload, (om, req) ->
        getKeyLock(om, req.getCommitMultiPartUploadRequest().getKeyArgs()));
    reqLockMap.put(Type.AbortMultiPartUpload, (om, req) ->
        getKeyLock(om, req.getAbortMultiPartUploadRequest().getKeyArgs()));
    reqLockMap.put(Type.CompleteMultiPartUpload, (om, req) ->
        getKeyLock(om, req.getCompleteMultiPartUploadRequest().getKeyArgs()));
    reqLockMap.put(Type.SetTimes, (om, req) -> getKeyLock(om, req.getSetTimesRequest().getKeyArgs()));
  }
  
  private static OmLockOpr aclLockInfo(OzoneManager om, OzoneManagerProtocolProtos.OzoneObj obj) {
    try {
      if (obj.getResType() == OzoneManagerProtocolProtos.OzoneObj.ObjectType.VOLUME) {
        return new OmLockOpr(LockType.R_VOLUME, obj.getPath().substring(1));
      } else if (obj.getResType() == OzoneManagerProtocolProtos.OzoneObj.ObjectType.BUCKET) {
        ObjectParser objParser = new ObjectParser(obj.getPath(), OzoneManagerProtocolProtos.OzoneObj.ObjectType.BUCKET);
        return new OmLockOpr(LockType.RW_VOLUME_BUCKET, objParser.getVolume(), objParser.getBucket());
      } else if (obj.getResType() == OzoneManagerProtocolProtos.OzoneObj.ObjectType.KEY) {
        ObjectParser objParser = new ObjectParser(obj.getPath(), OzoneManagerProtocolProtos.OzoneObj.ObjectType.KEY);
        String bucketKey = om.getMetadataManager().getBucketKey(objParser.getVolume(), objParser.getBucket());
        OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable().get(bucketKey);
        if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
          return new OmLockOpr(LockType.W_FSO, objParser.getVolume(), objParser.getBucket(), objParser.getKey());
        }
        if (bucketInfo.getBucketLayout().isObjectStore(om.getEnableFileSystemPaths())) {
          return new OmLockOpr(LockType.W_OBS, objParser.getVolume(), objParser.getBucket(), objParser.getKey());
        }
        // OBS and LegacyFso will follow same path for lock with full key path
        return new OmLockOpr(LockType.W_OBS, objParser.getVolume(), objParser.getBucket(), objParser.getKey());
      } else {
        // prefix type with full path
        return new OmLockOpr(LockType.WRITE, obj.getPath());
      }
    } catch (Exception ex) {
      return EXP_LOCK;
    }
  }
  
  private static OmLockOpr getTenantLock(OzoneManager om, LockType type, String tenantId) {
    try {
      OmDBTenantState omDBTenantState = om.getMetadataManager().getTenantStateTable().get(tenantId);
      return new OmLockOpr(type, omDBTenantState.getBucketNamespaceName());
    } catch (Exception ex) {
      return EXP_LOCK;
    }
  }

  private static OmLockOpr getKeyLock(OzoneManager om, OzoneManagerProtocolProtos.KeyArgs args) {
    try {
      String bucketKey = om.getMetadataManager().getBucketKey(args.getVolumeName(), args.getBucketName());
      OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable().get(bucketKey);
      if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
        return new OmLockOpr(LockType.W_FSO, args.getVolumeName(), args.getBucketName(), args.getKeyName());
      }
      if (bucketInfo.getBucketLayout().isObjectStore(om.getEnableFileSystemPaths())) {
        return new OmLockOpr(LockType.W_OBS, args.getVolumeName(), args.getBucketName(), args.getKeyName());
      }
      // as LegacyFSO, need extract all path to be created and do write lock for missing parent
      return new OmLockOpr(LockType.W_LEGACY_FSO, args.getVolumeName(), args.getBucketName(), args.getKeyName());
    } catch (Exception ex) {
      return EXP_LOCK;
    }
  }
  private static OmLockOpr getDeleteKeyLock(OzoneManager om, OzoneManagerProtocolProtos.DeleteKeyArgs args) {
    String bucketKey = om.getMetadataManager().getBucketKey(args.getVolumeName(), args.getBucketName());
    try {
      OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable().get(bucketKey);
      if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
        return new OmLockOpr(LockType.W_FSO, args.getVolumeName(), args.getBucketName(), args.getKeysList());
      }
      return new OmLockOpr(LockType.W_OBS, args.getVolumeName(), args.getBucketName(), args.getKeysList());
    } catch (IOException e) {
      return EXP_LOCK;
    }
  }
  private static OmLockOpr getRenameKey(OzoneManager om, OzoneManagerProtocolProtos.RenameKeyRequest req) {
    String bucketKey = om.getMetadataManager().getBucketKey(req.getKeyArgs().getVolumeName(),
        req.getKeyArgs().getBucketName());
    try {
      OmBucketInfo bucketInfo = om.getMetadataManager().getBucketTable().get(bucketKey);
      if (bucketInfo.getBucketLayout().isFileSystemOptimized()) {
        return new OmLockOpr(LockType.W_FSO, req.getKeyArgs().getVolumeName(), req.getKeyArgs().getBucketName(),
            Arrays.asList(req.getKeyArgs().getKeyName(), req.getToKeyName()));
      }
      return new OmLockOpr(LockType.W_OBS, req.getKeyArgs().getVolumeName(), req.getKeyArgs().getBucketName(),
          Arrays.asList(req.getKeyArgs().getKeyName(), req.getToKeyName()));
    } catch (IOException e) {
      return EXP_LOCK;
    }
  }
  
  public static OmLockOpr getLockOperation(OzoneManager om, OzoneManagerProtocolProtos.OMRequest req) {
    return reqLockMap.get(req.getCmdType()).apply(om, req);
  }
}
