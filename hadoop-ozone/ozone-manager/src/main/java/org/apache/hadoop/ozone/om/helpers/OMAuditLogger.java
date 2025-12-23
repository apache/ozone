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

package org.apache.hadoop.ozone.om.helpers;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.audit.OMAction;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ratis.server.protocol.TermIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is used for OM Audit logs.
 */
public final class OMAuditLogger {
  private OMAuditLogger() {
  }

  private static final Map<Type, OMAction> CMD_AUDIT_ACTION_MAP = new HashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(OMAuditLogger.class);

  static {
    init();
  }

  private static void init() {
    CMD_AUDIT_ACTION_MAP.put(Type.CreateVolume, OMAction.CREATE_VOLUME);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteVolume, OMAction.DELETE_VOLUME);
    CMD_AUDIT_ACTION_MAP.put(Type.CreateBucket, OMAction.CREATE_BUCKET);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteBucket, OMAction.DELETE_BUCKET);
    CMD_AUDIT_ACTION_MAP.put(Type.AddAcl, OMAction.ADD_ACL);
    CMD_AUDIT_ACTION_MAP.put(Type.RemoveAcl, OMAction.REMOVE_ACL);
    CMD_AUDIT_ACTION_MAP.put(Type.SetAcl, OMAction.SET_ACL);
    CMD_AUDIT_ACTION_MAP.put(Type.GetDelegationToken, OMAction.GET_DELEGATION_TOKEN);
    CMD_AUDIT_ACTION_MAP.put(Type.CancelDelegationToken, OMAction.CANCEL_DELEGATION_TOKEN);
    CMD_AUDIT_ACTION_MAP.put(Type.RenewDelegationToken, OMAction.RENEW_DELEGATION_TOKEN);
    CMD_AUDIT_ACTION_MAP.put(Type.GetS3Secret, OMAction.GET_S3_SECRET);
    CMD_AUDIT_ACTION_MAP.put(Type.SetS3Secret, OMAction.SET_S3_SECRET);
    CMD_AUDIT_ACTION_MAP.put(Type.RevokeS3Secret, OMAction.REVOKE_S3_SECRET);
    CMD_AUDIT_ACTION_MAP.put(Type.CreateTenant, OMAction.CREATE_TENANT);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteTenant, OMAction.DELETE_TENANT);
    CMD_AUDIT_ACTION_MAP.put(Type.TenantAssignUserAccessId, OMAction.TENANT_ASSIGN_USER_ACCESSID);
    CMD_AUDIT_ACTION_MAP.put(Type.TenantRevokeUserAccessId, OMAction.TENANT_REVOKE_USER_ACCESSID);
    CMD_AUDIT_ACTION_MAP.put(Type.TenantRevokeAdmin, OMAction.TENANT_REVOKE_ADMIN);
    CMD_AUDIT_ACTION_MAP.put(Type.CreateSnapshot, OMAction.CREATE_SNAPSHOT);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteSnapshot, OMAction.DELETE_SNAPSHOT);
    CMD_AUDIT_ACTION_MAP.put(Type.RenameSnapshot, OMAction.RENAME_SNAPSHOT);
    CMD_AUDIT_ACTION_MAP.put(Type.RecoverLease, OMAction.RECOVER_LEASE);
    CMD_AUDIT_ACTION_MAP.put(Type.CreateDirectory, OMAction.CREATE_DIRECTORY);
    CMD_AUDIT_ACTION_MAP.put(Type.CreateFile, OMAction.CREATE_FILE);
    CMD_AUDIT_ACTION_MAP.put(Type.CreateKey, OMAction.ALLOCATE_KEY);
    CMD_AUDIT_ACTION_MAP.put(Type.AllocateBlock, OMAction.ALLOCATE_BLOCK);
    CMD_AUDIT_ACTION_MAP.put(Type.CommitKey, OMAction.COMMIT_KEY);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteKey, OMAction.DELETE_KEY);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteKeys, OMAction.DELETE_KEYS);
    CMD_AUDIT_ACTION_MAP.put(Type.RenameKey, OMAction.RENAME_KEY);
    CMD_AUDIT_ACTION_MAP.put(Type.RenameKeys, OMAction.RENAME_KEYS);
    CMD_AUDIT_ACTION_MAP.put(Type.InitiateMultiPartUpload, OMAction.INITIATE_MULTIPART_UPLOAD);
    CMD_AUDIT_ACTION_MAP.put(Type.CommitMultiPartUpload, OMAction.COMMIT_MULTIPART_UPLOAD_PARTKEY);
    CMD_AUDIT_ACTION_MAP.put(Type.AbortMultiPartUpload, OMAction.ABORT_MULTIPART_UPLOAD);
    CMD_AUDIT_ACTION_MAP.put(Type.CompleteMultiPartUpload, OMAction.COMPLETE_MULTIPART_UPLOAD);
    CMD_AUDIT_ACTION_MAP.put(Type.SetTimes, OMAction.SET_TIMES);
    CMD_AUDIT_ACTION_MAP.put(Type.AbortExpiredMultiPartUploads, OMAction.ABORT_EXPIRED_MULTIPART_UPLOAD);
    CMD_AUDIT_ACTION_MAP.put(Type.SetVolumeProperty, OMAction.SET_OWNER);
    CMD_AUDIT_ACTION_MAP.put(Type.SetBucketProperty, OMAction.UPDATE_BUCKET);
    CMD_AUDIT_ACTION_MAP.put(Type.Prepare, OMAction.UPGRADE_PREPARE);
    CMD_AUDIT_ACTION_MAP.put(Type.CancelPrepare, OMAction.UPGRADE_CANCEL);
    CMD_AUDIT_ACTION_MAP.put(Type.FinalizeUpgrade, OMAction.UPGRADE_FINALIZE);
    CMD_AUDIT_ACTION_MAP.put(Type.GetObjectTagging, OMAction.GET_OBJECT_TAGGING);
    CMD_AUDIT_ACTION_MAP.put(Type.PutObjectTagging, OMAction.PUT_OBJECT_TAGGING);
    CMD_AUDIT_ACTION_MAP.put(Type.DeleteObjectTagging, OMAction.DELETE_OBJECT_TAGGING);
    CMD_AUDIT_ACTION_MAP.put(Type.SetLifecycleServiceStatus, OMAction.SET_LIFECYCLE_SERVICE_STATUS);
  }

  private static OMAction getAction(OzoneManagerProtocolProtos.OMRequest request) {
    Type cmdType = request.getCmdType();
    OMAction omAction = CMD_AUDIT_ACTION_MAP.get(cmdType);
    if (null != omAction) {
      if (cmdType.equals(Type.SetVolumeProperty)) {
        boolean hasQuota = request.getSetVolumePropertyRequest().hasQuotaInBytes();
        if (hasQuota) {
          return OMAction.SET_QUOTA;
        }
      }
      if (cmdType.equals(Type.SetBucketProperty)) {
        boolean hasBucketOwner = request.getSetBucketPropertyRequest().getBucketArgs().hasOwnerName();
        if (hasBucketOwner) {
          return OMAction.SET_OWNER;
        }
      }
    }
    return omAction;
  }

  public static void log(OMAuditLogger.Builder builder, TermIndex termIndex) {
    if (builder.isLog.get()) {
      if (null == builder.getAuditMap()) {
        builder.setAuditMap(new HashMap<>());
      }
      builder.getAuditMap().put("Transaction", String.valueOf(termIndex.getIndex()));
      builder.getMessageBuilder().withParams(builder.getAuditMap());
      builder.getAuditLogger().logWrite(builder.getMessageBuilder().build());
    }
  }

  public static void log(OMAuditLogger.Builder builder) {
    if (builder.isLog.get()) {
      builder.getMessageBuilder().withParams(builder.getAuditMap());
      builder.getAuditLogger().logWrite(builder.getMessageBuilder().build());
    }
  }

  public static void log(OMAuditLogger.Builder builder, OMClientRequest request, OzoneManager om,
                         TermIndex termIndex, Throwable th) {
    if (builder.isLog.get()) {
      builder.getAuditLogger().logWrite(builder.getMessageBuilder().build());
      return;
    }

    OMAction action = getAction(request.getOmRequest());
    if (null == action) {
      // no audit log defined
      return;
    }
    if (builder.getAuditMap() == null) {
      builder.setAuditMap(new HashMap<>());
    }
    try {
      builder.getAuditMap().put("Command", request.getOmRequest().getCmdType().name());
      builder.getAuditMap().put("Transaction", String.valueOf(termIndex.getIndex()));
      request.buildAuditMessage(action, builder.getAuditMap(),
          th, request.getUserInfo());
      builder.setLog(true);
      builder.setAuditLogger(om.getAuditLogger());
      log(builder);
    } catch (Exception ex) {
      LOG.error("Exception occurred while write audit log, ", ex);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder class for build AuditMessage.
   */
  public static class Builder {
    private AuditMessage.Builder messageBuilder = new AuditMessage.Builder();
    private final AtomicBoolean isLog = new AtomicBoolean(false);
    private Map<String, String> auditMap = null;
    private AuditLogger auditLogger;

    public OMAuditLogger build() throws IOException {
      return new OMAuditLogger();
    }

    public AuditMessage.Builder getMessageBuilder() {
      return this.messageBuilder;
    }

    public void setLog(boolean flag) {
      this.isLog.set(flag);
    }

    public void setAuditLogger(AuditLogger auditLogger) {
      this.auditLogger = auditLogger;
    }

    public AuditLogger getAuditLogger() {
      return auditLogger;
    }

    public void setAuditMap(Map<String, String> auditMap) {
      this.auditMap = auditMap;
    }
    
    public Map<String, String> getAuditMap() {
      return this.auditMap;
    }
  }
}
