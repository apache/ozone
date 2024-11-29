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

package org.apache.hadoop.ozone.om.ratis.execution.request;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.helpers.OMAuditLogger;
import org.apache.hadoop.ozone.om.lock.OmLockOpr;
import org.apache.hadoop.ozone.om.ratis.execution.DbChangesRecorder;
import org.apache.hadoop.ozone.om.request.RequestAuditor;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;

/**
 * define methods for request to handle.
 */
public class OMRequestExecutor implements RequestAuditor {
  private final OMAuditLogger.Builder auditBuilder = OMAuditLogger.newBuilder();
  private DbChangesRecorder recorder = new DbChangesRecorder();
  private OMRequest omRequest;

  public OMRequestExecutor(OMRequest omRequest) {
    this.omRequest = omRequest;
  }
  /**
   * Perform request validation, bucket type check, update parameter like user Info, update time and others.
   */
  public OzoneManagerProtocolProtos.OMRequest preProcess(OzoneManager ozoneManager)
      throws IOException {
    OzoneManagerProtocolProtos.LayoutVersion layoutVersion = OzoneManagerProtocolProtos.LayoutVersion.newBuilder()
        .setVersion(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .build();
    omRequest = getOmRequest().toBuilder()
        .setUserInfo(OmKeyUtils.getUserIfNotExists(ozoneManager, getOmRequest()))
        .setLayoutVersion(layoutVersion).build();
    return omRequest;
  }

  public void authorize(OzoneManager ozoneManager) throws IOException {
  }

  public OmLockOpr.OmLockInfo lock(OmLockOpr lockOpr) throws IOException {
    return null;
  }

  public void unlock(OmLockOpr lockOpr, OmLockOpr.OmLockInfo lockInfo) {
  }

  /**
   * perform request processing such as prepare changes, resource validation.
   */
  public OMClientResponse process(OzoneManager ozoneManager, ExecutionContext exeCtx) throws IOException {
    return null;
  }
  public DbChangesRecorder changeRecorder() {
    return recorder;
  }

  public OMRequest getOmRequest() {
    return omRequest;
  }

  @Override
  public OMAuditLogger.Builder buildAuditMessage(
      AuditAction op, Map< String, String > auditMap, Throwable throwable,
      OzoneManagerProtocolProtos.UserInfo userInfo) {
    auditBuilder.getMessageBuilder()
        .setUser(userInfo != null ? userInfo.getUserName() : null)
        .atIp(userInfo != null ? userInfo.getRemoteAddress() : null)
        .forOperation(op).withParams(auditMap)
        .withResult(throwable != null ? AuditEventStatus.FAILURE : AuditEventStatus.SUCCESS)
        .withException(throwable);
    auditBuilder.setAuditMap(auditMap);
    return auditBuilder;
  }

  @Override
  public Map<String, String> buildVolumeAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }
  /**
   * Mark ready for log audit.
   * @param auditLogger audit logger instance
   * @param builder builder for audit logger
   */
  protected void markForAudit(AuditLogger auditLogger, OMAuditLogger.Builder builder) {
    builder.setLog(true);
    builder.setAuditLogger(auditLogger);
  }

  public OMAuditLogger.Builder getAuditBuilder() {
    return auditBuilder;
  }
}
