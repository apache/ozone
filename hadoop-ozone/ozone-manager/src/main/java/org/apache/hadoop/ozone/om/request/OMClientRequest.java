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

package org.apache.hadoop.ozone.om.request;

import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.WithObjectID;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .UnDeletedKeysResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;

import javax.annotation.Nonnull;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status.REPLAY;

/**
 * OMClientRequest provides methods which every write OM request should
 * implement.
 */
public abstract class OMClientRequest implements RequestAuditor {

  private OMRequest omRequest;

  /**
   * Stores the result of request execution in
   * OMClientRequest#validateAndUpdateCache.
   */
  public enum Result {
    SUCCESS, // The request was executed successfully

    REPLAY, // The request is a replay and was ignored

    FAILURE // The request failed and exception was thrown
  }

  public OMClientRequest(OMRequest omRequest) {
    Preconditions.checkNotNull(omRequest);
    this.omRequest = omRequest;
  }
  /**
   * Perform pre-execute steps on a OMRequest.
   *
   * Called from the RPC context, and generates a OMRequest object which has
   * all the information that will be either persisted
   * in RocksDB or returned to the caller once this operation
   * is executed.
   *
   * @return OMRequest that will be serialized and handed off to Ratis for
   *         consensus.
   */
  public OMRequest preExecute(OzoneManager ozoneManager)
      throws IOException {
    omRequest = getOmRequest().toBuilder().setUserInfo(getUserInfo()).build();
    return omRequest;
  }

  /**
   * Validate the OMRequest and update the cache.
   * This step should verify that the request can be executed, perform
   * any authorization steps and update the in-memory cache.

   * This step does not persist the changes to the database.
   *
   * @return the response that will be returned to the client.
   */
  public abstract OMClientResponse validateAndUpdateCache(
      OzoneManager ozoneManager, long transactionLogIndex,
      OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper);

  @VisibleForTesting
  public OMRequest getOmRequest() {
    return omRequest;
  }

  /**
   * Get User information which needs to be set in the OMRequest object.
   * @return User Info.
   */
  public OzoneManagerProtocolProtos.UserInfo getUserInfo() {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    InetAddress remoteAddress = ProtobufRpcEngine.Server.getRemoteIp();
    OzoneManagerProtocolProtos.UserInfo.Builder userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder();

    // Added not null checks, as in UT's these values might be null.
    if (user != null) {
      userInfo.setUserName(user.getUserName());
    }

    if (remoteAddress != null) {
      userInfo.setHostName(remoteAddress.getHostName());
      userInfo.setRemoteAddress(remoteAddress.getHostAddress()).build();
    }

    return userInfo.build();
  }

  /**
   * Check Acls of ozone object.
   * @param ozoneManager
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @throws IOException
   */
  public void checkAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key) throws IOException {
    ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
        createUGI(), getRemoteAddress(), getHostName());
  }

  /**
   * Return UGI object created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return UserGroupInformation.
   */
  @VisibleForTesting
  public UserGroupInformation createUGI() {
    if (omRequest.hasUserInfo() &&
        !StringUtils.isBlank(omRequest.getUserInfo().getUserName())) {
      return UserGroupInformation.createRemoteUser(
          omRequest.getUserInfo().getUserName());
    } else {
      // This will never happen, as for every OM request preExecute, we
      // should add userInfo.
      return null;
    }
  }

  /**
   * Return InetAddress created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return InetAddress
   * @throws IOException
   */
  @VisibleForTesting
  public InetAddress getRemoteAddress() throws IOException {
    if (omRequest.hasUserInfo()) {
      return InetAddress.getByName(omRequest.getUserInfo()
          .getRemoteAddress());
    } else {
      return null;
    }
  }

  /**
   * Return String created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return String
   * @throws IOException
   */
  @VisibleForTesting
  public String getHostName() {
    if (omRequest.hasUserInfo()) {
      return omRequest.getUserInfo().getHostName();
    } else {
      return null;
    }
  }

  /**
   * Set parameters needed for return error response to client.
   * @param omResponse
   * @param ex - IOException
   * @return error response need to be returned to client - OMResponse.
   */
  protected OMResponse createErrorOMResponse(
      @Nonnull OMResponse.Builder omResponse, @Nonnull IOException ex) {

    omResponse.setSuccess(false);
    String errorMsg = exceptionErrorMessage(ex);
    if (errorMsg != null) {
      omResponse.setMessage(errorMsg);
    }
    omResponse.setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
    return omResponse.build();
  }

  /**
   * Set parameters needed for return error response to client.
   *
   * @param omResponse
   * @param ex         - IOException
   * @param unDeletedKeys    - Set<OmKeyInfo>
   * @return error response need to be returned to client - OMResponse.
   */
  protected OMResponse createOperationKeysErrorOMResponse(
      @Nonnull OMResponse.Builder omResponse,
      @Nonnull IOException ex, @Nonnull Set<OmKeyInfo> unDeletedKeys) {
    omResponse.setSuccess(false);
    StringBuffer errorMsg = new StringBuffer();
    errorMsg.append(exceptionErrorMessage(ex) + "\n The Keys not deleted: ");
    UnDeletedKeysResponse.Builder resp =
        UnDeletedKeysResponse.newBuilder();
    for (OmKeyInfo key : unDeletedKeys) {
      if(key != null) {
        resp.addKeyInfo(key.getProtobuf());
        errorMsg.append(key.getObjectInfo() + "\n");
      }
    }
    if (errorMsg != null) {
      omResponse.setMessage(errorMsg.toString());
    }
    // TODO: Currently all delete operations in OzoneBucket.java are void. Here
    //  we put the List of unDeletedKeys into Response. These KeyInfo can be
    //  used to continue deletion if client support delete retry.
    omResponse.setUnDeletedKeysResponse(resp.build());
    omResponse.setStatus(OzoneManagerRatisUtils.exceptionToResponseStatus(ex));
    return omResponse.build();
  }

  /**
   * Add the client response to double buffer and set the flush future.
   * For responses which has status set to REPLAY it is a no-op.
   * @param trxIndex
   * @param omClientResponse
   * @param omDoubleBufferHelper
   */
  protected void addResponseToDoubleBuffer(long trxIndex,
      OMClientResponse omClientResponse,
      OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    if (omClientResponse != null) {
      // For replay transaction we do not need to add to double buffer, as
      // for these transactions there is nothing needs to be done for
      // addDBToBatch.
      if (omClientResponse.getOMResponse().getStatus() != REPLAY) {
        omClientResponse.setFlushFuture(
            omDoubleBufferHelper.add(omClientResponse, trxIndex));
      }
    }
  }

  private String exceptionErrorMessage(IOException ex) {
    if (ex instanceof OMException) {
      return ex.getMessage();
    } else {
      return org.apache.hadoop.util.StringUtils.stringifyException(ex);
    }
  }

  /**
   * Log the auditMessage.
   * @param auditLogger
   * @param auditMessage
   */
  protected void auditLog(AuditLogger auditLogger, AuditMessage auditMessage) {
    auditLogger.logWrite(auditMessage);
  }

  @Override
  public AuditMessage buildAuditMessage(AuditAction op,
      Map< String, String > auditMap, Throwable throwable,
      OzoneManagerProtocolProtos.UserInfo userInfo) {
    return new AuditMessage.Builder()
        .setUser(userInfo != null ? userInfo.getUserName() : null)
        .atIp(userInfo != null ? userInfo.getRemoteAddress() : null)
        .forOperation(op)
        .withParams(auditMap)
        .withResult(throwable != null ? AuditEventStatus.FAILURE :
            AuditEventStatus.SUCCESS)
        .withException(throwable)
        .build();
  }

  @Override
  public Map<String, String> buildVolumeAuditMap(String volume) {
    Map<String, String> auditMap = new LinkedHashMap<>();
    auditMap.put(OzoneConsts.VOLUME, volume);
    return auditMap;
  }

  /**
   * Check if the transaction is a replay.
   * @param ozoneObj OMVolumeArgs or OMBucketInfo or OMKeyInfo object whose 
   *                 updateID needs to be compared with
   * @param transactionID the current transaction ID
   * @return true if transactionID is less than or equal to updateID, false
   * otherwise.
   */
  protected boolean isReplay(OzoneManager om, WithObjectID ozoneObj,
      long transactionID) {
    return om.isRatisEnabled() && ozoneObj.isUpdateIDset() &&
        transactionID <= ozoneObj.getUpdateID();
  }

  /**
   * Return a dummy OMClientResponse for when the transactions are replayed.
   */
  protected OMResponse createReplayOMResponse(
      @Nonnull OMResponse.Builder omResponse) {

    omResponse.setSuccess(false);
    omResponse.setStatus(REPLAY);
    return omResponse.build();
  }
}
