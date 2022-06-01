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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditAction;
import org.apache.hadoop.ozone.audit.AuditEventStatus;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.OzonePrefixPathImpl;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerRatisUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.LayoutVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_KEY_NAME;

/**
 * OMClientRequest provides methods which every write OM request should
 * implement.
 */
public abstract class OMClientRequest implements RequestAuditor {

  private static final Logger LOG =
      LoggerFactory.getLogger(OMClientRequest.class);

  private OMRequest omRequest;

  private UserGroupInformation userGroupInformation;
  private InetAddress inetAddress;

  /**
   * Stores the result of request execution in
   * OMClientRequest#validateAndUpdateCache.
   */
  public enum Result {
    SUCCESS, // The request was executed successfully

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
    LayoutVersion layoutVersion = LayoutVersion.newBuilder()
        .setVersion(ozoneManager.getVersionManager().getMetadataLayoutVersion())
        .build();
    omRequest = getOmRequest().toBuilder()
        .setUserInfo(getUserIfNotExists(ozoneManager))
        .setLayoutVersion(layoutVersion).build();
    return omRequest;
  }

  /**
   * Performs any request specific failure handling during request
   * submission. An example of this would be an undo of any steps
   * taken during pre-execute.
   */
  public void handleRequestFailure(OzoneManager ozoneManager) {
    // Most requests would not have any un-do processing.
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
  public OzoneManagerProtocolProtos.UserInfo getUserInfo() throws IOException {
    UserGroupInformation user = ProtobufRpcEngine.Server.getRemoteUser();
    InetAddress remoteAddress = ProtobufRpcEngine.Server.getRemoteIp();
    OzoneManagerProtocolProtos.UserInfo.Builder userInfo =
        OzoneManagerProtocolProtos.UserInfo.newBuilder();

    // If S3 Authentication is set, determine user based on access ID.
    if (omRequest.hasS3Authentication()) {
      String principal = OzoneAclUtils.accessIdToUserPrincipal(
          omRequest.getS3Authentication().getAccessId());
      userInfo.setUserName(principal);
    } else if (user != null) {
      // Added not null checks, as in UT's these values might be null.
      userInfo.setUserName(user.getUserName());
    }

    // for gRPC s3g omRequests that contain user name
    if (user == null && omRequest.hasUserInfo()) {
      userInfo.setUserName(omRequest.getUserInfo().getUserName());
    }

    if (remoteAddress != null) {
      userInfo.setHostName(remoteAddress.getHostName());
      userInfo.setRemoteAddress(remoteAddress.getHostAddress()).build();
    }

    return userInfo.build();
  }

  /**
   * For non-rpc internal calls Server.getRemoteUser()
   * and Server.getRemoteIp() will be null.
   * Passing getCurrentUser() and Ip of the Om node that started it.
   * @return User Info.
   */
  public OzoneManagerProtocolProtos.UserInfo getUserIfNotExists(
      OzoneManager ozoneManager) throws IOException {
    OzoneManagerProtocolProtos.UserInfo userInfo = getUserInfo();
    if (!userInfo.hasRemoteAddress() || !userInfo.hasUserName()) {
      OzoneManagerProtocolProtos.UserInfo.Builder newuserInfo =
          OzoneManagerProtocolProtos.UserInfo.newBuilder();
      UserGroupInformation user;
      InetAddress remoteAddress;
      try {
        user = UserGroupInformation.getCurrentUser();
        remoteAddress = ozoneManager.getOmRpcServerAddr()
            .getAddress();
      } catch (Exception e) {
        LOG.debug("Couldn't get om Rpc server address", e);
        return getUserInfo();
      }
      newuserInfo.setUserName(user.getUserName());
      newuserInfo.setHostName(remoteAddress.getHostName());
      newuserInfo.setRemoteAddress(remoteAddress.getHostAddress());
      return newuserInfo.build();
    }
    return getUserInfo();
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
  @SuppressWarnings("parameternumber")
  public void checkAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key) throws IOException {
    checkAcls(ozoneManager, resType, storeType, aclType, vol, bucket, key,
        ozoneManager.getVolumeOwner(vol, aclType, resType),
        ozoneManager.getBucketOwner(vol, bucket, aclType, resType));
  }

  /**
   * Check Acls for the ozone key.
   * @param ozoneManager
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @throws IOException
   */
  protected void checkACLsWithFSO(OzoneManager ozoneManager, String volumeName,
                                  String bucketName, String keyName,
                                  IAccessAuthorizer.ACLType aclType)
      throws IOException {

    // TODO: Presently not populating sub-paths under a single bucket
    //  lock. Need to revisit this to handle any concurrent operations
    //  along with this.
    OzonePrefixPathImpl pathViewer = new OzonePrefixPathImpl(volumeName,
        bucketName, keyName, ozoneManager.getKeyManager());

    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(OzoneObj.ResourceType.KEY)
        .setStoreType(OzoneObj.StoreType.OZONE)
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setKeyName(keyName)
        .setOzonePrefixPath(pathViewer).build();

    RequestContext.Builder contextBuilder = RequestContext.newBuilder()
        .setAclRights(aclType)
        // recursive checks for a dir with sub-directories or sub-files
        .setRecursiveAccessCheck(pathViewer.isCheckRecursiveAccess());

    // check Acl
    if (ozoneManager.getAclsEnabled()) {
      String volumeOwner = ozoneManager.getVolumeOwner(obj.getVolumeName(),
          contextBuilder.getAclRights(), obj.getResourceType());
      String bucketOwner = ozoneManager.getBucketOwner(obj.getVolumeName(),
          obj.getBucketName(), contextBuilder.getAclRights(),
          obj.getResourceType());
      UserGroupInformation currentUser = createUGI();
      contextBuilder.setClientUgi(currentUser);
      contextBuilder.setIp(getRemoteAddress());
      contextBuilder.setHost(getHostName());
      contextBuilder.setAclType(IAccessAuthorizer.ACLIdentityType.USER);

      boolean isVolOwner = isOwner(currentUser, volumeOwner);
      IAccessAuthorizer.ACLType parentAclRight = aclType;
      if (isVolOwner) {
        contextBuilder.setOwnerName(volumeOwner);
      } else {
        contextBuilder.setOwnerName(bucketOwner);
      }
      if (ozoneManager.isNativeAuthorizerEnabled()) {
        if (aclType == IAccessAuthorizer.ACLType.CREATE ||
                aclType == IAccessAuthorizer.ACLType.DELETE ||
                aclType == IAccessAuthorizer.ACLType.WRITE_ACL) {
          parentAclRight = IAccessAuthorizer.ACLType.WRITE;
        } else if (aclType == IAccessAuthorizer.ACLType.READ_ACL ||
                aclType == IAccessAuthorizer.ACLType.LIST) {
          parentAclRight = IAccessAuthorizer.ACLType.READ;
        }
      } else {
        parentAclRight = IAccessAuthorizer.ACLType.READ;

      }
      OzoneObj volumeObj = OzoneObjInfo.Builder.newBuilder()
              .setResType(OzoneObj.ResourceType.VOLUME)
              .setStoreType(OzoneObj.StoreType.OZONE)
              .setVolumeName(volumeName)
              .setBucketName(bucketName)
              .setKeyName(keyName).build();
      RequestContext volumeContext = RequestContext.newBuilder()
              .setClientUgi(currentUser)
              .setIp(getRemoteAddress())
              .setHost(getHostName())
              .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
              .setAclRights(parentAclRight)
              .setOwnerName(volumeOwner)
              .build();
      ozoneManager.checkAcls(volumeObj, volumeContext, true);
      ozoneManager.checkAcls(obj, contextBuilder.build(), true);
    }
  }

  private boolean isOwner(UserGroupInformation callerUgi,
                                 String ownerName) {
    if (ownerName == null) {
      return false;
    }
    if (callerUgi.getUserName().equals(ownerName) ||
        callerUgi.getShortUserName().equals(ownerName)) {
      return true;
    }
    return false;
  }

  /**
   * Check Acls of ozone object with volOwner given.
   * @param ozoneManager
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @param volOwner
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public void checkAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key, String volOwner)
      throws IOException {
    ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
        createUGI(), getRemoteAddress(), getHostName(), true,
        volOwner);
  }

  /**
   * Check Acls of ozone object with volOwner given.
   * @param ozoneManager
   * @param resType
   * @param storeType
   * @param aclType
   * @param vol
   * @param bucket
   * @param key
   * @param volOwner
   * @param bucketOwner
   * @throws IOException
   */
  @SuppressWarnings("parameternumber")
  public void checkAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key, String volOwner,
      String bucketOwner)
      throws IOException {

    OzoneAclUtils.checkAllAcls(ozoneManager, resType, storeType, aclType,
            vol, bucket, key, volOwner, bucketOwner, createUGI(),
            getRemoteAddress(), getHostName());
  }

  /**
   * Return UGI object created from OMRequest userInfo. If userInfo is not
   * set, returns null.
   * @return UserGroupInformation.
   */
  @VisibleForTesting
  public UserGroupInformation createUGI() {

    if (userGroupInformation != null) {
      return userGroupInformation;
    }
    if (omRequest.hasUserInfo() &&
        !StringUtils.isBlank(omRequest.getUserInfo().getUserName())) {
      userGroupInformation = UserGroupInformation.createRemoteUser(
          omRequest.getUserInfo().getUserName());
      return userGroupInformation;
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
    if (inetAddress != null) {
      return inetAddress;
    }

    if (omRequest.hasUserInfo()) {
      inetAddress = InetAddress.getByName(omRequest.getUserInfo()
          .getRemoteAddress());
      return inetAddress;
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
   * Add the client response to double buffer and set the flush future.
   * @param trxIndex
   * @param omClientResponse
   * @param omDoubleBufferHelper
   */
  protected void addResponseToDoubleBuffer(long trxIndex,
      OMClientResponse omClientResponse,
      OzoneManagerDoubleBufferHelper omDoubleBufferHelper) {
    if (omClientResponse != null) {
      omClientResponse.setFlushFuture(
          omDoubleBufferHelper.add(omClientResponse, trxIndex));
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


  public static String validateAndNormalizeKey(boolean enableFileSystemPaths,
      String keyName) throws OMException {
    if (enableFileSystemPaths) {
      return validateAndNormalizeKey(keyName);
    } else {
      return keyName;
    }
  }

  public static String validateAndNormalizeKey(boolean enableFileSystemPaths,
      String keyPath, BucketLayout bucketLayout) throws OMException {
    LOG.debug("Bucket Layout: {}", bucketLayout);
    if (bucketLayout.shouldNormalizePaths(enableFileSystemPaths)) {
      keyPath = validateAndNormalizeKey(true, keyPath);
      if (keyPath.endsWith("/")) {
        throw new OMException(
                "Invalid KeyPath, key names with trailing / "
                        + "are not allowed." + keyPath,
                OMException.ResultCodes.INVALID_KEY_NAME);
      }
    }
    return keyPath;
  }

  public static String validateAndNormalizeKey(String keyName)
      throws OMException {
    String normalizedKeyName = OmUtils.normalizeKey(keyName, false);
    return isValidKeyPath(normalizedKeyName);
  }

  /**
   * Whether the pathname is valid.  Check key names which contain a
   * ":", ".", "..", "//", "". If it has any of these characters throws
   * OMException, else return the path.
   */
  private static String isValidKeyPath(String path) throws OMException {
    boolean isValid = true;

    // If keyName is empty string throw error.
    if (path.length() == 0) {
      throw new OMException("Invalid KeyPath, empty keyName" + path,
          INVALID_KEY_NAME);
    } else if (path.startsWith("/")) {
      isValid = false;
    } else {
      // Check for ".." "." ":" "/"
      String[] components = StringUtils.split(path, '/');
      for (int i = 0; i < components.length; i++) {
        String element = components[i];
        if (element.equals(".") ||
            (element.contains(":")) ||
            (element.contains("/") || element.equals(".."))) {
          isValid = false;
          break;
        }

        // The string may end with a /, but not have
        // "//" in the middle.
        if (element.isEmpty() && i != components.length - 1) {
          isValid = false;
        }
      }
    }

    if (isValid) {
      return path;
    } else {
      throw new OMException("Invalid KeyPath " + path, INVALID_KEY_NAME);
    }
  }
}
