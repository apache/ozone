/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.om;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.ipc.ProtobufRpcEngine;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmVolumeArgs;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.apache.hadoop.ozone.om.OzoneManager.getS3Auth;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.BUCKET_LOCK;
import static org.apache.hadoop.ozone.om.lock.OzoneManagerLock.Resource.VOLUME_LOCK;

/**
 * AclManager is used to manage the ACLs for Ozone resources.
 */
public class AclManager {
  public static final Logger LOG =
      LoggerFactory.getLogger(AclManager.class);

  private final OMMetadataManager metadataManager;
  private final InetSocketAddress omRpcAddress;
  private final IAccessAuthorizer accessAuthorizer;
  private final boolean isNativeAuthorizerEnabled;

  public AclManager(OMMetadataManager metadataManager,
                    InetSocketAddress omRpcAddress,
                    IAccessAuthorizer accessAuthorizer,
                    boolean isNativeAuthorizerEnabled) {
    this.metadataManager = metadataManager;
    this.omRpcAddress = omRpcAddress;
    this.accessAuthorizer = accessAuthorizer;
    this.isNativeAuthorizerEnabled = isNativeAuthorizerEnabled;
  }

  public void checkAcls(OzoneObj.ResourceType resType,
                        OzoneObj.StoreType store,
                        IAccessAuthorizer.ACLType acl,
                        String vol,
                        String bucket,
                        String key)
      throws IOException {
    UserGroupInformation user;
    if (getS3Auth() != null) {
      String principal =
          OzoneAclUtils.accessIdToUserPrincipal(getS3Auth().getAccessId());
      user = UserGroupInformation.createRemoteUser(principal);
    } else {
      user = ProtobufRpcEngine.Server.getRemoteUser();
    }

    InetAddress remoteIp = ProtobufRpcEngine.Server.getRemoteIp();

    checkAllAcls(resType, store, acl,
        vol, bucket, key,
        user != null ? user : getRemoteUser(),
        remoteIp != null ? remoteIp : omRpcAddress.getAddress(),
        remoteIp != null ? remoteIp.getHostName()
            : omRpcAddress.getHostName());
  }

  @SuppressWarnings("parameternumber")
  private boolean checkAcls(OzoneObj.ResourceType resType,
                            OzoneObj.StoreType storeType,
                            IAccessAuthorizer.ACLType aclType,
                            String vol, String bucket, String key,
                            UserGroupInformation ugi,
                            InetAddress remoteAddress, String hostName,
                            boolean throwIfPermissionDenied, String volumeOwner)
      throws OMException {
    OzoneObj obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(resType)
        .setStoreType(storeType)
        .setVolumeName(vol)
        .setBucketName(bucket)
        .setKeyName(key).build();
    RequestContext context = RequestContext.newBuilder()
        .setClientUgi(ugi)
        .setIp(remoteAddress)
        .setHost(hostName)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(aclType)
        .setOwnerName(volumeOwner)
        .build();

    return checkAcls(obj, context, throwIfPermissionDenied);
  }

  private boolean checkAcls(OzoneObj obj, RequestContext context,
                            boolean throwIfPermissionDenied)
      throws OMException {

    if (!accessAuthorizer.checkAccess(obj, context)) {
      if (throwIfPermissionDenied) {
        String volumeName = obj.getVolumeName() != null ?
            "Volume:" + obj.getVolumeName() + " " : "";
        String bucketName = obj.getBucketName() != null ?
            "Bucket:" + obj.getBucketName() + " " : "";
        String keyName = obj.getKeyName() != null ?
            "Key:" + obj.getKeyName() : "";
        LOG.warn("User {} doesn't have {} permission to access {} {}{}{}",
            context.getClientUgi().getUserName(), context.getAclRights(),
            obj.getResourceType(), volumeName, bucketName, keyName);
        throw new OMException("User " + context.getClientUgi().getUserName() +
            " doesn't have " + context.getAclRights() +
            " permission to access " + obj.getResourceType() + " " +
            volumeName + bucketName + keyName,
            OMException.ResultCodes.PERMISSION_DENIED);
      }
      return false;
    } else {
      return true;
    }
  }

  @SuppressWarnings("parameternumber")
  public void checkAllAcls(
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key,
      UserGroupInformation user, InetAddress remoteAddress,
      String hostName) throws IOException {

    String volOwner = getVolumeOwner(vol, aclType, resType);
    String bucketOwner = getBucketOwner(vol, bucket, aclType, resType);
    boolean isVolOwner = isOwner(user, volOwner);

    IAccessAuthorizer.ACLType parentAclRight = aclType;

    //OzoneNativeAuthorizer differs from Ranger Authorizer as Ranger requires
    // only READ access on parent level access. OzoneNativeAuthorizer has
    // different parent level access based on the child level access type
    if (isNativeAuthorizerEnabled) {
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

    switch (resType) {
      //For Volume level access we only need to check {OWNER} equal
      // to Volume Owner.
    case VOLUME:
      checkAcls(resType, storeType, aclType, vol, bucket, key,
          user, remoteAddress, hostName, true,
          volOwner);
      break;
    case BUCKET:
    case KEY:
      //For Bucket/Key/Prefix level access, first we need to check {OWNER}
      //equal to volume owner on parent volume. Then we need to check
      //{OWNER} equals volume owner if current ugi user is volume owner else
      //we need check {OWNER} equals bucket owner for bucket/key/prefix.
    case PREFIX:
      checkAcls(OzoneObj.ResourceType.VOLUME, storeType,
          parentAclRight, vol, bucket, key, user,
          remoteAddress, hostName, true,
          volOwner);
      if (isVolOwner) {
        checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            volOwner);
      } else {
        checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            bucketOwner);
      }
      break;
    default:
      throw new OMException("Unexpected object type:" +
          resType, INVALID_REQUEST);
    }
  }

  @VisibleForTesting
  public static Logger getLogger() {
    return LOG;
  }

  private String getVolumeOwner(String volume,
                                IAccessAuthorizer.ACLType type,
                                OzoneObj.ResourceType resType)
      throws OMException {
    if (volume.equals(OzoneConsts.OZONE_ROOT) ||
        type == IAccessAuthorizer.ACLType.CREATE &&
            resType == OzoneObj.ResourceType.VOLUME) {
      return null;
    }
    Boolean lockAcquired = metadataManager.getLock().acquireReadLock(
        VOLUME_LOCK, volume);
    String dbVolumeKey = metadataManager.getVolumeKey(volume);
    OmVolumeArgs volumeArgs = null;
    try {
      volumeArgs = metadataManager.getVolumeTable().get(dbVolumeKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException) ioe;
      } else {
        throw new OMException("getVolumeOwner for Volume " + volume + " failed",
            OMException.ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(VOLUME_LOCK, volume);
      }
    }
    if (volumeArgs != null) {
      return volumeArgs.getOwnerName();
    } else {
      throw new OMException("Volume " + volume + " is not found",
          OMException.ResultCodes.VOLUME_NOT_FOUND);
    }
  }

  private String getBucketOwner(String volume, String bucket,
                                IAccessAuthorizer.ACLType type,
                                OzoneObj.ResourceType resType)
      throws OMException {
    if ((resType == OzoneObj.ResourceType.VOLUME) ||
        type == IAccessAuthorizer.ACLType.CREATE &&
            resType == OzoneObj.ResourceType.BUCKET) {
      return null;
    }
    Boolean lockAcquired = metadataManager.getLock().acquireReadLock(
        BUCKET_LOCK, volume, bucket);
    String dbBucketKey = metadataManager.getBucketKey(volume, bucket);
    OmBucketInfo bucketInfo = null;
    try {
      bucketInfo = metadataManager.getBucketTable().get(dbBucketKey);
    } catch (IOException ioe) {
      if (ioe instanceof OMException) {
        throw (OMException) ioe;
      } else {
        throw new OMException("getBucketOwner for Bucket " + volume + "/" +
            bucket + " failed: " + ioe.getMessage(),
            OMException.ResultCodes.INTERNAL_ERROR);
      }
    } finally {
      if (lockAcquired) {
        metadataManager.getLock().releaseReadLock(BUCKET_LOCK, volume, bucket);
      }
    }
    if (bucketInfo != null) {
      return bucketInfo.getOwner();
    } else {
      throw new OMException("Bucket not found",
          OMException.ResultCodes.BUCKET_NOT_FOUND);
    }
  }

  private static UserGroupInformation getRemoteUser() throws IOException {
    UserGroupInformation ugi = Server.getRemoteUser();
    return (ugi != null) ? ugi : UserGroupInformation.getCurrentUser();
  }

  private static boolean isOwner(UserGroupInformation callerUgi,
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
   * A variant of checkAcls that doesn't throw exception if permission denied.
   *
   * @return true if permission granted, false if permission denied.
   */
  public boolean hasAcls(String userName, OzoneObj.ResourceType resType,
                         OzoneObj.StoreType store,
                         IAccessAuthorizer.ACLType acl,
                         String vol, String bucket, String key) {
    try {
      return checkAcls(resType, store, acl, vol, bucket, key,
          UserGroupInformation.createRemoteUser(userName),
          ProtobufRpcEngine.Server.getRemoteIp(),
          ProtobufRpcEngine.Server.getRemoteIp().getHostName(),
          false, getVolumeOwner(vol, acl, resType));
    } catch (OMException ex) {
      // Should not trigger exception here at all
      return false;
    }
  }

  @SuppressWarnings("parameternumber")
  public void checkACLsWithFSO(
      OzoneManager ozoneManager,
      String volumeName,
      String bucketName, String keyName,
      IAccessAuthorizer.ACLType aclType,
      InetAddress remoteAddress, String hostName,
      UserGroupInformation ugi) throws IOException {

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

    String volumeOwner = getVolumeOwner(obj.getVolumeName(),
        contextBuilder.getAclRights(), obj.getResourceType());
    String bucketOwner = getBucketOwner(obj.getVolumeName(),
        obj.getBucketName(), contextBuilder.getAclRights(),
        obj.getResourceType());
    UserGroupInformation currentUser = ugi;
    contextBuilder.setClientUgi(currentUser);
    contextBuilder.setIp(remoteAddress);
    contextBuilder.setHost(hostName);
    contextBuilder.setAclType(IAccessAuthorizer.ACLIdentityType.USER);

    boolean isVolOwner = isOwner(currentUser, volumeOwner);
    IAccessAuthorizer.ACLType parentAclRight = aclType;
    if (isVolOwner) {
      contextBuilder.setOwnerName(volumeOwner);
    } else {
      contextBuilder.setOwnerName(bucketOwner);
    }
    if (isNativeAuthorizerEnabled) {
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
        .setIp(remoteAddress)
        .setHost(hostName)
        .setAclType(IAccessAuthorizer.ACLIdentityType.USER)
        .setAclRights(parentAclRight)
        .setOwnerName(volumeOwner)
        .build();
    checkAcls(volumeObj, volumeContext, true);
    checkAcls(obj, contextBuilder.build(), true);
  }
}
