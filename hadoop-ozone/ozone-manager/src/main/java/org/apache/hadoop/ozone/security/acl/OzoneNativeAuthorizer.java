/**
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
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.ozone.security.acl;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.VolumeManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

/**
 * Public API for Ozone ACLs. Security providers providing support for Ozone
 * ACLs should implement this.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public class OzoneNativeAuthorizer implements IAccessAuthorizer {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneNativeAuthorizer.class);
  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManager prefixManager;
  private OzoneAdmins ozAdmins;
  private boolean allowListAllVolumes;

  public OzoneNativeAuthorizer() {
  }

  public OzoneNativeAuthorizer(VolumeManager volumeManager,
      BucketManager bucketManager, KeyManager keyManager,
      PrefixManager prefixManager, OzoneAdmins ozoneAdmins) {
    this.volumeManager = volumeManager;
    this.bucketManager = bucketManager;
    this.keyManager = keyManager;
    this.prefixManager = prefixManager;
    this.ozAdmins = ozoneAdmins;
  }

  /**
   * Check access for given ozoneObject.
   *
   * @param ozObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @return true if user has access else false.
   */
  @Override
  public boolean checkAccess(IOzoneObj ozObject, RequestContext context)
      throws OMException {
    Objects.requireNonNull(ozObject);
    Objects.requireNonNull(context);
    OzoneObjInfo objInfo;
    RequestContext parentContext;
    boolean isACLTypeCreate = (context.getAclRights() == ACLType.CREATE);

    if (ozObject instanceof OzoneObjInfo) {
      objInfo = (OzoneObjInfo) ozObject;
    } else {
      throw new OMException("Unexpected input received. OM native acls are " +
          "configured to work with OzoneObjInfo type only.", INVALID_REQUEST);
    }

    // bypass all checks for admin
    boolean isAdmin = isAdmin(context.getClientUgi());
    if (isAdmin) {
      return true;
    }

    boolean isOwner = isOwner(context.getClientUgi(), context.getOwnerName());
    boolean isListAllVolume = ((context.getAclRights() == ACLType.LIST) &&
        objInfo.getVolumeName().equals(OzoneConsts.OZONE_ROOT));
    if (isListAllVolume) {
      return getAllowListAllVolumes();
    }

    // Refined the parent context
    // OP         |CHILD     |PARENT

    // CREATE      NONE         WRITE
    // DELETE      DELETE       WRITE
    // WRITE       WRITE        WRITE
    // WRITE_ACL   WRITE_ACL    WRITE     (V1 WRITE_ACL=>WRITE)

    // READ        READ         READ
    // LIST        LIST         READ      (V1 LIST=>READ)
    // READ_ACL    READ_ACL     READ      (V1 READ_ACL=>READ)

    ACLType aclRight = context.getAclRights();
    ACLType parentAclRight = aclRight;

    if (aclRight == ACLType.CREATE || aclRight == ACLType.DELETE ||
        aclRight == ACLType.WRITE_ACL) {
      parentAclRight = ACLType.WRITE;
    } else if (aclRight == ACLType.READ_ACL || aclRight == ACLType.LIST) {
      parentAclRight = ACLType.READ;
    }
    parentContext = RequestContext.newBuilder()
        .setClientUgi(context.getClientUgi())
        .setIp(context.getIp())
        .setAclType(context.getAclType())
        .setAclRights(parentAclRight).build();

    switch (objInfo.getResourceType()) {
    case VOLUME:
      LOG.trace("Checking access for volume: {}", objInfo);
      if (isACLTypeCreate) {
        // only admin is allowed to create volume and list all volumes
        return false;
      }
      boolean volumeAccess =  isOwner ||
          volumeManager.checkAccess(objInfo, context);
      return volumeAccess;
    case BUCKET:
      LOG.trace("Checking access for bucket: {}", objInfo);
      // Skip check for volume owner
      if (isOwner) {
        return true;
      }
      // Skip bucket access check for CREATE acl since
      // bucket will not exist at the time of creation
      boolean bucketAccess = isACLTypeCreate
          || bucketManager.checkAccess(objInfo, context);
      return (bucketAccess
          && volumeManager.checkAccess(objInfo, parentContext));
    case KEY:
      LOG.trace("Checking access for Key: {}", objInfo);
      // Skip check for volume owner
      if (isOwner) {
        return true;
      }
      // Skip key access check for CREATE acl since
      // key will not exist at the time of creation
      boolean keyAccess = isACLTypeCreate
          || keyManager.checkAccess(objInfo, context);
      return (keyAccess
          && prefixManager.checkAccess(objInfo, parentContext)
          && bucketManager.checkAccess(objInfo, parentContext)
          && volumeManager.checkAccess(objInfo, parentContext));
    case PREFIX:
      LOG.trace("Checking access for Prefix: {}", objInfo);
      // Skip check for volume owner
      if (isOwner) {
        return true;
      }
      // Skip prefix access check for CREATE acl since
      // prefix will not exist at the time of creation
      boolean prefixAccess = isACLTypeCreate
          || prefixManager.checkAccess(objInfo, context);
      return (prefixAccess
          && bucketManager.checkAccess(objInfo, parentContext)
          && volumeManager.checkAccess(objInfo, parentContext));
    default:
      throw new OMException("Unexpected object type:" +
          objInfo.getResourceType(), INVALID_REQUEST);
    }
  }

  public void setVolumeManager(VolumeManager volumeManager) {
    this.volumeManager = volumeManager;
  }

  public void setBucketManager(BucketManager bucketManager) {
    this.bucketManager = bucketManager;
  }

  public void setKeyManager(KeyManager keyManager) {
    this.keyManager = keyManager;
  }

  public void setPrefixManager(PrefixManager prefixManager) {
    this.prefixManager = prefixManager;
  }

  public void setOzoneAdmins(OzoneAdmins ozoneAdmins) {
    this.ozAdmins = ozoneAdmins;
  }

  public OzoneAdmins getOzoneAdmins() {
    return ozAdmins;
  }

  public void setAllowListAllVolumes(boolean allowListAllVolumes) {
    this.allowListAllVolumes = allowListAllVolumes;
  }

  public boolean getAllowListAllVolumes() {
    return allowListAllVolumes;
  }

  private boolean isOwner(UserGroupInformation callerUgi, String ownerName) {
    if (ownerName == null) {
      return false;
    }
    if (callerUgi.getUserName().equals(ownerName) ||
        callerUgi.getShortUserName().equals(ownerName)) {
      return true;
    }
    return false;
  }

  private boolean isAdmin(UserGroupInformation callerUgi) {
    Preconditions.checkNotNull(callerUgi, "callerUgi should not be null!");

    if (ozAdmins == null) {
      return false;
    }

    return ozAdmins.isAdmin(callerUgi);
  }
}
