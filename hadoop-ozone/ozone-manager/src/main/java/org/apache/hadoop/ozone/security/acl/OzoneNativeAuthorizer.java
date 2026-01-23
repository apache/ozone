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

package org.apache.hadoop.ozone.security.acl;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.server.OzoneAdmins;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.BucketManager;
import org.apache.hadoop.ozone.om.KeyManager;
import org.apache.hadoop.ozone.om.OzoneAclUtils;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.PrefixManager;
import org.apache.hadoop.ozone.om.VolumeManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Native (internal) implementation of {@link IAccessAuthorizer}.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public class OzoneNativeAuthorizer implements OzoneManagerAuthorizer {

  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneNativeAuthorizer.class);

  private static final Predicate<UserGroupInformation> NO_ADMIN = any -> false;

  private VolumeManager volumeManager;
  private BucketManager bucketManager;
  private KeyManager keyManager;
  private PrefixManager prefixManager;
  private Predicate<UserGroupInformation> adminCheck = NO_ADMIN;
  private Predicate<UserGroupInformation> readOnlyAdminCheck = NO_ADMIN;
  private BooleanSupplier allowListAllVolumes = () -> false;

  public OzoneNativeAuthorizer() {
    // required for instantiation in OmMetadataReader#getACLAuthorizerInstance
  }

  OzoneNativeAuthorizer(VolumeManager volumeManager,
      BucketManager bucketManager, KeyManager keyManager,
      PrefixManager prefixManager, OzoneAdmins ozoneAdmins) {
    this.volumeManager = volumeManager;
    this.bucketManager = bucketManager;
    this.keyManager = keyManager;
    this.prefixManager = prefixManager;
    this.adminCheck = ozoneAdmins::isAdmin;
  }

  @Override
  public boolean isNative() {
    return true;
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
    RequestContext parentVolContext;
    boolean isACLTypeCreate = (context.getAclRights() == ACLType.CREATE);

    if (ozObject instanceof OzoneObjInfo) {
      objInfo = (OzoneObjInfo) ozObject;
    } else {
      throw new OMException("Unexpected input received. OM native acls are " +
          "configured to work with OzoneObjInfo type only.", INVALID_REQUEST);
    }

    // bypass all checks for admin
    if (adminCheck.test(context.getClientUgi())) {
      return true;
    }

    // bypass read checks for read only admin users
    if (readOnlyAdminCheck.test(context.getClientUgi())
        && (context.getAclRights() == ACLType.READ
        || context.getAclRights() == ACLType.READ_ACL
        || context.getAclRights() == ACLType.LIST)) {
      return true;
    }

    boolean isOwner = isOwner(context.getClientUgi(), context.getOwnerName());
    boolean isListAllVolume = ((context.getAclRights() == ACLType.LIST) &&
        objInfo.getVolumeName().equals(OzoneConsts.OZONE_ROOT));
    if (isListAllVolume) {
      return getAllowListAllVolumes();
    }

    ACLType parentAclRight = OzoneAclUtils.getParentNativeAcl(
        context.getAclRights(), objInfo.getResourceType());

    parentContext = RequestContext.newBuilder()
        .setClientUgi(context.getClientUgi())
        .setIp(context.getIp())
        .setAclType(context.getAclType())
        .setAclRights(parentAclRight).build();
    
    // Volume will be always read in case of key and prefix
    parentVolContext = RequestContext.newBuilder()
        .setClientUgi(context.getClientUgi())
        .setIp(context.getIp())
        .setAclType(context.getAclType())
        .setAclRights(ACLType.READ).build();

    switch (objInfo.getResourceType()) {
    case VOLUME:
      LOG.trace("Checking access for volume: {}", objInfo);
      if (isACLTypeCreate) {
        // only admin is allowed to create volume and list all volumes
        return false;
      }
      return isOwner || volumeManager.checkAccess(objInfo, context);
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
          && volumeManager.checkAccess(objInfo, parentVolContext));
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
          && volumeManager.checkAccess(objInfo, parentVolContext));
    default:
      throw new OMException("Unexpected object type:" +
          objInfo.getResourceType(), INVALID_REQUEST);
    }
  }

  @Override
  public OzoneNativeAuthorizer configure(OzoneManager om, KeyManager km, PrefixManager pm) {
    Objects.requireNonNull(om, "om == null");
    volumeManager = om.getVolumeManager();
    bucketManager = om.getBucketManager();
    allowListAllVolumes = () -> om.getConfig().isListAllVolumesAllowed();
    setAdminCheck(om::isAdmin);
    setReadOnlyAdminCheck(om::isReadOnlyAdmin);

    keyManager = km;
    prefixManager = pm;
    return this;
  }

  public void setAdminCheck(Predicate<UserGroupInformation> check) {
    adminCheck = Objects.requireNonNull(check, "admin check");
  }

  public void setReadOnlyAdminCheck(Predicate<UserGroupInformation> check) {
    readOnlyAdminCheck = Objects.requireNonNull(check, "read-only admin check");
  }

  public boolean getAllowListAllVolumes() {
    return allowListAllVolumes.getAsBoolean();
  }

  private static boolean isOwner(UserGroupInformation ugi, String ownerName) {
    return ownerName != null && ownerName.equals(ugi.getShortUserName());
  }
}
