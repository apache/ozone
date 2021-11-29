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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.net.InetAddress;

import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.INVALID_REQUEST;

/**
 * Ozone Acl Wrapper class.
 */
public final class OzoneAclUtils {

  private OzoneAclUtils() {
  }

  /**
   * Check Acls of ozone object with volume owner and bucket owner.
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
  public static void checkAllAcls(OzoneManager ozoneManager,
      OzoneObj.ResourceType resType,
      OzoneObj.StoreType storeType, IAccessAuthorizer.ACLType aclType,
      String vol, String bucket, String key, String volOwner,
      String bucketOwner, UserGroupInformation user, InetAddress remoteAddress,
      String hostName) throws IOException {

    boolean isVolOwner = isOwner(user, volOwner);

    IAccessAuthorizer.ACLType parentAclRight = aclType;

    //OzoneNativeAuthorizer differs from Ranger Authorizer as Ranger requires
    // only READ access on parent level access. OzoneNativeAuthorizer has
    // different parent level access based on the child level access type
    if(ozoneManager.isNativeAuthorizerEnabled()) {
      if (aclType == IAccessAuthorizer.ACLType.CREATE ||
          aclType == IAccessAuthorizer.ACLType.DELETE ||
          aclType == IAccessAuthorizer.ACLType.WRITE_ACL) {
        parentAclRight = IAccessAuthorizer.ACLType.WRITE;
      } else if (aclType == IAccessAuthorizer.ACLType.READ_ACL ||
          aclType == IAccessAuthorizer.ACLType.LIST) {
        parentAclRight = IAccessAuthorizer.ACLType.READ;
      }
    } else {
      parentAclRight =  IAccessAuthorizer.ACLType.READ;

    }

    switch (resType) {
    //For Volume level access we only need to check {OWNER} equal
    // to Volume Owner.
    case VOLUME:
      ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
          user, remoteAddress, hostName, true,
          volOwner);
      break;
    case BUCKET:
    case KEY:
    //For Bucket/Key/Prefix level access, first we need to check {OWNER} equal
    // to volume owner on parent volume. Then we need to check {OWNER} equals
    // volume owner if current ugi user is volume owner else we need check
    //{OWNER} equals bucket owner for bucket/key/prefix.
    case PREFIX:
      ozoneManager.checkAcls(OzoneObj.ResourceType.VOLUME, storeType,
          parentAclRight, vol, bucket, key, user,
          remoteAddress, hostName, true,
          volOwner);
      if (isVolOwner) {
        ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            volOwner);
      } else {
        ozoneManager.checkAcls(resType, storeType, aclType, vol, bucket, key,
            user, remoteAddress, hostName, true,
            bucketOwner);
      }
      break;
    default:
      throw new OMException("Unexpected object type:" +
              resType, INVALID_REQUEST);
    }
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
}
