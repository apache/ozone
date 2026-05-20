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

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.DEFAULT;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.GROUP;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OzoneAclInfo;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for ozone acls operations.
 */
public final class OzoneAclUtil {
  static final Logger LOG = LoggerFactory.getLogger(OzoneAclUtil.class);

  private OzoneAclUtil() {
  }

  /**
   * Helper function to get default access acl list for current user.
   *
   * @param ugi current login user
   * @param conf current configuration
   * @return list of OzoneAcls
   * */
  public static List<OzoneAcl> getDefaultAclList(UserGroupInformation ugi, OmConfig conf) {
    // Get default acl rights for user and group.
    List<OzoneAcl> listOfAcls = new ArrayList<>();
    // User ACL.
    listOfAcls.add(OzoneAcl.of(USER, ugi.getShortUserName(), ACCESS, conf.getUserDefaultRights()));
    try {
      String groupName = ugi.getPrimaryGroupName();
      listOfAcls.add(OzoneAcl.of(GROUP, groupName, ACCESS, conf.getGroupDefaultRights()));
    } catch (IOException e) {
      // do nothing, since user has the permission, user can add ACL for selected groups later.
      LOG.warn("Failed to get primary group from user {}", ugi);
    }
    return listOfAcls;
  }

  public static List<OzoneAcl> getAclList(UserGroupInformation ugi, ACLType userPrivilege, ACLType groupPrivilege) {
    List<OzoneAcl> listOfAcls = new ArrayList<>();
    // User ACL.
    listOfAcls.add(OzoneAcl.of(USER, ugi.getShortUserName(), ACCESS, userPrivilege));
    try {
      String groupName = ugi.getPrimaryGroupName();
      listOfAcls.add(OzoneAcl.of(GROUP, groupName, ACCESS, groupPrivilege));
    } catch (IOException e) {
      // do nothing, since user has the permission, user can add ACL for selected groups later.
      LOG.warn("Failed to get primary group from user {}", ugi);
    }
    return listOfAcls;
  }

  /**
   * Helper function to get acl list for one user/group.
   *
   * @param identityName
   * @param type
   * @param aclList
   * @return list of OzoneAcls
   * */
  public static List<OzoneAcl> filterAclList(String identityName,
      IAccessAuthorizer.ACLIdentityType type, List<OzoneAcl> aclList) {

    if (aclList == null || aclList.isEmpty()) {
      return new ArrayList<>();
    }

    List retList = aclList.stream().filter(acl -> acl.getType() == type
        && acl.getName().equals(identityName)).collect(Collectors.toList());
    return retList;
  }

  private static boolean checkAccessInAcl(OzoneAcl a, UserGroupInformation ugi,
      ACLType aclToCheck) {
    switch (a.getType()) {
    case USER:
      if (a.getName().equals(ugi.getShortUserName())) {
        return a.checkAccess(aclToCheck);
      }
      break;
    case GROUP:
      for (String grp : ugi.getGroupNames()) {
        if (a.getName().equals(grp)) {
          return a.checkAccess(aclToCheck);
        }
      }
      break;

    default:
      return a.checkAccess(aclToCheck);
    }
    return false;
  }

  /**
   * Check if acl right requested for given RequestContext exist
   * in provided acl list.
   * Acl validation rules:
   * 1. If user/group has ALL bit set than all user should have all rights.
   * 2. If user/group has NONE bit set than user/group will not have any right.
   * 3. For all other individual rights individual bits should be set.
   *
   * @param acls
   * @param context
   * @return return true if acl list contains right requsted in context.
   * */
  public static boolean checkAclRights(List<OzoneAcl> acls,
      RequestContext context) throws OMException {
    UserGroupInformation clientUgi = context.getClientUgi();
    ACLType aclToCheck = context.getAclRights();
    for (OzoneAcl acl : acls) {
      if (checkAccessInAcl(acl, clientUgi, aclToCheck)) {
        return true;
      }
    }
    return false;
  }

  public static boolean inheritDefaultAcls(AclListBuilder acls,
      List<OzoneAcl> parentAcls, OzoneAcl.AclScope scope) {
    return inheritDefaultAcls(acls::add, parentAcls, scope);
  }

  /**
   * Helper function to inherit default ACL with given {@code scope} for child object.
   * @param acls child object ACL list
   * @param parentAcls parent object ACL list
   * @param scope scope applied to inherited ACL
   * @return true if any ACL was inherited from parent, false otherwise
   */
  public static boolean inheritDefaultAcls(List<OzoneAcl> acls,
      List<OzoneAcl> parentAcls, OzoneAcl.AclScope scope) {
    return inheritDefaultAcls(acl -> addAcl(acls, acl), parentAcls, scope);
  }

  private static boolean inheritDefaultAcls(Predicate<OzoneAcl> op,
      List<OzoneAcl> parentAcls, OzoneAcl.AclScope scope) {
    if (parentAcls != null && !parentAcls.isEmpty()) {
      Stream<OzoneAcl> aclStream = parentAcls.stream()
          .filter(a -> a.getAclScope() == DEFAULT);

      if (scope != DEFAULT) {
        aclStream = aclStream.map(acl -> acl.withScope(scope));
      }

      List<OzoneAcl> inheritedAcls = aclStream.collect(Collectors.toList());
      boolean changed = false;
      for (OzoneAcl acl : inheritedAcls) {
        changed |= op.test(acl);
      }
      return changed;
    }

    return false;
  }

  /**
   * Convert a list of OzoneAclInfo(protoc) to list of OzoneAcl(java).
   * @param protoAcls
   * @return list of OzoneAcl.
   */
  public static List<OzoneAcl> fromProtobuf(List<OzoneAclInfo> protoAcls) {
    List<OzoneAcl> ozoneAcls = new ArrayList<>();
    for (OzoneAclInfo aclInfo : protoAcls) {
      ozoneAcls.add(OzoneAcl.fromProtobuf(aclInfo));
    }
    return ozoneAcls;
  }

  /**
   * Convert a list of OzoneAcl(java) to list of OzoneAclInfo(protoc).
   * @param protoAcls
   * @return list of OzoneAclInfo.
   */
  public static List<OzoneAclInfo> toProtobuf(List<OzoneAcl> protoAcls) {
    List<OzoneAclInfo> ozoneAclInfos = new ArrayList<>();
    for (OzoneAcl acl : protoAcls) {
      ozoneAclInfos.add(OzoneAcl.toProtobuf(acl));
    }
    return ozoneAclInfos;
  }

  /**
   * Add an OzoneAcl to existing list of OzoneAcls.
   * @return true if current OzoneAcls are changed, false otherwise.
   */
  public static boolean addAcl(List<OzoneAcl> existingAcls, OzoneAcl acl) {
    if (existingAcls == null || acl == null) {
      return false;
    }

    for (int i = 0; i < existingAcls.size(); i++) {
      final OzoneAcl a = existingAcls.get(i);
      if (a.getName().equals(acl.getName()) &&
          a.getType().equals(acl.getType()) &&
          a.getAclScope().equals(acl.getAclScope())) {
        final OzoneAcl updated = a.add(acl);
        final boolean changed = !Objects.equals(updated, a);
        if (changed) {
          existingAcls.set(i, updated);
        }
        return changed;
      }
    }

    existingAcls.add(acl);
    return true;
  }

  public static boolean addAllAcl(List<OzoneAcl> existingAcls, List<OzoneAcl> acls) {
    // TOOD optimize
    boolean changed = false;
    for (OzoneAcl acl : acls) {
      changed |= addAcl(existingAcls, acl);
    }
    return changed;
  }

  /**
   * remove OzoneAcl from existing list of OzoneAcls.
   * @return true if current OzoneAcls are changed, false otherwise.
   */
  public static boolean removeAcl(List<OzoneAcl> existingAcls, OzoneAcl acl) {
    if (existingAcls == null || existingAcls.isEmpty() || acl == null) {
      return false;
    }

    for (int i = 0; i < existingAcls.size(); i++) {
      final OzoneAcl a = existingAcls.get(i);
      if (a.getName().equals(acl.getName()) &&
          a.getType().equals(acl.getType()) &&
          a.getAclScope().equals(acl.getAclScope())) {
        final OzoneAcl updated = a.remove(acl);
        final boolean changed = !Objects.equals(updated, a);
        if (updated.isEmpty()) {
          existingAcls.remove(i);
        } else if (changed) {
          existingAcls.set(i, updated);
        }
        return changed;
      }
    }
    return false;
  }

  /**
   * Set existingAcls to newAcls.
   * @param existingAcls
   * @param newAcls
   * @return true if newAcls are set successfully, false otherwise.
   */
  public static boolean setAcl(List<OzoneAcl> existingAcls,
      List<OzoneAcl> newAcls) {
    if (existingAcls == null) {
      return false;
    } else {
      existingAcls.clear();
      if (newAcls != null) {
        existingAcls.addAll(newAcls);
      }
    }
    return true;
  }
}
