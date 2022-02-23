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
package org.apache.hadoop.ozone.om.multitenant;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.http.auth.BasicUserPrincipal;

/**
 * Public API for Ozone MultiTenant Gatekeeper. Security providers providing
 * support for Ozone MultiTenancy should implement this.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "Yarn", "Ranger", "Hive", "HBase"})
@InterfaceStability.Evolving
public interface MultiTenantAccessAuthorizer extends IAccessAuthorizer {

  /**
   * Initialize the MultiTenantGateKeeper. Initialize any external state.
   *
   * @param configuration
   * @throws IOException
   */
  void init(Configuration configuration) throws IOException;;

  /**
   * Shutdown for the MultiTenantGateKeeper.
   * @throws Exception
   */
  void shutdown() throws Exception;

  /**
   * Assign user to an existing role in the Authorizer.
   * @param principal User principal
   * @param existingRole A JSON String representation of the existing role
   *                     returned from the Authorizer (Ranger).
   * @param isAdmin
   * @return unique and opaque userID that can be used to refer to the user in
   * MultiTenantGateKeeperplugin Implementation. E.g. a Ranger
   * based Implementation can return some ID thats relevant for it.
   */
  String assignUser(BasicUserPrincipal principal, String existingRole,
      boolean isAdmin) throws IOException;

  /**
   * Update the exising role details and push the changes to Ranger.
   *
   * @param principal contains user name, must be an existing user in Ranger.
   * @param existingRole An existing role's JSON response String from Ranger.
   * @return roleId (not useful for now)
   * @throws IOException
   */
  String revokeUserFromRole(BasicUserPrincipal principal,
                                   String existingRole) throws IOException;

  /**
   * Assign all the users to an existing role
   * @param users list of user principals
   * @param existingRole roleName
   */
  public String assignAllUsers(HashSet<String> users,
                               String existingRole) throws IOException;

  /**
   * @param principal
   * @return Unique userID maintained by the authorizer plugin.
   * @throws IOException
   */
  String getUserId(BasicUserPrincipal principal) throws IOException;

  /**
   * @param principal
   * @return Unique groupID maintained by the authorizer plugin.
   * @throws IOException
   */
  String getRole(OzoneTenantRolePrincipal principal)
      throws IOException;

  /**
   * Returs the details of a role, given the rolename.
   * @param roleName
   * @return
   * @throws IOException
   */
  String getRole(String roleName)
      throws IOException;

  /**
   * Delete the user userID in MultiTenantGateKeeper plugin.
   * @param opaqueUserID : unique ID that was returned by
   *                    MultiTenantGatekeeper in
   *               createUser().
   */
  void deleteUser(String opaqueUserID) throws IOException;

  /**
   * Create Role entity for MultiTenantGatekeeper plugin.
   * @param role
   * @param adminRoleName (Optional) admin role name that will be added to
   *                      manage this role.
   * @return unique groupID that can be used to refer to the role in
   * MultiTenantGateKeeper plugin Implementation e.g. corresponding ID on the
   * Ranger end for a ranger based implementation .
   */
  String createRole(String role, String adminRoleName)
      throws IOException;

  /**
   * Creates a new user.
   * @param userName
   * @param password
   * @return
   * @throws IOException
   */
  public String createUser(String userName,
                           String password)
      throws IOException;
  /**
   * Delete the group groupID in MultiTenantGateKeeper plugin.
   * @param groupID : unique opaque ID that was returned by
   *                MultiTenantGatekeeper in createGroup().
   */
  void deleteRole(String groupID) throws IOException;

  /**
   *
   * @param policy
   * @return unique and opaque policy ID that is maintained by the plugin.
   * @throws Exception
   */
  String createAccessPolicy(AccessPolicy policy) throws Exception;

  /**
   *
   * @param policyName
   * @return unique and opaque policy ID that is maintained by the plugin.
   * @throws Exception
   */
  AccessPolicy getAccessPolicyByName(String policyName) throws Exception;

  /**
   * given a policy Id, returs the policy.
   * @param policyId
   * @return
   * @throws Exception
   */
  AccessPolicy getAccessPolicyById(String policyId) throws Exception;

  /**
   *
   * @param policyId that was returned earlier by the createAccessPolicy().
   * @throws Exception
   */
  void deletePolicybyId(String policyId) throws IOException;

  /**
   *
   * @param policyName unique policyName.
   * @throws Exception
   */
  void deletePolicybyName(String policyName) throws Exception;
  /**
   * Grant user aclType access to bucketNameSpace.
   * @param bucketNameSpace
   * @param user
   * @param aclType
   */
  void grantAccess(BucketNameSpace bucketNameSpace,
                   BasicUserPrincipal user, ACLType aclType);

  /**
   * Revoke from user aclType access from bucketNameSpace.
   * @param bucketNameSpace
   * @param user
   * @param aclType
   */
  void revokeAccess(BucketNameSpace bucketNameSpace,
                    BasicUserPrincipal user, ACLType aclType);

  /**
   * Grant user aclType access to accountNameSpace.
   * @param accountNameSpace
   * @param user
   * @param aclType
   */
  void grantAccess(AccountNameSpace accountNameSpace,
                   BasicUserPrincipal user,
                   ACLType aclType);

  /**
   * Revoke from user aclType access from bucketNameSpace.
   * @param accountNameSpace
   * @param user
   * @param aclType
   */
  void revokeAccess(AccountNameSpace accountNameSpace,
                    BasicUserPrincipal user, ACLType aclType);

  /**
   * Return all bucketnamespace accesses granted to user.
   * @param user
   * @return list of access
   */
  List<Pair<BucketNameSpace, ACLType>> getAllBucketNameSpaceAccesses(
      BasicUserPrincipal user);

  /**
   * Checks if the user has access to bucketNameSpace.
   * @param bucketNameSpace
   * @param user
   * @return true if access is granted, false otherwise.
   */
  boolean checkAccess(BucketNameSpace bucketNameSpace,
                      BasicUserPrincipal user);

  /**
   * Checks if the user has access to accountNameSpace.
   * @param accountNameSpace
   * @param user
   * @return true if access is granted, false otherwise.
   */
  boolean checkAccess(AccountNameSpace accountNameSpace,
                      BasicUserPrincipal user);

  /**
   * Check access for given ozoneObject. Access for the object would be
   * checked in the context of a MultiTenant environment.
   *
   * @param ozoneObject object for which access needs to be checked.
   * @param context Context object encapsulating all user related information.
   * @throws org.apache.hadoop.ozone.om.exceptions.OMException
   * @return true if user has access else false.
   */
  @Override
  boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OMException;
}
