/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.ozone.om.multitenant;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.security.acl.IOzoneObj;
import org.apache.hadoop.ozone.security.acl.RequestContext;
import org.apache.http.auth.BasicUserPrincipal;

import java.io.IOException;
import java.util.List;

/**
 * Dummy implementation of MultiTenantAccessAuthorizer when some parts of
 * testing don't need to deal with Ranger.
 */
public class MultiTenantAccessAuthorizerDummyPlugin implements
    MultiTenantAccessAuthorizer {

  @Override
  public void init(Configuration configuration) throws IOException {

  }

  @Override
  public void shutdown() throws Exception {

  }

  @Override
  public String assignUser(BasicUserPrincipal principal, String existingRole)
      throws Exception {
    return null;
  }

  @Override
  public String getUserId(BasicUserPrincipal principal) throws Exception {
    return null;
  }

  @Override
  public String getRole(OzoneTenantRolePrincipal principal)
      throws Exception {
    return null;
  }

  @Override
  public void deleteUser(String opaqueUserID) throws IOException {

  }

  @Override
  public String createRole(OzoneTenantRolePrincipal role, String adminRoleName)
      throws IOException {
    return null;
  }

  @Override
  public void deleteGroup(String groupID) throws IOException {

  }

  @Override
  public String createAccessPolicy(AccessPolicy policy) throws Exception {
    return null;
  }

  @Override
  public AccessPolicy getAccessPolicyByName(String policyName)
      throws Exception {
    return null;
  }

  @Override
  public void deletePolicybyId(String policyId) throws IOException {

  }

  @Override
  public void deletePolicybyName(String policyName) throws Exception {

  }

  @Override
  public void grantAccess(BucketNameSpace bucketNameSpace,
      BasicUserPrincipal user, ACLType aclType) {

  }

  @Override
  public void revokeAccess(BucketNameSpace bucketNameSpace,
      BasicUserPrincipal user, ACLType aclType) {

  }

  @Override
  public void grantAccess(AccountNameSpace accountNameSpace,
      BasicUserPrincipal user, ACLType aclType) {

  }

  @Override
  public void revokeAccess(AccountNameSpace accountNameSpace,
      BasicUserPrincipal user, ACLType aclType) {

  }

  @Override
  public List<Pair<BucketNameSpace, ACLType>> getAllBucketNameSpaceAccesses(
      BasicUserPrincipal user) {
    return null;
  }

  @Override
  public boolean checkAccess(BucketNameSpace bucketNameSpace,
      BasicUserPrincipal user) {
    return false;
  }

  @Override
  public boolean checkAccess(AccountNameSpace accountNameSpace,
      BasicUserPrincipal user) {
    return false;
  }

  @Override
  public boolean checkAccess(IOzoneObj ozoneObject, RequestContext context)
      throws OMException {
    return false;
  }
}
