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
package org.apache.hadoop.ozone.om;

import static org.apache.hadoop.ozone.om.multitenant.AccessPolicy.AccessGrantType.ALLOW;
import static org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal.OzonePrincipalType.GROUP_PRINCIPAL;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.CREATE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.LIST;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.NONE;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.READ_ACL;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.BUCKET;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.KEY;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenant.AccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.AccountNameSpace;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.om.multitenant.CephCompatibleTenantImpl;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantGateKeeper;
import org.apache.hadoop.ozone.om.multitenant.MultiTenantGateKeeperRangerPlugin;
import org.apache.hadoop.ozone.om.multitenant.OzoneMultiTenantPrincipal;
import org.apache.hadoop.ozone.om.multitenant.RangerAccessPolicy;
import org.apache.hadoop.ozone.om.multitenant.Tenant;
import org.apache.hadoop.ozone.om.multitenantImpl.OzoneMultiTenantPrincipalImpl;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;
import org.apache.http.auth.BasicUserPrincipal;

public class OMMultiTenantManagerImpl implements OMMultiTenantManager {
  private MultiTenantGateKeeper gateKeeper;
  private OMMetadataManager omMetadataManager;
  private OzoneConfiguration conf;
  private Map<String, Tenant> allTenants;

  OMMultiTenantManagerImpl(OMMetadataManager mgr, OzoneConfiguration conf)
      throws IOException {
    this.conf = conf;
    allTenants = new ConcurrentHashMap<>();
    omMetadataManager = mgr;
    start(conf);
  }

  @Override
  public void start(OzoneConfiguration configuration) throws IOException {
    gateKeeper = new MultiTenantGateKeeperRangerPlugin();
    gateKeeper.init(configuration);
  }

  @Override
  public void stop() throws Exception {

  }

  @Override
  public OMMetadataManager getOmMetadataManager() {
    return omMetadataManager;
  }

  @Override
  public Tenant createTenant(String tenantID) throws IOException {

    Tenant tenant = new CephCompatibleTenantImpl(tenantID);
    try {
      allTenants.put(tenantID, tenant);

      //TBD : for now just create state in the Ranger.
      // OM state is already created in ValidateAndUpdateCache for
      // the ratis transaction.
      OzoneMultiTenantPrincipal groupTenantAllUsers = getOzonePrincipal(
          tenantID, "GroupTenantAllUsers", GROUP_PRINCIPAL);
      String groupTenantAllUsersID =
          gateKeeper.createGroup(groupTenantAllUsers);
      tenant.addTenantAccessGroup(groupTenantAllUsersID);

      BucketNameSpace bucketNameSpace = tenant.getTenantBucketNameSpace();
      for (OzoneObj volume : bucketNameSpace.getBucketNameSpaceObjects()) {
        String volumeName = volume.getVolumeName();
        // Allow Volume List access
        AccessPolicy tenantVolumeAccessPolicy = createVolumeAccessPolicy(
            volumeName, tenantID, groupTenantAllUsers.getUserID());
        tenantVolumeAccessPolicy.setPolicyID(
            gateKeeper.createAccessPolicy(tenantVolumeAccessPolicy));
        tenant.addTenantAccessPolicy(tenantVolumeAccessPolicy);

        // Allow Bucket Create within Volume
        AccessPolicy tenantBucketCreatePolicy = allowCreateBucketPolicy(
            volumeName, tenantID, groupTenantAllUsers.getUserID());
        tenantBucketCreatePolicy.setPolicyID(
            gateKeeper.createAccessPolicy(tenantBucketCreatePolicy));
        tenant.addTenantAccessPolicy(tenantBucketCreatePolicy);
      }
    } catch (Exception e) {
      throw new IOException(e.getMessage());
    }
    return tenant;
  }

  @Override
  public Tenant getTenantInfo(String tenantID) throws IOException {
    return null;
  }

  @Override
  public void deactivateTenant(String tenantID) throws IOException {

  }

  @Override
  public void destroyTenant(Tenant tenant) throws Exception {
    for (AccessPolicy policy : tenant.getTenantAccessPolicies()) {
      gateKeeper.deletePolicy(policy.getPolicyID());
    }
    for (String groupID : tenant.getTenantGroups()) {
      gateKeeper.deleteGroup(groupID);
    }
  }

  @Override
  public OzoneMultiTenantPrincipal createUser(
      String tenantName, String userName) throws IOException {
    return null;
  }

  @Override
  public String getUserSecret(OzoneMultiTenantPrincipal user)
      throws IOException {
    return "";
  }

  @Override
  public void modifyUser(OzoneMultiTenantPrincipal user) throws IOException {

  }

  @Override
  public void deactivateUser(OzoneMultiTenantPrincipal user)
      throws IOException {

  }

  @Override
  public List<OzoneMultiTenantPrincipal> listAllUsers(String tenantID)
      throws IOException {
    return null;
  }

  @Override
  public Tenant getUserTenantInfo(OzoneMultiTenantPrincipal user)
      throws IOException {
    return null;
  }

  @Override
  public void assignTenantAdminRole(OzoneMultiTenantPrincipal user)
      throws IOException {

  }

  @Override
  public void revokeTenantAdmin(OzoneMultiTenantPrincipal user)
      throws IOException {

  }

  @Override
  public List<OzoneMultiTenantPrincipal> listAllTenantAdmin(String tenantID)
      throws IOException {
    return null;
  }

  @Override
  public void grantAccess(OzoneMultiTenantPrincipal user,
      BucketNameSpace bucketNameSpace) throws IOException {

  }

  @Override
  public void revokeAccess(OzoneMultiTenantPrincipal user,
      BucketNameSpace bucketNameSpace) throws IOException {

  }

  @Override
  public void grantAccess(OzoneMultiTenantPrincipal user,
      AccountNameSpace accountNameSpace) throws IOException {

  }

  @Override
  public void revokeAccess(OzoneMultiTenantPrincipal user,
      AccountNameSpace accountNameSpace) throws IOException {

  }

  @Override
  public String createTenantDefaultPolicy(Tenant tenant,
      AccessPolicy policy) throws IOException {
    return null;
  }

  @Override
  public List<Pair<String, AccessPolicy>> listDefaultTenantPolicies(
      Tenant tenant) throws IOException {
    return null;
  }

  @Override
  public List<Pair<String, AccessPolicy>> listAllTenantPolicies(
      Tenant tenant) throws IOException {
    return null;
  }

  @Override
  public void updateTenantPolicy(Tenant tenant, String policyID,
      AccessPolicy policy) throws IOException {

  }

  private OzoneMultiTenantPrincipal getOzonePrincipal(String tenant, String id,
      OzoneMultiTenantPrincipal.OzonePrincipalType type) {
    OzoneMultiTenantPrincipal principal = new OzoneMultiTenantPrincipalImpl(
        new BasicUserPrincipal(tenant),
        new BasicUserPrincipal(id), type);
    return principal;
  }

  private AccessPolicy createVolumeAccessPolicy(String vol, String tenant,
      String group) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "VolumeAccess" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(VOLUME).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("").setKeyName("").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, READ, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, LIST, ALLOW);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal,
        READ_ACL, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowCreateBucketPolicy(String vol, String tenant,
      String group) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketCreate" + vol + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName("*").setKeyName("").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, CREATE, ALLOW);
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessBucketPolicy(String vol, String tenant,
      String group, String bucketName) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketAccess" + vol + bucketName + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(BUCKET).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    for (ACLType acl : ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }

  private AccessPolicy allowAccessKeyPolicy(String vol, String tenant,
      String group, String bucketName) throws IOException {
    AccessPolicy tenantVolumeAccessPolicy = new RangerAccessPolicy(
        tenant + group + "AllowBucketKeyAccess" + vol + bucketName + "Policy");
    OzoneObjInfo obj = OzoneObjInfo.Builder.newBuilder()
        .setResType(KEY).setStoreType(OZONE).setVolumeName(vol)
        .setBucketName(bucketName).setKeyName("*").build();
    OzoneMultiTenantPrincipal principal = getOzonePrincipal(tenant, group,
        GROUP_PRINCIPAL);
    for (ACLType acl :ACLType.values()) {
      if (acl != NONE) {
        tenantVolumeAccessPolicy.addAccessPolicyElem(obj, principal, acl,
            ALLOW);
      }
    }
    return tenantVolumeAccessPolicy;
  }
}
