package org.apache.hadoop.ozone.om.multitenant;

import static org.apache.hadoop.ozone.security.acl.OzoneObj.ResourceType.VOLUME;
import static org.apache.hadoop.ozone.security.acl.OzoneObj.StoreType.OZONE;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.om.multitenantImpl.AccountNameSpaceImpl;
import org.apache.hadoop.ozone.om.multitenantImpl.BucketNameSpaceImpl;
import org.apache.hadoop.ozone.security.acl.OzoneObj;
import org.apache.hadoop.ozone.security.acl.OzoneObjInfo;

public class CephCompatibleTenantImpl implements Tenant {
  private final String tenantID;
  private List<String> tenantGroupsIDs;
  private List<AccessPolicy> accessPolicies;
  private final AccountNameSpace accountNameSpace;
  private final BucketNameSpace bucketNameSpace;


  public CephCompatibleTenantImpl(String id) {
    tenantID = id;
    accessPolicies = new ArrayList<>();
    tenantGroupsIDs = new ArrayList<>();
    accountNameSpace = new AccountNameSpaceImpl(id);
    bucketNameSpace = new BucketNameSpaceImpl(id);
    OzoneObj volume = new OzoneObjInfo.Builder().newBuilder()
        .setResType(VOLUME)
        .setStoreType(OZONE)
        .setVolumeName(bucketNameSpace.getBucketNameSpaceID()).build();
    bucketNameSpace.addBucketNameSpaceObjects(volume);
  }

  @Override
  public String getTenantId() {
    return tenantID;
  }

  @Override
  public AccountNameSpace getTenantAccountNameSpace() {
    return accountNameSpace;
  }

  @Override
  public BucketNameSpace getTenantBucketNameSpace() {
    return bucketNameSpace;
  }

  @Override
  public List<AccessPolicy> getTenantAccessPolicies() {
    return accessPolicies;
  }

  @Override
  public void addTenantAccessPolicy(AccessPolicy policy) {
    accessPolicies.add(policy);
  }

  @Override
  public void removeTenantAccessPolicy(AccessPolicy policy) {
    accessPolicies.remove(policy);
  }

  @Override
  public void addTenantAccessGroup(String groupID) {
    tenantGroupsIDs.add(groupID);

  }

  @Override
  public void removeTenantAccessGroup(String groupID) {
    tenantGroupsIDs.remove(groupID);
  }

  @Override
  public List<String> getTenantGroups() {
    return tenantGroupsIDs;
  }
}
