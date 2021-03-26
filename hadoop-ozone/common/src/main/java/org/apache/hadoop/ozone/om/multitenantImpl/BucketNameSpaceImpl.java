package org.apache.hadoop.ozone.om.multitenantImpl;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hdds.client.OzoneQuota;
import org.apache.hadoop.hdds.fs.SpaceUsageSource;
import org.apache.hadoop.ozone.om.multitenant.BucketNameSpace;
import org.apache.hadoop.ozone.security.acl.OzoneObj;

public class BucketNameSpaceImpl implements BucketNameSpace {
  private final String bucketNameSpaceID;
  private List<OzoneObj> bucketNameSpaceObjects;

  public BucketNameSpaceImpl(String id) {
    bucketNameSpaceID = id;
    bucketNameSpaceObjects = new ArrayList<>();
  }

  @Override
  public String getBucketNameSpaceID() {
    return bucketNameSpaceID;
  }

  @Override
  public List<OzoneObj> getBucketNameSpaceObjects() {
    return bucketNameSpaceObjects;
  }

  @Override
  public void addBucketNameSpaceObjects(OzoneObj e) {
    bucketNameSpaceObjects.add(e);
  }

  @Override
  public SpaceUsageSource getSpaceUsage() {
    return null;
  }

  @Override
  public void setQuota(OzoneQuota quota) {

  }

  @Override
  public OzoneQuota getQuota() {
    return null;
  }
}
