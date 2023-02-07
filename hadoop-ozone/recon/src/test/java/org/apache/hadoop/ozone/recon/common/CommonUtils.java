/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.common;

import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.recon.api.NSSummaryEndpoint;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.KeyObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.security.acl.IAccessAuthorizer;
import org.junit.Assert;

import javax.ws.rs.core.Response;

import java.util.Collections;
import java.util.HashMap;

import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;

/**
 * This is a utility class for common code for test cases.
 */
public class CommonUtils {
  private static final String ROOT_PATH = "/";
  private static final String VOL_PATH = "/vol";
  private static final String BUCKET_ONE_PATH = "/vol/bucket1";
  private static final String BUCKET_TWO_PATH = "/vol/bucket2";
  private static final String DIR_ONE_PATH = "/vol/bucket1/dir1";
  private static final String INVALID_PATH = "/vol/path/not/found";
  private static final String KEY_PATH = "/vol/bucket2/file4";

  private OmPrefixInfo getOmPrefixInfoForTest(
      String path,
      IAccessAuthorizer.ACLIdentityType identityType,
      String identityString,
      IAccessAuthorizer.ACLType aclType,
      OzoneAcl.AclScope scope) {
    return new OmPrefixInfo(path,
        Collections.singletonList(new OzoneAcl(
            identityType, identityString,
            aclType, scope)), new HashMap<>(), 10, 100);
  }

  public void testNSSummaryBasicInfoRoot(
      NSSummaryEndpoint nsSummaryEndpoint,
      ReconOMMetadataManager reconOMMetadataManager) throws Exception {
    String username = "myuser";
    OmPrefixInfo omPrefixInfo = getOmPrefixInfoForTest(ROOT_PATH,
        IAccessAuthorizer.ACLIdentityType.USER,
        username,
        IAccessAuthorizer.ACLType.WRITE,
        ACCESS);
    omPrefixInfo.getMetadata().put("key", "value");
    reconOMMetadataManager.getPrefixTable()
        .put(OzoneConsts.OM_KEY_PREFIX, omPrefixInfo);
    // Test root basics
    Response rootResponse = nsSummaryEndpoint.getBasicInfo(ROOT_PATH);
    NamespaceSummaryResponse rootResponseObj =
        (NamespaceSummaryResponse) rootResponse.getEntity();
    Assert.assertEquals(EntityType.ROOT, rootResponseObj.getEntityType());
    Assert.assertEquals(2, rootResponseObj.getCountStats().getNumVolume());
    Assert.assertEquals(4, rootResponseObj.getCountStats().getNumBucket());
    Assert.assertEquals(5, rootResponseObj.getCountStats().getNumTotalDir());
    Assert.assertEquals(10, rootResponseObj.getCountStats().getNumTotalKey());
    Assert.assertEquals(IAccessAuthorizer.ACLIdentityType.USER,
        rootResponseObj.getObjectDBInfo().getAcls().get(0).getType());
    Assert.assertEquals(IAccessAuthorizer.ACLType.WRITE.toString(),
        rootResponseObj.getObjectDBInfo().getAcls().get(0)
            .getAclList().get(0).toString());
    Assert.assertEquals(username,
        rootResponseObj.getObjectDBInfo().getAcls().get(0).getName());
    Assert.assertEquals("value",
        rootResponseObj.getObjectDBInfo().getMetadata().get("key"));
    Assert.assertEquals(ACCESS,
        rootResponseObj.getObjectDBInfo().getAcls().get(0).getAclScope());
  }

  public void testNSSummaryBasicInfoVolume(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response volResponse = nsSummaryEndpoint.getBasicInfo(VOL_PATH);
    NamespaceSummaryResponse volResponseObj =
        (NamespaceSummaryResponse) volResponse.getEntity();
    Assert.assertEquals(EntityType.VOLUME,
        volResponseObj.getEntityType());
    Assert.assertEquals(2,
        volResponseObj.getCountStats().getNumBucket());
    Assert.assertEquals(4,
        volResponseObj.getCountStats().getNumTotalDir());
    Assert.assertEquals(6,
        volResponseObj.getCountStats().getNumTotalKey());
    Assert.assertEquals("TestUser",
        ((VolumeObjectDBInfo) volResponseObj.
            getObjectDBInfo()).getAdmin());
    Assert.assertEquals("TestUser",
        ((VolumeObjectDBInfo) volResponseObj.
            getObjectDBInfo()).getOwner());
    Assert.assertEquals("vol",
        volResponseObj.getObjectDBInfo().getName());
    Assert.assertEquals(2097152,
        volResponseObj.getObjectDBInfo().getQuotaInBytes());
    Assert.assertEquals(-1,
        volResponseObj.getObjectDBInfo().getQuotaInNamespace());
  }

  public void testNSSummaryBasicInfoBucketOne(BucketLayout bucketLayout,
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response bucketOneResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_ONE_PATH);
    NamespaceSummaryResponse bucketOneObj =
        (NamespaceSummaryResponse) bucketOneResponse.getEntity();
    Assert.assertEquals(EntityType.BUCKET, bucketOneObj.getEntityType());
    Assert.assertEquals(4, bucketOneObj.getCountStats().getNumTotalDir());
    Assert.assertEquals(4, bucketOneObj.getCountStats().getNumTotalKey());
    Assert.assertEquals("vol",
        ((BucketObjectDBInfo) bucketOneObj.getObjectDBInfo()).getVolumeName());
    Assert.assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketOneObj.getObjectDBInfo()).getStorageType());
    Assert.assertEquals(bucketLayout,
        ((BucketObjectDBInfo)
            bucketOneObj.getObjectDBInfo()).getBucketLayout());
    Assert.assertEquals("bucket1",
        ((BucketObjectDBInfo) bucketOneObj.getObjectDBInfo()).getName());
  }

  public void testNSSummaryBasicInfoBucketTwo(
        BucketLayout bucketLayout,
        NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response bucketTwoResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_TWO_PATH);
    NamespaceSummaryResponse bucketTwoObj =
        (NamespaceSummaryResponse) bucketTwoResponse.getEntity();
    Assert.assertEquals(EntityType.BUCKET, bucketTwoObj.getEntityType());
    Assert.assertEquals(0, bucketTwoObj.getCountStats().getNumTotalDir());
    Assert.assertEquals(2, bucketTwoObj.getCountStats().getNumTotalKey());
    Assert.assertEquals("vol",
        ((BucketObjectDBInfo) bucketTwoObj.getObjectDBInfo()).getVolumeName());
    Assert.assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketTwoObj.getObjectDBInfo()).getStorageType());
    Assert.assertEquals(bucketLayout,
        ((BucketObjectDBInfo)
            bucketTwoObj.getObjectDBInfo()).getBucketLayout());
    Assert.assertEquals("bucket2",
        ((BucketObjectDBInfo) bucketTwoObj.getObjectDBInfo()).getName());
  }

  public void testNSSummaryBasicInfoDir(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response dirOneResponse = nsSummaryEndpoint.getBasicInfo(DIR_ONE_PATH);
    NamespaceSummaryResponse dirOneObj =
        (NamespaceSummaryResponse) dirOneResponse.getEntity();
    Assert.assertEquals(EntityType.DIRECTORY, dirOneObj.getEntityType());
    Assert.assertEquals(3,
        dirOneObj.getCountStats().getNumTotalDir());
    Assert.assertEquals(3,
        dirOneObj.getCountStats().getNumTotalKey());
    Assert.assertEquals("dir1",
        dirOneObj.getObjectDBInfo().getName());
    Assert.assertEquals(0,
        dirOneObj.getObjectDBInfo().getMetadata().size());
    Assert.assertEquals(0,
        dirOneObj.getObjectDBInfo().getQuotaInBytes());
    Assert.assertEquals(0,
        dirOneObj.getObjectDBInfo().getQuotaInNamespace());
    Assert.assertEquals(0,
        dirOneObj.getObjectDBInfo().getUsedNamespace());
  }

  public void testNSSummaryBasicInfoNoPath(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response invalidResponse = nsSummaryEndpoint
        .getBasicInfo(INVALID_PATH);
    NamespaceSummaryResponse invalidObj =
        (NamespaceSummaryResponse) invalidResponse.getEntity();
    Assert.assertEquals(ResponseStatus.PATH_NOT_FOUND,
        invalidObj.getStatus());
    Assert.assertEquals(null, invalidObj.getCountStats());
    Assert.assertEquals(null, invalidObj.getObjectDBInfo());
  }

  public void testNSSummaryBasicInfoKey(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response keyResponse = nsSummaryEndpoint.getBasicInfo(KEY_PATH);
    NamespaceSummaryResponse keyResObj =
        (NamespaceSummaryResponse) keyResponse.getEntity();
    Assert.assertEquals(EntityType.KEY, keyResObj.getEntityType());
    Assert.assertEquals("vol",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getVolumeName());
    Assert.assertEquals("bucket2",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getBucketName());
    Assert.assertEquals("file4",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getKeyName());
    Assert.assertEquals(2049,
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getDataSize());
    Assert.assertEquals(HddsProtos.ReplicationType.STAND_ALONE,
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).
            getReplicationConfig().getReplicationType());
  }
}
