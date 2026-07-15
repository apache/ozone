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

package org.apache.hadoop.ozone.recon.api;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.OzoneAcl.AclScope.ACCESS;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLIdentityType.USER;
import static org.apache.hadoop.ozone.security.acl.IAccessAuthorizer.ACLType.WRITE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.protocol.StorageType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmPrefixInfo;
import org.apache.hadoop.ozone.recon.api.types.BucketObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.EntityType;
import org.apache.hadoop.ozone.recon.api.types.KeyObjectDBInfo;
import org.apache.hadoop.ozone.recon.api.types.NamespaceSummaryResponse;
import org.apache.hadoop.ozone.recon.api.types.ResponseStatus;
import org.apache.hadoop.ozone.recon.api.types.VolumeObjectDBInfo;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;

/**
 * Shared NSSummary test cases.
 */
public abstract class NSSummaryTests {
  private static final String ROOT_PATH = "/";
  private static final String VOL_PATH = "/vol";
  private static final String BUCKET_ONE_PATH = "/vol/bucket1";
  private static final String BUCKET_TWO_PATH = "/vol/bucket2";
  private static final String DIR_ONE_PATH = "/vol/bucket1/dir1";
  private static final String INVALID_PATH = "/vol/path/not/found";
  private static final String KEY_PATH = "/vol/bucket2/file4";

  public static void testNSSummaryBasicInfoRoot(
      NSSummaryEndpoint nsSummaryEndpoint,
      ReconOMMetadataManager reconOMMetadataManager) throws Exception {
    String username = "myuser";
    OmPrefixInfo omPrefixInfo = OmPrefixInfo.newBuilder()
        .setName(ROOT_PATH)
        .setObjectID(10)
        .setUpdateID(100)
        .setAcls(singletonList(OzoneAcl.of(USER, username, ACCESS, WRITE)))
        .addAllMetadata(singletonMap("key", "value"))
        .build();
    reconOMMetadataManager.getPrefixTable()
        .put(OzoneConsts.OM_KEY_PREFIX, omPrefixInfo);
    // Test root basics
    Response rootResponse = nsSummaryEndpoint.getBasicInfo(ROOT_PATH);
    NamespaceSummaryResponse rootResponseObj =
        (NamespaceSummaryResponse) rootResponse.getEntity();
    assertEquals(EntityType.ROOT, rootResponseObj.getEntityType());
    assertEquals(3, rootResponseObj.getCountStats().getNumVolume());
    assertEquals(5, rootResponseObj.getCountStats().getNumBucket());
    assertEquals(7, rootResponseObj.getCountStats().getNumTotalDir());
    assertEquals(14, rootResponseObj.getCountStats().getNumTotalKey());
    assertEquals("USER",
        rootResponseObj.getObjectDBInfo().getAcls().get(0).getType());
    assertEquals("WRITE", rootResponseObj.getObjectDBInfo().getAcls().get(0)
            .getAclList().get(0));
    assertEquals(username,
        rootResponseObj.getObjectDBInfo().getAcls().get(0).getName());
    assertEquals("value",
        rootResponseObj.getObjectDBInfo().getMetadata().get("key"));
    assertEquals("ACCESS",
        rootResponseObj.getObjectDBInfo().getAcls().get(0).getScope());
  }

  public void testNSSummaryBasicInfoVolume(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response volResponse = nsSummaryEndpoint.getBasicInfo(VOL_PATH);
    NamespaceSummaryResponse volResponseObj =
        (NamespaceSummaryResponse) volResponse.getEntity();
    assertEquals(EntityType.VOLUME,
        volResponseObj.getEntityType());
    assertEquals(2, volResponseObj.getCountStats().getNumBucket());
    assertEquals(4, volResponseObj.getCountStats().getNumTotalDir());
    assertEquals(7, volResponseObj.getCountStats().getNumTotalKey());
    assertEquals("TestUser", ((VolumeObjectDBInfo) volResponseObj.
            getObjectDBInfo()).getAdmin());
    assertEquals("TestUser", ((VolumeObjectDBInfo) volResponseObj.
            getObjectDBInfo()).getOwner());
    assertEquals("vol", volResponseObj.getObjectDBInfo().getName());
    assertEquals(2097152, volResponseObj.getObjectDBInfo().getQuotaInBytes());
    assertEquals(-1, volResponseObj.getObjectDBInfo().getQuotaInNamespace());
  }

  public void testNSSummaryBasicInfoBucketOne(BucketLayout bucketLayout,
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response bucketOneResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_ONE_PATH);
    NamespaceSummaryResponse bucketOneObj =
        (NamespaceSummaryResponse) bucketOneResponse.getEntity();
    assertEquals(EntityType.BUCKET, bucketOneObj.getEntityType());
    assertEquals(4, bucketOneObj.getCountStats().getNumTotalDir());
    assertEquals(5, bucketOneObj.getCountStats().getNumTotalKey());
    assertEquals("vol",
        ((BucketObjectDBInfo) bucketOneObj.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketOneObj.getObjectDBInfo()).getStorageType());
    assertEquals(bucketLayout,
        ((BucketObjectDBInfo)
            bucketOneObj.getObjectDBInfo()).getBucketLayout());
    assertEquals("bucket1",
        ((BucketObjectDBInfo) bucketOneObj.getObjectDBInfo()).getName());
  }

  public void testNSSummaryBasicInfoBucketTwo(
        BucketLayout bucketLayout,
        NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response bucketTwoResponse =
        nsSummaryEndpoint.getBasicInfo(BUCKET_TWO_PATH);
    NamespaceSummaryResponse bucketTwoObj =
        (NamespaceSummaryResponse) bucketTwoResponse.getEntity();
    assertEquals(EntityType.BUCKET, bucketTwoObj.getEntityType());
    assertEquals(0, bucketTwoObj.getCountStats().getNumTotalDir());
    assertEquals(2, bucketTwoObj.getCountStats().getNumTotalKey());
    assertEquals("vol",
        ((BucketObjectDBInfo) bucketTwoObj.getObjectDBInfo()).getVolumeName());
    assertEquals(StorageType.DISK,
        ((BucketObjectDBInfo)
            bucketTwoObj.getObjectDBInfo()).getStorageType());
    assertEquals(bucketLayout,
        ((BucketObjectDBInfo)
            bucketTwoObj.getObjectDBInfo()).getBucketLayout());
    assertEquals("bucket2",
        ((BucketObjectDBInfo) bucketTwoObj.getObjectDBInfo()).getName());
  }

  public void testNSSummaryBasicInfoDir(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response dirOneResponse = nsSummaryEndpoint.getBasicInfo(DIR_ONE_PATH);
    NamespaceSummaryResponse dirOneObj =
        (NamespaceSummaryResponse) dirOneResponse.getEntity();
    assertEquals(EntityType.DIRECTORY, dirOneObj.getEntityType());
    assertEquals(3, dirOneObj.getCountStats().getNumTotalDir());
    assertEquals(4, dirOneObj.getCountStats().getNumTotalKey());
    assertEquals("dir1", dirOneObj.getObjectDBInfo().getName());
    assertEquals(0, dirOneObj.getObjectDBInfo().getMetadata().size());
    assertEquals(0, dirOneObj.getObjectDBInfo().getQuotaInBytes());
    assertEquals(0, dirOneObj.getObjectDBInfo().getQuotaInNamespace());
    assertEquals(0, dirOneObj.getObjectDBInfo().getUsedNamespace());
  }

  public void testNSSummaryBasicInfoNoPath(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response invalidResponse = nsSummaryEndpoint
        .getBasicInfo(INVALID_PATH);
    NamespaceSummaryResponse invalidObj =
        (NamespaceSummaryResponse) invalidResponse.getEntity();
    assertEquals(ResponseStatus.PATH_NOT_FOUND, invalidObj.getStatus());
    assertNull(invalidObj.getCountStats());
    assertNull(invalidObj.getObjectDBInfo());
  }

  public void testNSSummaryBasicInfoKey(
      NSSummaryEndpoint nsSummaryEndpoint) throws Exception {
    Response keyResponse = nsSummaryEndpoint.getBasicInfo(KEY_PATH);
    NamespaceSummaryResponse keyResObj =
        (NamespaceSummaryResponse) keyResponse.getEntity();
    assertEquals(EntityType.KEY, keyResObj.getEntityType());
    assertEquals("vol",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getVolumeName());
    assertEquals("bucket2",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getBucketName());
    assertEquals("file4",
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getKeyName());
    assertEquals(2049,
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).getDataSize());
    assertEquals(HddsProtos.ReplicationType.STAND_ALONE,
        ((KeyObjectDBInfo) keyResObj.getObjectDBInfo()).
            getReplicationConfig().getReplicationType());
  }
}
