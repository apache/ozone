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

package org.apache.hadoop.ozone.om.request.s3.multipart;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.hdds.client.OzoneStoragePolicy;
import org.apache.hadoop.hdds.client.StoragePolicy;
import org.apache.hadoop.ozone.OzoneAcl;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.response.OMClientResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests S3 Initiate Multipart Upload request.
 */
public class TestS3InitiateMultipartUploadRequest
    extends S3MultipartRequestTests {

  @Test
  public void testPreExecute() throws Exception {
    doPreExecuteInitiateMPU(UUID.randomUUID().toString(),
        UUID.randomUUID().toString(), UUID.randomUUID().toString());
  }

  @Test
  public void testValidateAndUpdateCache() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    // Add volume and bucket to DB.
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    Map<String, String> tags = new HashMap<>();
    tags.put("tag-key1", "tag-value1");
    tags.put("tag-key2", "tag-value2");

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName, customMetadata, tags);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    OmKeyInfo openMPUKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey);
    assertNotNull(openMPUKeyInfo);
    assertNotNull(openMPUKeyInfo.getLatestVersionLocations());
    assertTrue(openMPUKeyInfo.getLatestVersionLocations().isMultipartKey());
    assertNotNull(openMPUKeyInfo.getMetadata());
    assertEquals("custom-value1", openMPUKeyInfo.getMetadata().get("custom-key1"));
    assertEquals("custom-value2", openMPUKeyInfo.getMetadata().get("custom-key2"));
    assertNotNull(openMPUKeyInfo.getTags());
    assertEquals("tag-value1", openMPUKeyInfo.getTags().get("tag-key1"));
    assertEquals("tag-value2", openMPUKeyInfo.getTags().get("tag-key2"));

    assertNotNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));

    assertEquals(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID(),
        omMetadataManager.getMultipartInfoTable().get(multipartKey)
            .getUploadID());

    assertEquals(
        modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
            .getModificationTime(), openMPUKeyInfo.getModificationTime());
    assertEquals(
        modifiedRequest.getInitiateMultiPartUploadRequest().getKeyArgs()
            .getModificationTime(), openMPUKeyInfo.getCreationTime());

  }

  /**
   * The schema version is a server-owned decision resolved in {@code preExecute}
   * (on the leader) from the finalized layout version; {@code
   * validateAndUpdateCache} only forwards the stamped value into the persisted
   * multipart info row. Pre-finalization -> legacy (0); finalized -> split (1).
   */
  @ParameterizedTest
  @CsvSource({
      // finalized, expectedSchemaVersion (0 = LEGACY, 1 = SPLIT_PARTS_TABLE)
      "false, 0",
      "true, 1",
  })
  public void testSchemaVersionStampedInPreExecuteByServer(
      boolean finalized, int expectedSchemaVersion) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    if (finalized) {
      finalizeMpuPartsTableSplit();
    }

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    // preExecute (leader) stamps the schema version onto the request.
    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);
    assertEquals(expectedSchemaVersion,
        modifiedRequest.getInitiateMultiPartUploadRequest().getSchemaVersion());

    // validateAndUpdateCache only forwards the already-stamped value.
    OMClientResponse response = getS3InitiateMultipartUploadReq(modifiedRequest)
        .validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());
    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager
        .getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(expectedSchemaVersion, multipartKeyInfo.getSchemaVersion());
  }

  /**
   * The schema version is server-owned: a client-supplied value on the request
   * is ignored and overwritten in {@code preExecute} with the server decision,
   * in both directions (client asks for split pre-finalization, and client asks
   * for legacy post-finalization).
   */
  @ParameterizedTest
  @CsvSource({
      // finalized, expectedSchemaVersion (0 = LEGACY, 1 = SPLIT_PARTS_TABLE)
      "false, 0",
      "true, 1",
  })
  public void testServerIgnoresClientSuppliedSchemaVersion(
      boolean finalized, int expectedSchemaVersion) throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    if (finalized) {
      finalizeMpuPartsTableSplit();
    }

    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    // Client supplies the opposite of what the server should decide.
    int clientSuppliedSchemaVersion = finalized
        ? OmMultipartKeyInfo.LEGACY_SCHEMA_VERSION
        : OmMultipartKeyInfo.SPLIT_PARTS_TABLE_SCHEMA_VERSION;
    OMRequest clientRequest = OMRequestTestUtils.createInitiateMPURequest(
        volumeName, bucketName, keyName, Collections.emptyMap(),
        Collections.emptyMap());
    clientRequest = clientRequest.toBuilder()
        .setInitiateMultiPartUploadRequest(
            clientRequest.getInitiateMultiPartUploadRequest().toBuilder()
                .setSchemaVersion(clientSuppliedSchemaVersion))
        .build();

    // preExecute must overwrite the client value with the server decision.
    OMRequest modifiedRequest =
        getS3InitiateMultipartUploadReq(clientRequest).preExecute(ozoneManager);
    assertEquals(expectedSchemaVersion,
        modifiedRequest.getInitiateMultiPartUploadRequest().getSchemaVersion());

    OMClientResponse response = getS3InitiateMultipartUploadReq(modifiedRequest)
        .validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        response.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());
    OmMultipartKeyInfo multipartKeyInfo = omMetadataManager
        .getMultipartInfoTable().get(multipartKey);
    assertNotNull(multipartKeyInfo);
    assertEquals(expectedSchemaVersion, multipartKeyInfo.getSchemaVersion());
  }

  @Test
  public void testStoragePolicyInRequestOverridesBucketPolicy() throws Exception {
    assertMpuInitiatedWithStoragePolicy(OzoneStoragePolicy.HOT, OzoneStoragePolicy.COLD, OzoneStoragePolicy.COLD);
  }

  @Test
  public void testStoragePolicyDefaultsToBucketPolicy() throws Exception {
    assertMpuInitiatedWithStoragePolicy(OzoneStoragePolicy.COLD, null, OzoneStoragePolicy.COLD);
  }

  @Test
  public void testStoragePolicyDefaultsToClusterDefaultWithoutBucketPolicy() throws Exception {
    assertMpuInitiatedWithStoragePolicy(null, null, OzoneStoragePolicy.getDefaultPolicy());
  }

  /**
   * A request logged by an OM that did not set the policy must be applied without one,
   * not resolved again from the bucket.
   */
  @Test
  public void testRequestWithoutStoragePolicyStoresNone() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());

    OMRequest preExecuted = doPreExecuteInitiateMPU(volumeName, bucketName, keyName);
    OzoneManagerProtocolProtos.MultipartInfoInitiateRequest initiateRequest =
        preExecuted.getInitiateMultiPartUploadRequest();
    OMRequest withoutPolicy = preExecuted.toBuilder()
        .setInitiateMultiPartUploadRequest(initiateRequest.toBuilder()
            .setKeyArgs(initiateRequest.getKeyArgs().toBuilder().clearStoragePolicy()))
        .build();

    OMClientResponse response = getS3InitiateMultipartUploadReq(withoutPolicy)
        .validateAndUpdateCache(ozoneManager, 100L);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(OzoneManagerProtocolProtos.Status.OK);

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        initiateRequest.getKeyArgs().getMultipartUploadID());
    assertThat(omMetadataManager.getMultipartInfoTable().get(multipartKey).getStoragePolicy()).isNull();
    assertThat(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(getMultipartOpenKey(volumeName, bucketName, keyName, initiateRequest.getKeyArgs().getMultipartUploadID()))
        .getStoragePolicy()).isNull();
  }

  /**
   * Before STORAGE_POLICY_SUPPORT is finalized, an old OM ignores the policy field.
   */
  @Test
  public void testStoragePolicyNotStampedBeforeFinalization() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    when(ozoneManager.getBucketInfo(volumeName, bucketName)).thenReturn(OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName)
        .setStoragePolicy(OzoneStoragePolicy.COLD)
        .build());

    OMRequest clientRequest = OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName, keyName,
        Collections.emptyMap(), Collections.emptyMap(), OzoneStoragePolicy.HOT);

    OMRequest preExecuted = getS3InitiateMultipartUploadReq(clientRequest).preExecute(ozoneManager);
    OzoneManagerProtocolProtos.KeyArgs keyArgs = preExecuted.getInitiateMultiPartUploadRequest().getKeyArgs();
    assertThat(keyArgs.hasStoragePolicy()).isFalse();

    OMClientResponse response = getS3InitiateMultipartUploadReq(preExecuted)
        .validateAndUpdateCache(ozoneManager, 100L);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(OzoneManagerProtocolProtos.Status.OK);

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName, keyArgs.getMultipartUploadID());
    assertThat(omMetadataManager.getMultipartInfoTable().get(multipartKey).getStoragePolicy()).isNull();
    assertThat(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(getMultipartOpenKey(volumeName, bucketName, keyName, keyArgs.getMultipartUploadID()))
        .getStoragePolicy()).isNull();
  }

  /**
   * Initiates an upload on a bucket with {@code bucketPolicy} (none if null), asking for
   * {@code requestedPolicy} (none if null), and checks that {@code expectedPolicy} is stamped on the
   * request and stored on both the multipart info and the open key.
   */
  private void assertMpuInitiatedWithStoragePolicy(StoragePolicy bucketPolicy, StoragePolicy requestedPolicy,
      StoragePolicy expectedPolicy) throws Exception {
    finalizeStoragePolicySupport();
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, bucketName,
        omMetadataManager, getBucketLayout());
    OmBucketInfo.Builder bucketInfo = OmBucketInfo.newBuilder()
        .setVolumeName(volumeName)
        .setBucketName(bucketName);
    if (bucketPolicy != null) {
      bucketInfo.setStoragePolicy(bucketPolicy);
    }
    when(ozoneManager.getBucketInfo(volumeName, bucketName)).thenReturn(bucketInfo.build());

    OMRequest clientRequest = OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName, keyName,
        Collections.emptyMap(), Collections.emptyMap(), requestedPolicy);

    OMRequest preExecuted = getS3InitiateMultipartUploadReq(clientRequest).preExecute(ozoneManager);
    OzoneManagerProtocolProtos.KeyArgs keyArgs = preExecuted.getInitiateMultiPartUploadRequest().getKeyArgs();
    assertThat(keyArgs.getStoragePolicy()).isEqualTo(OzoneStoragePolicy.toProto(expectedPolicy));

    OMClientResponse response = getS3InitiateMultipartUploadReq(preExecuted)
        .validateAndUpdateCache(ozoneManager, 100L);
    assertThat(response.getOMResponse().getStatus()).isEqualTo(OzoneManagerProtocolProtos.Status.OK);

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName, keyArgs.getMultipartUploadID());
    assertThat(omMetadataManager.getMultipartInfoTable().get(multipartKey).getStoragePolicy())
        .isEqualTo(expectedPolicy);
    assertThat(omMetadataManager.getOpenKeyTable(getBucketLayout())
        .get(getMultipartOpenKey(volumeName, bucketName, keyName, keyArgs.getMultipartUploadID()))
        .getStoragePolicy()).isEqualTo(expectedPolicy);
  }

  @Test
  public void testValidateAndUpdateCacheWithBucketNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequestTestUtils.addVolumeToDB(volumeName, omMetadataManager);

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(
        volumeName, bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.BUCKET_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    assertNull(omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey));
    assertNull(omMetadataManager.getMultipartInfoTable()
        .get(multipartKey));
  }

  @Test
  public void testValidateAndUpdateCacheWithVolumeNotFound() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName, bucketName,
        keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);

    assertEquals(OzoneManagerProtocolProtos.Status.VOLUME_NOT_FOUND,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    assertNull(omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey));
    assertNull(omMetadataManager.getMultipartInfoTable().get(multipartKey));
  }

  protected String getMultipartKey(String volumeName, String bucketName,
                                   String keyName, String multipartUploadID) {
    return omMetadataManager.getMultipartKey(volumeName,
        bucketName, keyName, multipartUploadID);
  }

  /** Key of the open key table row of a top-level key's multipart upload. */
  protected String getMultipartOpenKey(String volumeName, String bucketName,
                                       String keyName, String multipartUploadID) throws IOException {
    return getMultipartKey(volumeName, bucketName, keyName, multipartUploadID);
  }

  @Test
  public void testMultipartUploadInheritParentDefaultAcls()
      throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    List<OzoneAcl> acls = new ArrayList<>();
    acls.add(OzoneAcl.parseAcl("user:newUser:rw[DEFAULT]"));
    acls.add(OzoneAcl.parseAcl("user:noInherit:rw"));
    acls.add(OzoneAcl.parseAcl("group:newGroup:rwl[DEFAULT]"));

    // create bucket with DEFAULT acls
    OMRequestTestUtils.addVolumeAndBucketToDB(volumeName, omMetadataManager,
        OmBucketInfo.newBuilder().setVolumeName(volumeName)
            .setBucketName(bucketName)
            .setBucketLayout(getBucketLayout())
            .setAcls(acls));

    // Verify bucket has DEFAULT acls.
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    List<OzoneAcl> bucketAcls = omMetadataManager.getBucketTable()
        .get(bucketKey).getAcls();
    assertEquals(acls, bucketAcls);

    // create file with acls inherited from parent DEFAULT acls
    OMRequest modifiedRequest = doPreExecuteInitiateMPU(volumeName,
        bucketName, keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(modifiedRequest);

    OMClientResponse omClientResponse =
        s3InitiateMultipartUploadRequest.validateAndUpdateCache(ozoneManager, 100L);
    assertEquals(OzoneManagerProtocolProtos.Status.OK,
        omClientResponse.getOMResponse().getStatus());

    String multipartKey = getMultipartKey(volumeName, bucketName, keyName,
        modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());

    OmKeyInfo omKeyInfo = omMetadataManager
        .getOpenKeyTable(s3InitiateMultipartUploadRequest.getBucketLayout())
        .get(multipartKey);

    verifyKeyInheritAcls(omKeyInfo.getAcls(), bucketAcls);

  }

  /**
   * Leaf key has ACCESS scope acls which inherited
   * from parent DEFAULT acls.
   */
  private void verifyKeyInheritAcls(List<OzoneAcl> keyAcls,
      List<OzoneAcl> bucketAcls) {

    List<OzoneAcl> parentDefaultAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.DEFAULT)
        .map(acl -> acl.withScope(OzoneAcl.AclScope.ACCESS))
        .collect(Collectors.toList());

    List<OzoneAcl> parentAccessAcl = bucketAcls.stream()
        .filter(acl -> acl.getAclScope() == OzoneAcl.AclScope.ACCESS)
        .collect(Collectors.toList());

    // Should inherit parent DEFAULT Acls
    // [user:newUser:rw[DEFAULT], group:newGroup:rwl[DEFAULT]]
    assertTrue(keyAcls.containsAll(parentDefaultAcl),
        "Failed to inherit parent DEFAULT acls!");

    // Should not inherit parent ACCESS Acls
    assertThat(keyAcls).doesNotContainAnyElementsOf(parentAccessAcl);
  }

}
