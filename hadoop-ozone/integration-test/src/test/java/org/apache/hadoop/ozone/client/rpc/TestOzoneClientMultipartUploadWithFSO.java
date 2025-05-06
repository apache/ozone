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

package org.apache.hadoop.ozone.client.rpc;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneKeyLocation;
import org.apache.hadoop.ozone.client.OzoneMultipartUpload;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;
import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.test.NonHATests;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * This test verifies all the S3 multipart client apis - prefix layout.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestOzoneClientMultipartUploadWithFSO implements NonHATests.TestCase {

  private ObjectStore store;
  private MiniOzoneCluster cluster;
  private OzoneClient ozClient;
  private MessageDigest eTagProvider;

  private String volumeName;
  private String bucketName;
  private String keyName;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private boolean originalFileSystemPathEnabled;

  @BeforeAll
  void init() throws Exception {
    cluster = cluster();
    ozClient = cluster.newClient();
    store = ozClient.getObjectStore();
    eTagProvider = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    originalFileSystemPathEnabled = omConfig.isFileSystemPathEnabled();
    omConfig.setFileSystemPathEnabled(true);
  }

  @AfterAll
  void shutdown() {
    IOUtils.closeQuietly(ozClient);
    OmConfig omConfig = cluster().getOzoneManager().getConfig();
    omConfig.setFileSystemPathEnabled(originalFileSystemPathEnabled);
  }

  @BeforeEach
  public void preTest() throws Exception {
    volumeName = UUID.randomUUID().toString();
    bucketName = UUID.randomUUID().toString();
    keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    bucket = volume.getBucket(bucketName);
  }

  @Test
  public void testInitiateMultipartUploadWithReplicationInformationSet() throws
      IOException {
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName,
        ReplicationType.RATIS, ONE);

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    String uploadIDNew = initiateMultipartUploadWithAsserts(bucket, keyName,
        ReplicationType.RATIS, ONE);
    assertNotEquals(uploadIDNew, uploadID);
  }

  @Test
  public void testInitiateMultipartUploadWithDefaultReplication() throws
      IOException {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertEquals(volumeName, multipartInfo.getVolumeName());
    assertEquals(bucketName, multipartInfo.getBucketName());
    assertEquals(keyName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName);

    assertNotNull(multipartInfo);
    assertEquals(volumeName, multipartInfo.getVolumeName());
    assertEquals(bucketName, multipartInfo.getBucketName());
    assertEquals(keyName, multipartInfo.getKeyName());
    assertNotEquals(multipartInfo.getUploadID(), uploadID);
    assertNotNull(multipartInfo.getUploadID());
  }

  @Test
  public void testUploadPartWithNoOverride() throws IOException {
    String sampleData = "sample Value";
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName,
        ReplicationType.RATIS, ONE);

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(sampleData));
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    assertNotNull(commitUploadPartInfo);
    assertNotNull(commitUploadPartInfo.getETag());
  }

  @Test
  public void testUploadPartOverrideWithRatis() throws Exception {
    String sampleData = "sample Value";
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName,
        ReplicationType.RATIS, THREE);

    int partNumber = 1;
    Pair<String, String> partNameAndETag = uploadPart(bucket, keyName, uploadID,
        partNumber, sampleData.getBytes(UTF_8));

    //Overwrite the part by creating part key with same part number.
    Pair<String, String> partNameAndETagNew = uploadPart(bucket, keyName,
        uploadID, partNumber, "name".getBytes(UTF_8));

    // PartName should be same from old part Name.
    // AWS S3 for same content generates same partName during upload part.
    // In AWS S3 ETag is generated from md5sum. In Ozone right now we
    // don't do this. For now to make things work for large file upload
    // through aws s3 cp, the partName are generated in a predictable fashion.
    // So, when a part is override partNames will still be same irrespective
    // of content in ozone s3. This will make S3 Mpu completeMPU pass when
    // comparing part names and large file uploads work using aws cp.
    assertEquals(partNameAndETag.getKey(), partNameAndETagNew.getKey());

    // ETags are not equal due to content differences
    assertNotEquals(partNameAndETag.getValue(), partNameAndETagNew.getValue());

    // old part bytes written needs discard and have only
    // new part bytes in quota for this bucket
    long byteWritten = "name".length() * 3; // data written with replication
    assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        byteWritten);
  }

  @Test
  public void testUploadTwiceWithEC()
      throws IOException, NoSuchAlgorithmException {
    bucketName = UUID.randomUUID().toString();
    bucket = getOzoneECBucket(bucketName);

    byte[] data = generateData(81920, (byte) 97);
    // perform upload and complete
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    String uploadID = multipartInfo.getUploadID();
    int partNumber = 1;

    Pair<String, String> partNameAndETag = uploadPart(bucket, keyName, uploadID,
        partNumber, data);
    
    Map<Integer, String> eTagsMap = new HashMap<>();
    eTagsMap.put(partNumber, partNameAndETag.getValue());
    bucket.completeMultipartUpload(keyName, uploadID, eTagsMap);

    long replicatedSize = QuotaUtil.getReplicatedSize(data.length,
        bucket.getReplicationConfig());
    assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);

    //upload same key again
    multipartInfo = bucket.initiateMultipartUpload(keyName);
    uploadID = multipartInfo.getUploadID();

    partNameAndETag = uploadPart(bucket, keyName, uploadID, partNumber,
        data);

    eTagsMap = new HashMap<>();
    eTagsMap.put(partNumber, partNameAndETag.getValue());
    bucket.completeMultipartUpload(keyName, uploadID, eTagsMap);

    // used sized should remain same, overwrite previous upload
    assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);
  }

  @Test
  public void testUploadAbortWithEC()
      throws IOException, NoSuchAlgorithmException {
    byte[] data = generateData(81920, (byte) 97);

    bucketName = UUID.randomUUID().toString();
    bucket = getOzoneECBucket(bucketName);

    // perform upload and abort
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    String uploadID = multipartInfo.getUploadID();
    int partNumber = 1;
    uploadPart(bucket, keyName, uploadID, partNumber, data);

    long replicatedSize = QuotaUtil.getReplicatedSize(data.length,
        bucket.getReplicationConfig());
    assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);

    bucket.abortMultipartUpload(keyName, uploadID);

    // used size should become zero after aport upload
    assertEquals(volume.getBucket(bucketName).getUsedBytes(), 0);
  }

  private OzoneBucket getOzoneECBucket(String myBucket)
      throws IOException {
    final BucketArgs.Builder bucketArgs = BucketArgs.newBuilder();
    bucketArgs.setDefaultReplicationConfig(
        new DefaultReplicationConfig(
            new ECReplicationConfig(3, 2, ECReplicationConfig.EcCodec.RS,
                (int) OzoneConsts.MB)));

    volume.createBucket(myBucket, bucketArgs.build());
    return volume.getBucket(myBucket);
  }

  @Test
  public void testMultipartUploadWithPartsLessThanMinSize() throws Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);

    // Upload Parts
    Map<Integer, String> eTagsMap = new TreeMap<>();
    // Uploading part 1 with less than min size
    Pair<String, String> partNameAndETag = uploadPart(bucket, keyName, uploadID,
        1, "data".getBytes(UTF_8));
    eTagsMap.put(1, partNameAndETag.getValue());

    partNameAndETag = uploadPart(bucket, keyName, uploadID, 2,
            "data".getBytes(UTF_8));
    eTagsMap.put(2, partNameAndETag.getValue());

    // Complete multipart upload
    OzoneTestUtils.expectOmException(OMException.ResultCodes.ENTITY_TOO_SMALL,
        () -> completeMultipartUpload(bucket, keyName, uploadID, eTagsMap));
  }

  @Test
  public void testMultipartUploadWithDiscardedUnusedPartSize()
      throws Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS, ONE);
    byte[] data = generateData(10000000, (byte) 97);

    // Upload Parts
    Map<Integer, String> eTagsMap = new TreeMap<>();

    // Upload part 1 and add it to the eTagsMap for completing the upload.
    Pair<String, String> partNameAndETag1 = uploadPart(bucket, keyName,
        uploadID, 1, data);
    eTagsMap.put(1, partNameAndETag1.getValue());

    // Upload part 2 and add it to the eTagsMap for completing the upload.
    Pair<String, String> partNameAndETag2 = uploadPart(bucket, keyName,
        uploadID, 2, data);
    eTagsMap.put(2, partNameAndETag2.getValue());

    // Upload part 3 but do not add it to the eTagsMap.
    uploadPart(bucket, keyName, uploadID, 3, data);

    completeMultipartUpload(bucket, keyName, uploadID, eTagsMap);

    // Check the bucket size. Since part number 3 was not added to the eTagsMap,
    // the unused part size should be discarded from the bucket size,
    // 30000000 - 10000000 = 20000000
    long bucketSize = volume.getBucket(bucketName).getUsedBytes();
    assertEquals(bucketSize, data.length * 2);
  }

  @Test
  public void testMultipartUploadWithPartsMisMatchWithListSizeDifferent()
      throws Exception {
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);

    // We have not uploaded any parts, but passing some list it should throw
    // error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, UUID.randomUUID().toString());

    OzoneTestUtils.expectOmException(OMException.ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));
  }

  @Test
  public void testMultipartUploadWithPartsMisMatchWithIncorrectPartName()
      throws Exception {
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));

    // passing with an incorrect part name, should throw INVALID_PART error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(1, UUID.randomUUID().toString());

    OzoneTestUtils.expectOmException(OMException.ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));
  }

  @Test
  public void testMultipartUploadWithMissingParts() throws Exception {
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);

    uploadPart(bucket, keyName, uploadID, 1, "data".getBytes(UTF_8));

    // passing with an incorrect part number, should throw INVALID_PART error.
    TreeMap<Integer, String> partsMap = new TreeMap<>();
    partsMap.put(3, "random");

    OzoneTestUtils.expectOmException(OMException.ResultCodes.INVALID_PART,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));
  }

  @Test
  public void testMultipartPartNumberExceedingAllowedRange() throws Exception {
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName,
        RATIS, ONE);
    byte[] data = "data".getBytes(UTF_8);

    // Multipart part number must be an integer between 1 and 10000. So the
    // part number 1, 5000, 10000 will succeed,
    // the part number 0, 10001 will fail.
    bucket.createMultipartKey(keyName, data.length, 1, uploadID);
    bucket.createMultipartKey(keyName, data.length, 5000, uploadID);
    bucket.createMultipartKey(keyName, data.length, 10000, uploadID);
    OzoneTestUtils.expectOmException(OMException.ResultCodes.INVALID_PART,
        () -> bucket.createMultipartKey(
            keyName, data.length, 0, uploadID));
    OzoneTestUtils.expectOmException(OMException.ResultCodes.INVALID_PART,
        () -> bucket.createMultipartKey(
            keyName, data.length, 10001, uploadID));
  }

  @Test
  public void testCommitPartAfterCompleteUpload() throws Exception {
    String parentDir = "a/b/c/d/";
    keyName = parentDir + UUID.randomUUID();
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS, ONE);

    assertEquals(volume.getBucket(bucketName).getUsedNamespace(), 4);

    // upload part 1.
    byte[] data = generateData(5 * 1024 * 1024,
        (byte) RandomUtils.secure().randomLong());
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    // Do not close output stream for part 2.
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 2, uploadID);
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.write(data, 0, data.length);

    Map<Integer, String> partsMap = new LinkedHashMap<>();
    partsMap.put(1, omMultipartCommitUploadPartInfo.getETag());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        bucket.completeMultipartUpload(keyName,
            uploadID, partsMap);
    assertNotNull(omMultipartUploadCompleteInfo);

    assertNotNull(omMultipartCommitUploadPartInfo);

    byte[] fileContent = new byte[data.length];
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {
      IOUtils.readFully(inputStream, fileContent);
    }
    StringBuilder sb = new StringBuilder(data.length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    sb.append(part1);
    assertEquals(sb.toString(), new String(fileContent, UTF_8));
    OzoneOutputStream finalOzoneOutputStream = ozoneOutputStream;
    OMException ex = assertThrows(OMException.class, () -> finalOzoneOutputStream.close());
    assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR, ex.getResult());
  }

  @Test
  public void testAbortUploadFail() throws Exception {
    OzoneTestUtils.expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR,
        () -> bucket.abortMultipartUpload(keyName, "random"));
  }

  @Test
  public void testAbortUploadFailWithInProgressPartUpload() throws Exception {
    String parentDir = "a/b/c/d/";
    keyName = parentDir + UUID.randomUUID();

    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName,
        RATIS, ONE);

    // Do not close output stream.
    byte[] data = "data".getBytes(UTF_8);
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);

    // Abort before completing part upload.
    bucket.abortMultipartUpload(keyName, uploadID);
    OMException ome = assertThrows(OMException.class, () -> ozoneOutputStream.close());
    assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR, ome.getResult());
  }

  @Test
  public void testAbortUploadSuccessWithOutAnyParts() throws Exception {
    String parentDir = "a/b/c/d/";
    keyName = parentDir + UUID.randomUUID();

    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testAbortUploadSuccessWithParts() throws Exception {
    String parentDir = "a/b/c/d/";
    keyName = parentDir + UUID.randomUUID();

    OzoneManager ozoneManager = cluster.getOzoneManager();
    String buckKey = ozoneManager.getMetadataManager()
        .getBucketKey(volume.getName(), bucket.getName());
    OmBucketInfo buckInfo =
        ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    BucketLayout bucketLayout = buckInfo.getBucketLayout();

    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);
    Pair<String, String> partNameAndETag = uploadPart(bucket, keyName, uploadID,
        1, "data".getBytes(UTF_8));

    OMMetadataManager metadataMgr =
        cluster.getOzoneManager().getMetadataManager();
    String multipartKey = verifyUploadedPart(uploadID, partNameAndETag.getKey(),
        metadataMgr);

    bucket.abortMultipartUpload(keyName, uploadID);

    String multipartOpenKey =
            metadataMgr.getMultipartKeyFSO(volumeName, bucketName, keyName, uploadID);
    OmKeyInfo omKeyInfo =
        metadataMgr.getOpenKeyTable(bucketLayout).get(multipartOpenKey);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);
    assertNull(omKeyInfo);
    assertNull(omMultipartKeyInfo);

    // Since deleteTable operation is performed via
    // batchOp - Table.putWithBatch(), which is an async operation and
    // not making any assertion for the same.
  }

  @Test
  public void testListMultipartUploadParts() throws Exception {
    String parentDir = "a/b/c/d/e/f/";
    keyName = parentDir + "file-ABC";

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);
    Pair<String, String> partNameAndETag1 = uploadPart(bucket, keyName,
        uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(bucket, keyName,
        uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(bucket, keyName,
        uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partNameAndETag3.getKey());

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 3);

    assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    assertEquals(3,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    verifyPartNamesInDB(partsMap,
        ozoneMultipartUploadPartListParts, uploadID);

    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  @Test
  public void testAbortMultipartUploadSuccessWithMissingParentDirectories() throws Exception {
    String parentDir = "parentDirToDelete/";
    keyName = parentDir + UUID.randomUUID();

    OzoneManager ozoneManager = cluster.getOzoneManager();
    String buckKey = ozoneManager.getMetadataManager()
        .getBucketKey(volume.getName(), bucket.getName());
    OmBucketInfo buckInfo =
        ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    BucketLayout bucketLayout = buckInfo.getBucketLayout();

    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);

    OMMetadataManager metadataMgr =
        cluster.getOzoneManager().getMetadataManager();
    String multipartOpenKey =
        metadataMgr.getMultipartKeyFSO(volumeName, bucketName, keyName, uploadID);
    String multipartKey = metadataMgr.getMultipartKey(volumeName, bucketName,
        keyName, uploadID);

    // Delete parent directory
    ozClient.getProxy().deleteKey(volumeName, bucketName, parentDir, false);

    // Abort multipart upload with missing parent directory
    bucket.abortMultipartUpload(keyName, uploadID);

    OmKeyInfo omKeyInfo =
        metadataMgr.getOpenKeyTable(bucketLayout).get(multipartOpenKey);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);
    assertNull(omKeyInfo);
    assertNull(omMultipartKeyInfo);
  }

  private void verifyPartNamesInDB(Map<Integer, String> partsMap,
                                   OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts,
                                   String uploadID) throws IOException {

    List<String> listPartNames = new ArrayList<>();
    String keyPartName = verifyPartNames(partsMap, 0,
        ozoneMultipartUploadPartListParts);
    listPartNames.add(keyPartName);

    keyPartName = verifyPartNames(partsMap, 1,
        ozoneMultipartUploadPartListParts);
    listPartNames.add(keyPartName);

    keyPartName = verifyPartNames(partsMap, 2,
        ozoneMultipartUploadPartListParts);
    listPartNames.add(keyPartName);

    OMMetadataManager metadataMgr =
        cluster.getOzoneManager().getMetadataManager();
    String multipartKey = metadataMgr.getMultipartKey(volumeName, bucketName,
        keyName, uploadID);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);
    assertNotNull(omMultipartKeyInfo);

    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo :
        omMultipartKeyInfo.getPartKeyInfoMap()) {
      String partKeyName = partKeyInfo.getPartName();

      // reconstruct full part name with volume, bucket, partKeyName
      String fullKeyPartName =
          metadataMgr.getOzoneKey(volumeName, bucketName, keyName);

      // partKeyName format in DB - partKeyName + ClientID
      assertTrue(partKeyName.startsWith(fullKeyPartName),
          "Invalid partKeyName format in DB: " + partKeyName
              + ", expected name:" + fullKeyPartName);

      listPartNames.remove(partKeyName);
    }
    assertThat(listPartNames).withFailMessage("Wrong partKeyName format in DB!").isEmpty();
  }

  private String verifyPartNames(Map<Integer, String> partsMap, int index,
                                 OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts) {

    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(index).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(index)
            .getPartName());

    return ozoneMultipartUploadPartListParts.getPartInfoList().get(index)
        .getPartName();
  }

  @Test
  public void testListMultipartUploadPartsWithContinuation()
      throws Exception {
    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);
    Pair<String, String> partNameAndETag1 = uploadPart(bucket, keyName,
        uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(bucket, keyName,
        uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(bucket, keyName,
        uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partNameAndETag3.getKey());

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 2);

    assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    assertEquals(2,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());

    // Get remaining
    assertTrue(ozoneMultipartUploadPartListParts.isTruncated());
    ozoneMultipartUploadPartListParts = bucket.listParts(keyName, uploadID,
        ozoneMultipartUploadPartListParts.getNextPartNumberMarker(), 2);

    assertEquals(1,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());


    // As we don't have any parts for this, we should get false here
    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @ParameterizedTest
  @CsvSource(value = {"-1,2,Should be greater than or equal to zero",
      "1,-1,Max Parts Should be greater than zero"})
  public void testListPartsWithInvalidInputs(int partNumberMarker, int maxParts, String expectedErrorMessage) {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
        () -> bucket.listParts(keyName, "random", partNumberMarker, maxParts));

    assertThat(exception).hasMessageContaining(expectedErrorMessage);
  }

  @Test
  public void testListPartsWithPartMarkerGreaterThanPartCount()
      throws Exception {
    String uploadID = initiateMultipartUploadWithAsserts(bucket, keyName, RATIS,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));


    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 100, 2);

    // Should return empty
    assertEquals(0, ozoneMultipartUploadPartListParts.getPartInfoList().size());

    assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsWithInvalidUploadID() throws Exception {
    OzoneTestUtils
        .expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR, () -> {
          OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
              bucket.listParts(keyName, "random", 100, 2);
        });
  }

  @Test
  public void testListMultipartUpload() throws Exception {
    String dirName = "dir1/dir2/dir3";
    String key1 = "dir1" + "/key1";
    String key2 = "dir1/dir2" + "/key2";
    String key3 = dirName + "/key3";
    List<String> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);

    // Initiate multipart upload
    String uploadID1 = initiateMultipartUploadWithAsserts(bucket, key1, RATIS,
        ONE);
    String uploadID2 = initiateMultipartUploadWithAsserts(bucket, key2, RATIS,
        ONE);
    String uploadID3 = initiateMultipartUploadWithAsserts(bucket, key3, RATIS,
        ONE);

    // Upload Parts
    // Uploading part 1 with less than min size
    uploadPart(bucket, key1, uploadID1, 1, "data".getBytes(UTF_8));
    uploadPart(bucket, key2, uploadID2, 1, "data".getBytes(UTF_8));
    uploadPart(bucket, key3, uploadID3, 1, "data".getBytes(UTF_8));

    OzoneMultipartUploadList listMPUs = bucket.listMultipartUploads("dir1", "", "", 1000);
    assertEquals(3, listMPUs.getUploads().size());
    List<String> expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    assertEquals(0, expectedList.size());

    listMPUs = bucket.listMultipartUploads("dir1/dir2", "", "", 1000);
    assertEquals(2, listMPUs.getUploads().size());
    expectedList = new ArrayList<>();
    expectedList.add(key2);
    expectedList.add(key3);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    assertEquals(0, expectedList.size());

    listMPUs = bucket.listMultipartUploads("dir1/dir2/dir3", "", "", 1000);
    assertEquals(1, listMPUs.getUploads().size());
    expectedList = new ArrayList<>();
    expectedList.add(key3);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    assertEquals(0, expectedList.size());

    // partial key
    listMPUs = bucket.listMultipartUploads("d", "", "", 1000);
    assertEquals(3, listMPUs.getUploads().size());
    expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    assertEquals(0, expectedList.size());

    // partial key
    listMPUs = bucket.listMultipartUploads("", "", "", 1000);
    assertEquals(3, listMPUs.getUploads().size());
    expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    assertEquals(0, expectedList.size());
  }

  @Test
  public void testListMultipartUploadsPagination() throws Exception {
    int numOfKeys = 25;
    List<String> keys = new ArrayList<>();
    Map<String, String> keyToUploadId = new HashMap<>();

    // Generate keys
    for (int i = 0; i < numOfKeys; i++) {
      StringBuilder key = new StringBuilder();
      int depth = 1 + i % 3; // Creates varying depth (1-3 levels)
      for (int j = 0; j < depth; j++) {
        key.append("dir").append(j + 1).append('/');
      }
      key.append("file").append(i);
      keys.add(key.toString());
    }

    for (String key : keys) {
      String uploadId = initiateMultipartUploadWithAsserts(bucket, key, RATIS, ONE);
      keyToUploadId.put(key, uploadId);
      uploadPart(bucket, key, uploadId, 1, "data".getBytes(UTF_8));
    }

    // Test full pagination process
    final int maxUploads = 10;
    final int expectedPages = 3;
    int pageCount = 0;
    String keyMarker = "";
    String uploadIdMarker = "";
    Set<String> retrievedKeys = new HashSet<>();
    boolean isTruncated = true;

    do {
      OzoneMultipartUploadList result = bucket.listMultipartUploads(
          "dir", keyMarker, uploadIdMarker, maxUploads);

      if (pageCount < 2) {
        assertEquals(maxUploads, result.getUploads().size());
        assertTrue(result.isTruncated());
      } else {
        assertEquals(numOfKeys - pageCount * maxUploads, result.getUploads().size());
        assertFalse(result.isTruncated());
      }

      for (OzoneMultipartUpload upload : result.getUploads()) {
        String key = upload.getKeyName();
        retrievedKeys.add(key);

        assertEquals(keyToUploadId.get(key), upload.getUploadId());
      }

      // Update markers for next iteration
      keyMarker = result.getNextKeyMarker();
      uploadIdMarker = result.getNextUploadIdMarker();
      isTruncated = result.isTruncated();

      pageCount++;
    } while (isTruncated);

    assertEquals(keys.size(), retrievedKeys.size());
    assertEquals(expectedPages, pageCount);
    assertThat(retrievedKeys.stream().sorted().collect(Collectors.toList()))
        .as("Retrieved keys should match expected keys in order")
        .isEqualTo(keys.stream().sorted().collect(Collectors.toList()));
  }

  @Test
  void testGetAllPartsWhenZeroPartNumber() throws Exception {
    String parentDir = "a/b/c/d/e/f/";
    keyName = parentDir + "file-ABC";
    OzoneVolume s3volume = store.getVolume("s3v");
    s3volume.createBucket(bucketName);
    OzoneBucket s3Bucket = s3volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(s3Bucket, keyName, RATIS,
            ONE);
    Pair<String, String> partNameAndETag1 = uploadPart(s3Bucket, keyName,
            uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(s3Bucket, keyName,
            uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(s3Bucket, keyName,
            uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(3, partNameAndETag3.getKey());

    s3Bucket.completeMultipartUpload(keyName, uploadID, partsMap);

    OzoneKeyDetails s3KeyDetailsWithAllParts = ozClient.getProxy()
            .getS3KeyDetails(s3Bucket.getName(), keyName, 0);
    List<OzoneKeyLocation> ozoneKeyLocations = s3KeyDetailsWithAllParts.getOzoneKeyLocations();
    assertEquals(6, ozoneKeyLocations.size());
  }

  @Test
  void testGetParticularPart() throws Exception {
    String parentDir = "a/b/c/d/e/f/";
    keyName = parentDir + "file-ABC";
    OzoneVolume s3volume = store.getVolume("s3v");
    s3volume.createBucket(bucketName);
    OzoneBucket s3Bucket = s3volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(s3Bucket, keyName, RATIS,
            ONE);
    Pair<String, String> partNameAndETag1 = uploadPart(s3Bucket, keyName,
            uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(s3Bucket, keyName,
            uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(s3Bucket, keyName,
            uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(3, partNameAndETag3.getKey());

    s3Bucket.completeMultipartUpload(keyName, uploadID, partsMap);

//  OzoneKeyLocations size is 2 because part size is 5MB and ozone.scm.block.size in ozone-site.xml
//  for integration-test is 4MB
    OzoneKeyDetails s3KeyDetailsOneParts = ozClient.getProxy().getS3KeyDetails(bucketName, keyName, 1);
    assertEquals(2, s3KeyDetailsOneParts.getOzoneKeyLocations().size());

    OzoneKeyDetails s3KeyDetailsTwoParts = ozClient.getProxy().getS3KeyDetails(bucketName, keyName, 2);
    assertEquals(2, s3KeyDetailsTwoParts.getOzoneKeyLocations().size());

    OzoneKeyDetails s3KeyDetailsThreeParts = ozClient.getProxy().getS3KeyDetails(bucketName, keyName, 3);
    assertEquals(2, s3KeyDetailsThreeParts.getOzoneKeyLocations().size());
  }

  @Test
  void testGetNotExistedPart() throws Exception {
    String parentDir = "a/b/c/d/e/f/";
    keyName = parentDir + "file-ABC";
    OzoneVolume s3volume = store.getVolume("s3v");
    s3volume.createBucket(bucketName);
    OzoneBucket s3Bucket = s3volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(s3Bucket, keyName, RATIS,
            ONE);
    Pair<String, String> partNameAndETag1 = uploadPart(s3Bucket, keyName,
            uploadID, 1, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(1, partNameAndETag1.getKey());

    Pair<String, String> partNameAndETag2 = uploadPart(s3Bucket, keyName,
            uploadID, 2, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(2, partNameAndETag2.getKey());

    Pair<String, String> partNameAndETag3 = uploadPart(s3Bucket, keyName,
            uploadID, 3, generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte) 97));
    partsMap.put(3, partNameAndETag3.getKey());

    s3Bucket.completeMultipartUpload(keyName, uploadID, partsMap);

    OzoneKeyDetails s3KeyDetailsWithNotExistedParts = ozClient.getProxy()
            .getS3KeyDetails(s3Bucket.getName(), keyName, 4);
    List<OzoneKeyLocation> ozoneKeyLocations = s3KeyDetailsWithNotExistedParts.getOzoneKeyLocations();
    assertEquals(0, ozoneKeyLocations.size());
  }

  private String verifyUploadedPart(String uploadID, String partName,
                                    OMMetadataManager metadataMgr) throws IOException {
    OzoneManager ozoneManager = cluster.getOzoneManager();
    String buckKey = ozoneManager.getMetadataManager()
        .getBucketKey(volumeName, bucketName);
    OmBucketInfo buckInfo =
        ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    BucketLayout bucketLayout = buckInfo.getBucketLayout();
    String multipartOpenKey =
        metadataMgr.getMultipartKeyFSO(volumeName, bucketName, keyName, uploadID);

    String multipartKey = metadataMgr.getMultipartKey(volumeName, bucketName,
        keyName, uploadID);
    OmKeyInfo omKeyInfo =
        metadataMgr.getOpenKeyTable(bucketLayout).get(multipartOpenKey);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);

    assertNotNull(omKeyInfo);
    assertNotNull(omMultipartKeyInfo);
    assertEquals(keyName, omKeyInfo.getKeyName());
    assertEquals(OzoneFSUtils.getFileName(keyName), omKeyInfo.getFileName());
    assertEquals(uploadID, omMultipartKeyInfo.getUploadID());

    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo :
        omMultipartKeyInfo.getPartKeyInfoMap()) {
      OmKeyInfo currentKeyPartInfo =
          OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

      assertEquals(keyName, currentKeyPartInfo.getKeyName());

      // verify dbPartName
      assertEquals(partName, partKeyInfo.getPartName());
    }
    return multipartKey;
  }

  private String initiateMultipartUploadWithAsserts(
          OzoneBucket oBucket, String kName, ReplicationType replicationType, ReplicationFactor replicationFactor
  ) throws IOException {
    OmMultipartInfo multipartInfo = oBucket.initiateMultipartUpload(kName, replicationType, replicationFactor);

    assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    assertEquals(volumeName, multipartInfo.getVolumeName());
    assertEquals(bucketName, multipartInfo.getBucketName());
    assertEquals(kName, multipartInfo.getKeyName());
    assertNotNull(multipartInfo.getUploadID());

    return uploadID;
  }

  private String initiateMultipartUpload(
          OzoneBucket oBucket, String kName, ReplicationType replicationType, ReplicationFactor replicationFactor
  ) throws IOException {
    OmMultipartInfo multipartInfo = oBucket.initiateMultipartUpload(kName, replicationType, replicationFactor);
    return multipartInfo.getUploadID();
  }

  private Pair<String, String> uploadPart(OzoneBucket oBucket, String kName,
                                          String uploadID, int partNumber,
                                          byte[] data)
      throws IOException, NoSuchAlgorithmException {

    OzoneOutputStream ozoneOutputStream = oBucket.createMultipartKey(kName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG,
        DatatypeConverter.printHexBinary(eTagProvider.digest(data))
            .toLowerCase());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    assertNotNull(omMultipartCommitUploadPartInfo);
    assertNotNull(omMultipartCommitUploadPartInfo.getETag());

    assertNotNull(omMultipartCommitUploadPartInfo.getPartName());

    return Pair.of(omMultipartCommitUploadPartInfo.getPartName(),
        omMultipartCommitUploadPartInfo.getETag());
  }

  private void completeMultipartUpload(OzoneBucket oBucket, String kName,
                                       String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = oBucket
        .completeMultipartUpload(kName, uploadID, partsMap);

    assertNotNull(omMultipartUploadCompleteInfo);
    assertEquals(omMultipartUploadCompleteInfo.getBucket(), oBucket
        .getName());
    assertEquals(omMultipartUploadCompleteInfo.getVolume(), oBucket
        .getVolumeName());
    assertEquals(omMultipartUploadCompleteInfo.getKey(), kName);
    assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }
}
