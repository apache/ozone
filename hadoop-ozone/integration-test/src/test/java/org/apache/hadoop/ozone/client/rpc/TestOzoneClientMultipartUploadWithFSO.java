/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.ozone.client.rpc;

import java.util.HashMap;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.DefaultReplicationConfig;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.RatisReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;
import org.apache.hadoop.ozone.client.OzoneMultipartUpload;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadList;
import org.apache.hadoop.ozone.client.OzoneMultipartUploadPartListParts;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.OmBucketInfo;
import org.apache.hadoop.ozone.om.helpers.OmKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartCommitUploadPartInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartInfo;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.OmMultipartKeyInfo;
import org.apache.hadoop.ozone.om.helpers.OmMultipartUploadCompleteInfo;
import org.apache.hadoop.ozone.om.helpers.OzoneFSUtils;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.StringUtils.string2Bytes;
import static org.apache.hadoop.hdds.client.ReplicationFactor.THREE;

import org.apache.hadoop.ozone.om.helpers.QuotaUtil;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * This test verifies all the S3 multipart client apis - prefix layout.
 */
@Timeout(300)
public class TestOzoneClientMultipartUploadWithFSO {

  private static ObjectStore store = null;
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;

  private static String scmId = UUID.randomUUID().toString();
  private String volumeName;
  private String bucketName;
  private String keyName;
  private OzoneVolume volume;
  private OzoneBucket bucket;

  /**
   * Create a MiniOzoneCluster for testing.
   * <p>
   * Ozone is made active by setting OZONE_ENABLED = true
   *
   * @throws IOException
   */
  @BeforeAll
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OMRequestTestUtils.configureFSOptimizedPaths(conf, true);
    startCluster(conf);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterAll
  public static void shutdown() throws IOException {
    shutdownCluster();
  }


  /**
   * Create a MiniOzoneCluster for testing.
   * @param conf Configurations to start the cluster.
   * @throws Exception
   */
  static void startCluster(OzoneConfiguration conf) throws Exception {
    cluster = MiniOzoneCluster.newBuilder(conf)
        .setNumDatanodes(5)
        .setTotalPipelineNumLimit(10)
        .setScmId(scmId)
        .build();
    cluster.waitForClusterToBeReady();
    ozClient = OzoneClientFactory.getRpcClient(conf);
    store = ozClient.getObjectStore();
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  static void shutdownCluster() throws IOException {
    if (ozClient != null) {
      ozClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
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
    String uploadID = initiateMultipartUpload(bucket, keyName,
        ReplicationType.RATIS, ONE);

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    String uploadIDNew = initiateMultipartUpload(bucket, keyName,
        ReplicationType.RATIS, ONE);
    Assertions.assertNotEquals(uploadIDNew, uploadID);
  }

  @Test
  public void testInitiateMultipartUploadWithDefaultReplication() throws
      IOException {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    Assertions.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assertions.assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName);

    Assertions.assertNotNull(multipartInfo);
    Assertions.assertNotEquals(multipartInfo.getUploadID(), uploadID);
    Assertions.assertNotNull(multipartInfo.getUploadID());
  }

  @Test
  public void testUploadPartWithNoOverride() throws IOException {
    String sampleData = "sample Value";
    String uploadID = initiateMultipartUpload(bucket, keyName,
        ReplicationType.RATIS, ONE);

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
        .getCommitUploadPartInfo();

    Assertions.assertNotNull(commitUploadPartInfo);
    Assertions.assertNotNull(commitUploadPartInfo.getPartName());
  }

  @Test
  public void testUploadPartOverrideWithRatis() throws Exception {
    String sampleData = "sample Value";
    String uploadID = initiateMultipartUpload(bucket, keyName,
        ReplicationType.RATIS, THREE);

    int partNumber = 1;
    String partName = uploadPart(bucket, keyName, uploadID, partNumber,
        sampleData.getBytes(UTF_8));

    //Overwrite the part by creating part key with same part number.
    String partNameNew = uploadPart(bucket, keyName, uploadID, partNumber,
        "name".getBytes(UTF_8));

    // PartName should be same from old part Name.
    // AWS S3 for same content generates same partName during upload part.
    // In AWS S3 ETag is generated from md5sum. In Ozone right now we
    // don't do this. For now to make things work for large file upload
    // through aws s3 cp, the partName are generated in a predictable fashion.
    // So, when a part is override partNames will still be same irrespective
    // of content in ozone s3. This will make S3 Mpu completeMPU pass when
    // comparing part names and large file uploads work using aws cp.
    Assertions.assertEquals(partName, partNameNew, "Part names should be same");

    // old part bytes written needs discard and have only
    // new part bytes in quota for this bucket
    long byteWritten = "name".length() * 3; // data written with replication
    Assertions.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        byteWritten);
  }

  @Test
  public void testUploadTwiceWithEC() throws IOException {
    bucketName = UUID.randomUUID().toString();
    bucket = getOzoneECBucket(bucketName);

    byte[] data = generateData(81920, (byte) 97);
    // perform upload and complete
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    String uploadID = multipartInfo.getUploadID();
    int partNumber = 1;

    String partName = uploadPart(bucket, keyName, uploadID, partNumber,
        data);

    Map<Integer, String> partsMap = new HashMap<>();
    partsMap.put(partNumber, partName);
    bucket.completeMultipartUpload(keyName, uploadID, partsMap);

    long replicatedSize = QuotaUtil.getReplicatedSize(data.length,
        bucket.getReplicationConfig());
    Assertions.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);

    //upload same key again
    multipartInfo = bucket.initiateMultipartUpload(keyName);
    uploadID = multipartInfo.getUploadID();

    partName = uploadPart(bucket, keyName, uploadID, partNumber,
        data);

    partsMap = new HashMap<>();
    partsMap.put(partNumber, partName);
    bucket.completeMultipartUpload(keyName, uploadID, partsMap);

    // used sized should remain same, overwrite previous upload
    Assertions.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);
  }

  @Test
  public void testUploadAbortWithEC() throws IOException {
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
    Assertions.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);

    bucket.abortMultipartUpload(keyName, uploadID);

    // used size should become zero after aport upload
    Assertions.assertEquals(volume.getBucket(bucketName).getUsedBytes(), 0);
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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);

    // Upload Parts
    Map<Integer, String> partsMap = new TreeMap<>();
    // Uploading part 1 with less than min size
    String partName = uploadPart(bucket, keyName, uploadID, 1,
        "data".getBytes(UTF_8));
    partsMap.put(1, partName);

    partName = uploadPart(bucket, keyName, uploadID, 2,
        "data".getBytes(UTF_8));
    partsMap.put(2, partName);

    // Complete multipart upload
    OzoneTestUtils.expectOmException(OMException.ResultCodes.ENTITY_TOO_SMALL,
        () -> completeMultipartUpload(bucket, keyName, uploadID, partsMap));
  }

  @Test
  public void testMultipartUploadWithDiscardedUnusedPartSize()
      throws Exception {
    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS, ONE);
    byte[] data = generateData(10000000, (byte) 97);

    // Upload Parts
    Map<Integer, String> partsMap = new TreeMap<>();

    // Upload part 1 and add it to the partsMap for completing the upload.
    String partName1 = uploadPart(bucket, keyName, uploadID, 1, data);
    partsMap.put(1, partName1);

    // Upload part 2 and add it to the partsMap for completing the upload.
    String partName2 = uploadPart(bucket, keyName, uploadID, 2, data);
    partsMap.put(2, partName2);

    // Upload part 3 but do not add it to the partsMap.
    uploadPart(bucket, keyName, uploadID, 3, data);

    completeMultipartUpload(bucket, keyName, uploadID, partsMap);

    // Check the bucket size. Since part number 3 was not added to the partsMap,
    // the unused part size should be discarded from the bucket size,
    // 30000000 - 10000000 = 20000000
    long bucketSize = volume.getBucket(bucketName).getUsedBytes();
    Assertions.assertEquals(bucketSize, data.length * 2);
  }

  @Test
  public void testMultipartUploadWithPartsMisMatchWithListSizeDifferent()
      throws Exception {
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
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
    String uploadID = initiateMultipartUpload(bucket, keyName,
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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS, ONE);

    Assertions.assertEquals(volume.getBucket(bucketName).getUsedNamespace(), 4);

    // upload part 1.
    byte[] data = generateData(5 * 1024 * 1024,
        (byte) RandomUtils.nextLong());
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    // Do not close output stream for part 2.
    ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 2, uploadID);
    ozoneOutputStream.write(data, 0, data.length);

    Map<Integer, String> partsMap = new LinkedHashMap<>();
    partsMap.put(1, omMultipartCommitUploadPartInfo.getPartName());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
        bucket.completeMultipartUpload(keyName,
            uploadID, partsMap);
    Assertions.assertNotNull(omMultipartUploadCompleteInfo);

    Assertions.assertNotNull(omMultipartCommitUploadPartInfo);

    byte[] fileContent = new byte[data.length];
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {
      inputStream.read(fileContent);
    }
    StringBuilder sb = new StringBuilder(data.length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    sb.append(part1);
    Assertions.assertEquals(sb.toString(), new String(fileContent, UTF_8));

    try {
      ozoneOutputStream.close();
      Assertions.fail("testCommitPartAfterCompleteUpload failed");
    } catch (IOException ex) {
      Assertions.assertTrue(ex instanceof OMException);
      Assertions.assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR,
          ((OMException) ex).getResult());
    }
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

    String uploadID = initiateMultipartUpload(bucket, keyName,
        RATIS, ONE);

    // Do not close output stream.
    byte[] data = "data".getBytes(UTF_8);
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, uploadID);
    ozoneOutputStream.write(data, 0, data.length);

    // Abort before completing part upload.
    bucket.abortMultipartUpload(keyName, uploadID);

    try {
      ozoneOutputStream.close();
      fail("testAbortUploadFailWithInProgressPartUpload failed");
    } catch (IOException ex) {
      assertTrue(ex instanceof OMException);
      assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR,
          ((OMException) ex).getResult());
    }
  }

  @Test
  public void testAbortUploadSuccessWithOutAnyParts() throws Exception {
    String parentDir = "a/b/c/d/";
    keyName = parentDir + UUID.randomUUID();

    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
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

    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    String partName = uploadPart(bucket, keyName, uploadID, 1,
        "data".getBytes(UTF_8));

    OMMetadataManager metadataMgr =
        cluster.getOzoneManager().getMetadataManager();
    String multipartKey = verifyUploadedPart(uploadID, partName, metadataMgr);

    bucket.abortMultipartUpload(keyName, uploadID);

    String multipartOpenKey =
        getMultipartOpenKey(uploadID, volumeName, bucketName, keyName,
            metadataMgr);
    OmKeyInfo omKeyInfo =
        metadataMgr.getOpenKeyTable(bucketLayout).get(multipartOpenKey);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);
    Assertions.assertNull(omKeyInfo);
    Assertions.assertNull(omMultipartKeyInfo);

    // Since deleteTable operation is performed via
    // batchOp - Table.putWithBatch(), which is an async operation and
    // not making any assertion for the same.
  }

  @Test
  public void testListMultipartUploadParts() throws Exception {
    String parentDir = "a/b/c/d/e/f/";
    keyName = parentDir + "file-ABC";

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 = uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 = uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 3);

    Assertions.assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    Assertions.assertEquals(3,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    verifyPartNamesInDB(partsMap,
        ozoneMultipartUploadPartListParts, uploadID);

    Assertions.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
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
    Assertions.assertNotNull(omMultipartKeyInfo);

    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo :
        omMultipartKeyInfo.getPartKeyInfoMap()) {
      String partKeyName = partKeyInfo.getPartName();

      // reconstruct full part name with volume, bucket, partKeyName
      String fullKeyPartName =
          metadataMgr.getOzoneKey(volumeName, bucketName, keyName);

      // partKeyName format in DB - partKeyName + ClientID
      Assertions.assertTrue(partKeyName.startsWith(fullKeyPartName),
          "Invalid partKeyName format in DB: " + partKeyName
              + ", expected name:" + fullKeyPartName);

      listPartNames.remove(partKeyName);
    }

    Assertions.assertTrue(listPartNames.isEmpty(),
        "Wrong partKeyName format in DB!");
  }

  private String verifyPartNames(Map<Integer, String> partsMap, int index,
                                 OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts) {

    Assertions.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 = uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 = uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 2);

    Assertions.assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    Assertions.assertEquals(2,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assertions.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    Assertions.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());

    // Get remaining
    Assertions.assertTrue(ozoneMultipartUploadPartListParts.isTruncated());
    ozoneMultipartUploadPartListParts = bucket.listParts(keyName, uploadID,
        ozoneMultipartUploadPartListParts.getNextPartNumberMarker(), 2);

    Assertions.assertEquals(1,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assertions.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());


    // As we don't have any parts for this, we should get false here
    Assertions.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));


    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 100, 2);

    // Should return empty

    Assertions.assertEquals(0,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assertions.assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    Assertions.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

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
    String uploadID1 = initiateMultipartUpload(bucket, key1, RATIS,
        ONE);
    String uploadID2 = initiateMultipartUpload(bucket, key2, RATIS,
        ONE);
    String uploadID3 = initiateMultipartUpload(bucket, key3, RATIS,
        ONE);

    // Upload Parts
    // Uploading part 1 with less than min size
    uploadPart(bucket, key1, uploadID1, 1, "data".getBytes(UTF_8));
    uploadPart(bucket, key2, uploadID2, 1, "data".getBytes(UTF_8));
    uploadPart(bucket, key3, uploadID3, 1, "data".getBytes(UTF_8));

    OzoneMultipartUploadList listMPUs = bucket.listMultipartUploads("dir1");
    Assertions.assertEquals(3, listMPUs.getUploads().size());
    List<String> expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assertions.assertEquals(0, expectedList.size());

    listMPUs = bucket.listMultipartUploads("dir1/dir2");
    Assertions.assertEquals(2, listMPUs.getUploads().size());
    expectedList = new ArrayList<>();
    expectedList.add(key2);
    expectedList.add(key3);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assertions.assertEquals(0, expectedList.size());

    listMPUs = bucket.listMultipartUploads("dir1/dir2/dir3");
    Assertions.assertEquals(1, listMPUs.getUploads().size());
    expectedList = new ArrayList<>();
    expectedList.add(key3);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assertions.assertEquals(0, expectedList.size());

    // partial key
    listMPUs = bucket.listMultipartUploads("d");
    Assertions.assertEquals(3, listMPUs.getUploads().size());
    expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assertions.assertEquals(0, expectedList.size());

    // partial key
    listMPUs = bucket.listMultipartUploads("");
    Assertions.assertEquals(3, listMPUs.getUploads().size());
    expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assertions.assertEquals(0, expectedList.size());
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
        getMultipartOpenKey(uploadID, volumeName, bucketName, keyName,
            metadataMgr);

    String multipartKey = metadataMgr.getMultipartKey(volumeName, bucketName,
        keyName, uploadID);
    OmKeyInfo omKeyInfo =
        metadataMgr.getOpenKeyTable(bucketLayout).get(multipartOpenKey);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);

    Assertions.assertNotNull(omKeyInfo);
    Assertions.assertNotNull(omMultipartKeyInfo);
    Assertions.assertEquals(OzoneFSUtils.getFileName(keyName),
        omKeyInfo.getKeyName());
    Assertions.assertEquals(uploadID, omMultipartKeyInfo.getUploadID());

    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo :
        omMultipartKeyInfo.getPartKeyInfoMap()) {
      OmKeyInfo currentKeyPartInfo =
          OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

      Assertions.assertEquals(keyName, currentKeyPartInfo.getKeyName());

      // verify dbPartName
      Assertions.assertEquals(partName, partKeyInfo.getPartName());
    }
    return multipartKey;
  }

  private String getMultipartOpenKey(String multipartUploadID,
                                     String volName, String buckName, String kName,
                                     OMMetadataManager omMetadataManager) throws IOException {

    String fileName = OzoneFSUtils.getFileName(kName);
    final long volumeId = omMetadataManager.getVolumeId(volName);
    final long bucketId = omMetadataManager.getBucketId(volName,
        buckName);
    long parentID = getParentID(volName, buckName, kName,
        omMetadataManager);

    String multipartKey = omMetadataManager.getMultipartKey(volumeId, bucketId,
        parentID, fileName, multipartUploadID);

    return multipartKey;
  }

  private long getParentID(String volName, String buckName,
                           String kName, OMMetadataManager omMetadataManager) throws IOException {
    final long volumeId = omMetadataManager.getVolumeId(volName);
    final long bucketId = omMetadataManager.getBucketId(volName,
        buckName);
    return OMFileRequest.getParentID(volumeId, bucketId,
        kName, omMetadataManager);
  }

  private String initiateMultipartUpload(OzoneBucket oBucket, String kName,
                                         ReplicationType replicationType, ReplicationFactor replicationFactor)
      throws IOException {
    OmMultipartInfo multipartInfo = oBucket.initiateMultipartUpload(kName,
        replicationType, replicationFactor);

    Assertions.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assertions.assertNotNull(multipartInfo.getUploadID());

    return uploadID;
  }

  private String uploadPart(OzoneBucket oBucket, String kName, String
      uploadID, int partNumber, byte[] data) throws IOException {

    OzoneOutputStream ozoneOutputStream = oBucket.createMultipartKey(kName,
        data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0,
        data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
        ozoneOutputStream.getCommitUploadPartInfo();

    Assertions.assertNotNull(omMultipartCommitUploadPartInfo);
    Assertions.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());

    return omMultipartCommitUploadPartInfo.getPartName();
  }

  private void completeMultipartUpload(OzoneBucket oBucket, String kName,
                                       String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = oBucket
        .completeMultipartUpload(kName, uploadID, partsMap);

    Assertions.assertNotNull(omMultipartUploadCompleteInfo);
    Assertions.assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }
}
