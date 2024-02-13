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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import javax.xml.bind.DatatypeConverter;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
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
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.apache.ozone.test.JUnit5AwareTimeout;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.hadoop.hdds.client.ReplicationFactor.ONE;
import static org.apache.hadoop.hdds.client.ReplicationType.RATIS;
import static org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes.NO_SUCH_MULTIPART_UPLOAD_ERROR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * This test verifies all the S3 multipart client apis - prefix layout.
 */
public class TestOzoneClientMultipartUploadWithFSO {

  private static ObjectStore store = null;
  private static MiniOzoneCluster cluster = null;
  private static OzoneClient ozClient = null;
  private static MessageDigest eTagProvider;

  private static String scmId = UUID.randomUUID().toString();

  /**
   * Set a timeout for each test.
   */
  @Rule
  public TestRule timeout = new JUnit5AwareTimeout(new Timeout(300000));
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
  @BeforeClass
  public static void init() throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    OMRequestTestUtils.configureFSOptimizedPaths(conf, true);
    startCluster(conf);
    eTagProvider = MessageDigest.getInstance(OzoneConsts.MD5_HASH);
  }

  /**
   * Close OzoneClient and shutdown MiniOzoneCluster.
   */
  @AfterClass
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

  @Before
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
    Assert.assertNotEquals(uploadIDNew, uploadID);
  }

  @Test
  public void testInitiateMultipartUploadWithDefaultReplication() throws
          IOException {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName);

    Assert.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    Assert.assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName);

    Assert.assertNotNull(multipartInfo);
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    Assert.assertNotEquals(multipartInfo.getUploadID(), uploadID);
    Assert.assertNotNull(multipartInfo.getUploadID());
  }

  @Test
  public void testUploadPartWithNoOverride() throws IOException {
    String sampleData = "sample Value";
    String uploadID = initiateMultipartUpload(bucket, keyName,
        ReplicationType.RATIS, ONE);

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
            sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.getMetadata().put(OzoneConsts.ETAG, DigestUtils.md5Hex(sampleData));
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
            .getCommitUploadPartInfo();

    Assert.assertNotNull(commitUploadPartInfo);
    Assert.assertNotNull(commitUploadPartInfo.getETag());
  }

  @Test
  public void testUploadPartOverrideWithRatis() throws Exception {
    String sampleData = "sample Value";
    String uploadID = initiateMultipartUpload(bucket, keyName,
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
    Assert.assertEquals(partNameAndETag.getKey(), partNameAndETagNew.getKey());

    // ETags are not equal due to content differences
    Assert.assertNotEquals(partNameAndETag.getValue(), partNameAndETagNew.getValue());

    // old part bytes written needs discard and have only
    // new part bytes in quota for this bucket
    long byteWritten = "name".length() * 3; // data written with replication
    Assert.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
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
    Assert.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
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
    Assert.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
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
    Assert.assertEquals(volume.getBucket(bucketName).getUsedBytes(),
        replicatedSize);

    bucket.abortMultipartUpload(keyName, uploadID);

    // used size should become zero after aport upload
    Assert.assertEquals(volume.getBucket(bucketName).getUsedBytes(), 0);
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
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS, ONE);
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
    Assert.assertEquals(bucketSize, data.length * 2);
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
    keyName = parentDir + UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS, ONE);

    Assert.assertEquals(volume.getBucket(bucketName).getUsedNamespace(), 4);

    // upload part 1.
    byte[] data = generateData(5 * 1024 * 1024,
            (byte) RandomUtils.nextLong());
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
    Assert.assertNotNull(omMultipartUploadCompleteInfo);

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);

    byte[] fileContent = new byte[data.length];
    try (OzoneInputStream inputStream = bucket.readKey(keyName)) {
      inputStream.read(fileContent);
    }
    StringBuilder sb = new StringBuilder(data.length);

    // Combine all parts data, and check is it matching with get key data.
    String part1 = new String(data, UTF_8);
    sb.append(part1);
    Assert.assertEquals(sb.toString(), new String(fileContent, UTF_8));

    try {
      ozoneOutputStream.close();
      Assert.fail("testCommitPartAfterCompleteUpload failed");
    } catch (IOException ex) {
      Assert.assertTrue(ex instanceof OMException);
      Assert.assertEquals(NO_SUCH_MULTIPART_UPLOAD_ERROR,
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
    keyName = parentDir + UUID.randomUUID().toString();

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
    keyName = parentDir + UUID.randomUUID().toString();

    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testAbortUploadSuccessWithParts() throws Exception {
    String parentDir = "a/b/c/d/";
    keyName = parentDir + UUID.randomUUID().toString();

    OzoneManager ozoneManager = cluster.getOzoneManager();
    String buckKey = ozoneManager.getMetadataManager()
        .getBucketKey(volume.getName(), bucket.getName());
    OmBucketInfo buckInfo =
        ozoneManager.getMetadataManager().getBucketTable().get(buckKey);
    BucketLayout bucketLayout = buckInfo.getBucketLayout();

    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    Pair<String, String> partNameAndETag = uploadPart(bucket, keyName, uploadID,
        1, "data".getBytes(UTF_8));

    OMMetadataManager metadataMgr =
        cluster.getOzoneManager().getMetadataManager();
    String multipartKey = verifyUploadedPart(uploadID, partNameAndETag.getKey(),
        metadataMgr);

    bucket.abortMultipartUpload(keyName, uploadID);

    String multipartOpenKey =
        getMultipartOpenKey(uploadID, volumeName, bucketName, keyName,
            metadataMgr);
    OmKeyInfo omKeyInfo =
        metadataMgr.getOpenKeyTable(bucketLayout).get(multipartOpenKey);
    OmMultipartKeyInfo omMultipartKeyInfo =
        metadataMgr.getMultipartInfoTable().get(multipartKey);
    Assert.assertNull(omKeyInfo);
    Assert.assertNull(omMultipartKeyInfo);

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

    Assert.assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    Assert.assertEquals(3,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    verifyPartNamesInDB(partsMap,
        ozoneMultipartUploadPartListParts, uploadID);

    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
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
    Assert.assertNotNull(omMultipartKeyInfo);

    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo :
        omMultipartKeyInfo.getPartKeyInfoMap()) {
      String partKeyName = partKeyInfo.getPartName();

      // reconstruct full part name with volume, bucket, partKeyName
      String fullKeyPartName =
          metadataMgr.getOzoneKey(volumeName, bucketName, keyName);

      // partKeyName format in DB - partKeyName + ClientID
      Assert.assertTrue("Invalid partKeyName format in DB: " + partKeyName
              + ", expected name:" + fullKeyPartName,
          partKeyName.startsWith(fullKeyPartName));

      listPartNames.remove(partKeyName);
    }
    Assert.assertTrue("Wrong partKeyName format in DB!",
        listPartNames.isEmpty());
  }

  private String verifyPartNames(Map<Integer, String> partsMap, int index,
      OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts) {

    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
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

    Assert.assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    Assert.assertEquals(2,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(1).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(1)
            .getPartName());

    // Get remaining
    Assert.assertTrue(ozoneMultipartUploadPartListParts.isTruncated());
    ozoneMultipartUploadPartListParts = bucket.listParts(keyName, uploadID,
        ozoneMultipartUploadPartListParts.getNextPartNumberMarker(), 2);

    Assert.assertEquals(1,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assert.assertEquals(partsMap.get(ozoneMultipartUploadPartListParts
            .getPartInfoList().get(0).getPartNumber()),
        ozoneMultipartUploadPartListParts.getPartInfoList().get(0)
            .getPartName());


    // As we don't have any parts for this, we should get false here
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsInvalidPartMarker() throws Exception {
    try {
      bucket.listParts(keyName, "random", -1, 2);
      Assert.fail("Should throw exception as partNumber is an invalid number!");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Should be greater than or "
          + "equal to zero", ex);
    }
  }

  @Test
  public void testListPartsInvalidMaxParts() throws Exception {
    try {
      bucket.listParts(keyName, "random", 1, -1);
      Assert.fail("Should throw exception as max parts is an invalid number!");
    } catch (IllegalArgumentException ex) {
      GenericTestUtils.assertExceptionContains("Max Parts Should be greater "
          + "than zero", ex);
    }
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
    Assert.assertEquals(0,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    Assert.assertEquals(
        RatisReplicationConfig.getInstance(HddsProtos.ReplicationFactor.ONE),
        ozoneMultipartUploadPartListParts.getReplicationConfig());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

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
    Assert.assertEquals(3, listMPUs.getUploads().size());
    List<String> expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assert.assertEquals(0, expectedList.size());

    listMPUs = bucket.listMultipartUploads("dir1/dir2");
    Assert.assertEquals(2, listMPUs.getUploads().size());
    expectedList = new ArrayList<>();
    expectedList.add(key2);
    expectedList.add(key3);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assert.assertEquals(0, expectedList.size());

    listMPUs = bucket.listMultipartUploads("dir1/dir2/dir3");
    Assert.assertEquals(1, listMPUs.getUploads().size());
    expectedList = new ArrayList<>();
    expectedList.add(key3);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assert.assertEquals(0, expectedList.size());

    // partial key
    listMPUs = bucket.listMultipartUploads("d");
    Assert.assertEquals(3, listMPUs.getUploads().size());
    expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assert.assertEquals(0, expectedList.size());

    // partial key
    listMPUs = bucket.listMultipartUploads("");
    Assert.assertEquals(3, listMPUs.getUploads().size());
    expectedList = new ArrayList<>(keys);
    for (OzoneMultipartUpload mpu : listMPUs.getUploads()) {
      expectedList.remove(mpu.getKeyName());
    }
    Assert.assertEquals(0, expectedList.size());
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

    Assert.assertNotNull(omKeyInfo);
    Assert.assertNotNull(omMultipartKeyInfo);
    Assert.assertEquals(OzoneFSUtils.getFileName(keyName),
        omKeyInfo.getKeyName());
    Assert.assertEquals(uploadID, omMultipartKeyInfo.getUploadID());

    for (OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo :
        omMultipartKeyInfo.getPartKeyInfoMap()) {
      OmKeyInfo currentKeyPartInfo =
          OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

      Assert.assertEquals(keyName, currentKeyPartInfo.getKeyName());

      // verify dbPartName
      Assert.assertEquals(partName, partKeyInfo.getPartName());
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
    Iterator<Path> pathComponents = Paths.get(kName).iterator();
    final long volumeId = omMetadataManager.getVolumeId(volName);
    final long bucketId = omMetadataManager.getBucketId(volName,
        buckName);
    return OMFileRequest.getParentID(volumeId, bucketId, pathComponents,
        kName, omMetadataManager);
  }

  private String initiateMultipartUpload(OzoneBucket oBucket, String kName,
      ReplicationType replicationType, ReplicationFactor replicationFactor)
          throws IOException {
    OmMultipartInfo multipartInfo = oBucket.initiateMultipartUpload(kName,
            replicationType, replicationFactor);

    Assert.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(kName, multipartInfo.getKeyName());
    Assert.assertNotNull(multipartInfo.getUploadID());

    return uploadID;
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

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);
    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getETag());

    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());

    return Pair.of(omMultipartCommitUploadPartInfo.getPartName(),
        omMultipartCommitUploadPartInfo.getETag());
  }

  private void completeMultipartUpload(OzoneBucket oBucket, String kName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = oBucket
            .completeMultipartUpload(kName, uploadID, partsMap);

    Assert.assertNotNull(omMultipartUploadCompleteInfo);
    Assert.assertEquals(omMultipartUploadCompleteInfo.getBucket(), oBucket
        .getName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getVolume(), oBucket
        .getVolumeName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getKey(), kName);
    Assert.assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }
}
