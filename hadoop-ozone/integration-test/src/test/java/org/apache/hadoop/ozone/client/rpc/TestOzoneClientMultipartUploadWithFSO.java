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

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.MiniOzoneCluster;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.OzoneTestUtils;
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

import org.apache.hadoop.ozone.om.request.TestOMRequestUtils;
import org.apache.hadoop.ozone.om.request.file.OMFileRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.ozone.test.GenericTestUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

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

  private static String scmId = UUID.randomUUID().toString();

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);

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
    TestOMRequestUtils.configureFSOptimizedPaths(conf, true);
    startCluster(conf);
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
            .setNumDatanodes(3)
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
    if(ozClient != null) {
      ozClient.close();
    }

    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testInitiateMultipartUploadWithReplicationInformationSet() throws
          IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        RATIS, ONE);

    Assert.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    Assert.assertNotNull(multipartInfo.getUploadID());

    // Call initiate multipart upload for the same key again, this should
    // generate a new uploadID.
    multipartInfo = bucket.initiateMultipartUpload(keyName,
        RATIS, ONE);

    Assert.assertNotNull(multipartInfo);
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    Assert.assertNotEquals(multipartInfo.getUploadID(), uploadID);
    Assert.assertNotNull(multipartInfo.getUploadID());
  }

  @Test
  public void testInitiateMultipartUploadWithDefaultReplication() throws
          IOException {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
        RATIS, ONE);

    Assert.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    Assert.assertNotNull(multipartInfo.getUploadID());

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
            sampleData.length(), 1, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
            .getCommitUploadPartInfo();

    Assert.assertNotNull(commitUploadPartInfo);
    Assert.assertNotNull(commitUploadPartInfo.getPartName());
  }

  @Test
  public void testUploadPartOverrideWithRatis() throws IOException {

    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();
    String sampleData = "sample Value";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
            ReplicationType.RATIS, THREE);

    Assert.assertNotNull(multipartInfo);
    String uploadID = multipartInfo.getUploadID();
    Assert.assertEquals(volumeName, multipartInfo.getVolumeName());
    Assert.assertEquals(bucketName, multipartInfo.getBucketName());
    Assert.assertEquals(keyName, multipartInfo.getKeyName());
    Assert.assertNotNull(multipartInfo.getUploadID());

    int partNumber = 1;

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
            sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, sampleData.length());
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo commitUploadPartInfo = ozoneOutputStream
            .getCommitUploadPartInfo();

    Assert.assertNotNull(commitUploadPartInfo);
    String partName = commitUploadPartInfo.getPartName();
    Assert.assertNotNull(commitUploadPartInfo.getPartName());

    //Overwrite the part by creating part key with same part number.
    sampleData = "sample Data Changed";
    ozoneOutputStream = bucket.createMultipartKey(keyName,
            sampleData.length(), partNumber, uploadID);
    ozoneOutputStream.write(string2Bytes(sampleData), 0, "name".length());
    ozoneOutputStream.close();

    commitUploadPartInfo = ozoneOutputStream
            .getCommitUploadPartInfo();

    Assert.assertNotNull(commitUploadPartInfo);
    Assert.assertNotNull(commitUploadPartInfo.getPartName());

    // PartName should be same from old part Name.
    // AWS S3 for same content generates same partName during upload part.
    // In AWS S3 ETag is generated from md5sum. In Ozone right now we
    // don't do this. For now to make things work for large file upload
    // through aws s3 cp, the partName are generated in a predictable fashion.
    // So, when a part is override partNames will still be same irrespective
    // of content in ozone s3. This will make S3 Mpu completeMPU pass when
    // comparing part names and large file uploads work using aws cp.
    Assert.assertEquals("Part names should be same", partName,
        commitUploadPartInfo.getPartName());
  }

  @Test
  public void testMultipartUploadWithPartsLessThanMinSize() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

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
  public void testMultipartUploadWithPartsMisMatchWithListSizeDifferent()
          throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

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
  public void testCommitPartAfterCompleteUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String parentDir = "a/b/c/d/";
    String keyName = parentDir + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        RATIS, ONE);

    Assert.assertNotNull(omMultipartInfo.getUploadID());

    String uploadID = omMultipartInfo.getUploadID();

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
            data.length, 2, omMultipartInfo.getUploadID());
    ozoneOutputStream.write(data, 0, data.length);

    Map<Integer, String> partsMap = new LinkedHashMap<>();
    partsMap.put(1, omMultipartCommitUploadPartInfo.getPartName());
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo =
            bucket.completeMultipartUpload(keyName,
                    uploadID, partsMap);
    Assert.assertNotNull(omMultipartUploadCompleteInfo);

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);

    byte[] fileContent = new byte[data.length];
    OzoneInputStream inputStream = bucket.readKey(keyName);
    inputStream.read(fileContent);
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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OzoneTestUtils.expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR,
        () -> bucket.abortMultipartUpload(keyName, "random"));
  }

  @Test
  public void testAbortUploadFailWithInProgressPartUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String parentDir = "a/b/c/d/";
    String keyName = parentDir + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    OmMultipartInfo omMultipartInfo = bucket.initiateMultipartUpload(keyName,
        RATIS, ONE);

    Assert.assertNotNull(omMultipartInfo.getUploadID());

    // Do not close output stream.
    byte[] data = "data".getBytes(UTF_8);
    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
        data.length, 1, omMultipartInfo.getUploadID());
    ozoneOutputStream.write(data, 0, data.length);

    // Abort before completing part upload.
    bucket.abortMultipartUpload(keyName, omMultipartInfo.getUploadID());

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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String parentDir = "a/b/c/d/";
    String keyName = parentDir + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    bucket.abortMultipartUpload(keyName, uploadID);
  }

  @Test
  public void testAbortUploadSuccessWithParts() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String parentDir = "a/b/c/d/";
    String keyName = parentDir + UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);
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
    String multipartKey = verifyUploadedPart(volumeName, bucketName, keyName,
        uploadID, partName, metadataMgr);

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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String parentDir = "a/b/c/d/e/f/";
    String keyName = parentDir + "file-ABC";

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 =uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 =uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 3);

    Assert.assertEquals(RATIS,
        ozoneMultipartUploadPartListParts.getReplicationType());
    Assert.assertEquals(3,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());

    verifyPartNamesInDB(volumeName, bucketName, keyName, partsMap,
        ozoneMultipartUploadPartListParts, uploadID);

    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());
  }

  private void verifyPartNamesInDB(String volumeName, String bucketName,
      String keyName, Map<Integer, String> partsMap,
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

    TreeMap<Integer, OzoneManagerProtocolProtos.PartKeyInfo> partKeyInfoMap =
        omMultipartKeyInfo.getPartKeyInfoMap();
    for (Map.Entry<Integer, OzoneManagerProtocolProtos.PartKeyInfo> entry :
        partKeyInfoMap.entrySet()) {
      OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo = entry.getValue();
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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

    Map<Integer, String> partsMap = new TreeMap<>();
    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    String partName1 = uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(1, partName1);

    String partName2 =uploadPart(bucket, keyName, uploadID, 2,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(2, partName2);

    String partName3 =uploadPart(bucket, keyName, uploadID, 3,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));
    partsMap.put(3, partName3);

    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 0, 2);

    Assert.assertEquals(RATIS,
        ozoneMultipartUploadPartListParts.getReplicationType());

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
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();

      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);

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
      String volumeName = UUID.randomUUID().toString();
      String bucketName = UUID.randomUUID().toString();
      String keyName = UUID.randomUUID().toString();

      store.createVolume(volumeName);
      OzoneVolume volume = store.getVolume(volumeName);
      volume.createBucket(bucketName);
      OzoneBucket bucket = volume.getBucket(bucketName);

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
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String keyName = UUID.randomUUID().toString();

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);


    String uploadID = initiateMultipartUpload(bucket, keyName, RATIS,
        ONE);
    uploadPart(bucket, keyName, uploadID, 1,
        generateData(OzoneConsts.OM_MULTIPART_MIN_SIZE, (byte)97));


    OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
        bucket.listParts(keyName, uploadID, 100, 2);

    // Should return empty

    Assert.assertEquals(0,
        ozoneMultipartUploadPartListParts.getPartInfoList().size());
    Assert.assertEquals(RATIS,
        ozoneMultipartUploadPartListParts.getReplicationType());

    // As we don't have any parts with greater than partNumberMarker and list
    // is not truncated, so it should return false here.
    Assert.assertFalse(ozoneMultipartUploadPartListParts.isTruncated());

  }

  @Test
  public void testListPartsWithInvalidUploadID() throws Exception {
    OzoneTestUtils
        .expectOmException(NO_SUCH_MULTIPART_UPLOAD_ERROR, () -> {
          String volumeName = UUID.randomUUID().toString();
          String bucketName = UUID.randomUUID().toString();
          String keyName = UUID.randomUUID().toString();

          store.createVolume(volumeName);
          OzoneVolume volume = store.getVolume(volumeName);
          volume.createBucket(bucketName);
          OzoneBucket bucket = volume.getBucket(bucketName);
          OzoneMultipartUploadPartListParts ozoneMultipartUploadPartListParts =
              bucket.listParts(keyName, "random", 100, 2);
        });
  }

  @Test
  public void testListMultipartUpload() throws Exception {
    String volumeName = UUID.randomUUID().toString();
    String bucketName = UUID.randomUUID().toString();
    String dirName = "dir1/dir2/dir3";
    String key1 = "dir1" + "/key1";
    String key2 = "dir1/dir2" + "/key2";
    String key3 = dirName + "/key3";
    List<String> keys = new ArrayList<>();
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);

    store.createVolume(volumeName);
    OzoneVolume volume = store.getVolume(volumeName);
    volume.createBucket(bucketName);
    OzoneBucket bucket = volume.getBucket(bucketName);

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

  private String verifyUploadedPart(String volumeName, String bucketName,
      String keyName, String uploadID, String partName,
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

    TreeMap<Integer, OzoneManagerProtocolProtos.PartKeyInfo> partKeyInfoMap =
        omMultipartKeyInfo.getPartKeyInfoMap();
    for (Map.Entry<Integer, OzoneManagerProtocolProtos.PartKeyInfo> entry :
        partKeyInfoMap.entrySet()) {
      OzoneManagerProtocolProtos.PartKeyInfo partKeyInfo = entry.getValue();
      OmKeyInfo currentKeyPartInfo =
          OmKeyInfo.getFromProtobuf(partKeyInfo.getPartKeyInfo());

      Assert.assertEquals(keyName, currentKeyPartInfo.getKeyName());

      // verify dbPartName
      Assert.assertEquals(partName, partKeyInfo.getPartName());
    }
    return multipartKey;
  }

  private String getMultipartOpenKey(String multipartUploadID,
      String volumeName, String bucketName, String keyName,
      OMMetadataManager omMetadataManager) throws IOException {

    String fileName = OzoneFSUtils.getFileName(keyName);
    long parentID = getParentID(volumeName, bucketName, keyName,
        omMetadataManager);

    String multipartKey = omMetadataManager.getMultipartKey(parentID,
        fileName, multipartUploadID);

    return multipartKey;
  }

  private long getParentID(String volumeName, String bucketName,
      String keyName, OMMetadataManager omMetadataManager) throws IOException {
    Iterator<Path> pathComponents = Paths.get(keyName).iterator();
    String bucketKey = omMetadataManager.getBucketKey(volumeName, bucketName);
    OmBucketInfo omBucketInfo =
        omMetadataManager.getBucketTable().get(bucketKey);
    long bucketId = omBucketInfo.getObjectID();
    return OMFileRequest.getParentID(bucketId, pathComponents,
        keyName, omMetadataManager);
  }

  private String initiateMultipartUpload(OzoneBucket bucket, String keyName,
      ReplicationType replicationType, ReplicationFactor replicationFactor)
          throws Exception {
    OmMultipartInfo multipartInfo = bucket.initiateMultipartUpload(keyName,
            replicationType, replicationFactor);

    String uploadID = multipartInfo.getUploadID();
    Assert.assertNotNull(uploadID);

    return uploadID;
  }

  private String uploadPart(OzoneBucket bucket, String keyName, String
      uploadID, int partNumber, byte[] data) throws Exception {

    OzoneOutputStream ozoneOutputStream = bucket.createMultipartKey(keyName,
            data.length, partNumber, uploadID);
    ozoneOutputStream.write(data, 0,
            data.length);
    ozoneOutputStream.close();

    OmMultipartCommitUploadPartInfo omMultipartCommitUploadPartInfo =
            ozoneOutputStream.getCommitUploadPartInfo();

    Assert.assertNotNull(omMultipartCommitUploadPartInfo);
    Assert.assertNotNull(omMultipartCommitUploadPartInfo.getPartName());

    return omMultipartCommitUploadPartInfo.getPartName();
  }

  private void completeMultipartUpload(OzoneBucket bucket, String keyName,
      String uploadID, Map<Integer, String> partsMap) throws Exception {
    OmMultipartUploadCompleteInfo omMultipartUploadCompleteInfo = bucket
            .completeMultipartUpload(keyName, uploadID, partsMap);

    Assert.assertNotNull(omMultipartUploadCompleteInfo);
    Assert.assertEquals(omMultipartUploadCompleteInfo.getBucket(), bucket
            .getName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getVolume(), bucket
            .getVolumeName());
    Assert.assertEquals(omMultipartUploadCompleteInfo.getKey(), keyName);
    Assert.assertNotNull(omMultipartUploadCompleteInfo.getHash());
  }

  private byte[] generateData(int size, byte val) {
    byte[] chars = new byte[size];
    Arrays.fill(chars, val);
    return chars;
  }
}
