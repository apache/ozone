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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.ozone.om.request.s3.multipart;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos
    .OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.ozone.om.ratis.utils.OzoneManagerDoubleBufferHelper;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Base test class for S3 Multipart upload request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestS3MultipartRequest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;

  // Just setting ozoneManagerDoubleBuffer which does nothing.
  protected OzoneManagerDoubleBufferHelper ozoneManagerDoubleBufferHelper =
      ((response, transactionIndex) -> {
        return null;
      });


  @Before
  public void setup() throws Exception {
    ozoneManager = Mockito.mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.newFolder().getAbsolutePath());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    auditLogger = Mockito.mock(AuditLogger.class);
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    Mockito.doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenAnswer(inv -> {
          KeyArgs args = (KeyArgs) inv.getArguments()[0];
          return new ResolvedBucket(
              Pair.of(args.getVolumeName(), args.getBucketName()),
              Pair.of(args.getVolumeName(), args.getBucketName()));
        });
  }


  @After
  public void stop() {
    omMetrics.unRegister();
    Mockito.framework().clearInlineMocks();
  }

  /**
   * Perform preExecute of Initiate Multipart upload request for given
   * volume, bucket and key name.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteInitiateMPU(
      String volumeName, String bucketName, String keyName) throws Exception {
    OMRequest omRequest =
        OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName,
            keyName);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(omRequest);

    OMRequest modifiedRequest =
        s3InitiateMultipartUploadRequest.preExecute(ozoneManager);

    Assert.assertNotEquals(omRequest, modifiedRequest);
    Assert.assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    Assert.assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getMultipartUploadID());
    Assert.assertTrue(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime() > 0);

    return modifiedRequest;
  }

  /**
   * Perform preExecute of Commit Multipart Upload request for given volume,
   * bucket and keyName.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param clientID
   * @param multipartUploadID
   * @param partNumber
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteCommitMPU(
      String volumeName, String bucketName, String keyName,
      long clientID, String multipartUploadID, int partNumber)
      throws Exception {

    // Just set dummy size
    long dataSize = 100L;
    OMRequest omRequest =
        OMRequestTestUtils.createCommitPartMPURequest(volumeName, bucketName,
            keyName, clientID, dataSize, multipartUploadID, partNumber);
    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
            getS3MultipartUploadCommitReq(omRequest);

    OMRequest modifiedRequest =
        s3MultipartUploadCommitPartRequest.preExecute(ozoneManager);

    // UserInfo and modification time is set.
    Assert.assertNotEquals(omRequest, modifiedRequest);

    return modifiedRequest;
  }

  /**
   * Perform preExecute of Abort Multipart Upload request for given volume,
   * bucket and keyName.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param multipartUploadID
   * @return OMRequest - returned from preExecute.
   * @throws IOException
   */
  protected OMRequest doPreExecuteAbortMPU(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID) throws IOException {

    OMRequest omRequest =
        OMRequestTestUtils.createAbortMPURequest(volumeName, bucketName,
            keyName, multipartUploadID);


    S3MultipartUploadAbortRequest s3MultipartUploadAbortRequest =
        getS3MultipartUploadAbortReq(omRequest);

    OMRequest modifiedRequest =
        s3MultipartUploadAbortRequest.preExecute(ozoneManager);

    // UserInfo and modification time is set.
    Assert.assertNotEquals(omRequest, modifiedRequest);

    return modifiedRequest;

  }

  protected OMRequest doPreExecuteCompleteMPU(String volumeName,
      String bucketName, String keyName, String multipartUploadID,
      List<Part> partList) throws IOException {

    OMRequest omRequest =
        OMRequestTestUtils.createCompleteMPURequest(volumeName, bucketName,
            keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
            getS3MultipartUploadCompleteReq(omRequest);

    OMRequest modifiedRequest =
        s3MultipartUploadCompleteRequest.preExecute(ozoneManager);

    // UserInfo and modification time is set.
    Assert.assertNotEquals(omRequest, modifiedRequest);

    return modifiedRequest;

  }


  /**
   * Perform preExecute of Initiate Multipart upload request for given
   * volume, bucket and key name.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteInitiateMPUWithFSO(
      String volumeName, String bucketName, String keyName) throws Exception {
    OMRequest omRequest =
            OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName,
                    keyName);

    S3InitiateMultipartUploadRequestWithFSO
        s3InitiateMultipartUploadRequestWithFSO =
        new S3InitiateMultipartUploadRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedRequest =
            s3InitiateMultipartUploadRequestWithFSO.preExecute(ozoneManager);

    Assert.assertNotEquals(omRequest, modifiedRequest);
    Assert.assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    Assert.assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getMultipartUploadID());
    Assert.assertTrue(modifiedRequest.getInitiateMultiPartUploadRequest()
            .getKeyArgs().getModificationTime() > 0);

    return modifiedRequest;
  }

  protected S3MultipartUploadCompleteRequest getS3MultipartUploadCompleteReq(
          OMRequest omRequest) throws OMException {
    return new S3MultipartUploadCompleteRequest(omRequest,
        BucketLayout.DEFAULT);
  }

  protected S3MultipartUploadCommitPartRequest getS3MultipartUploadCommitReq(
          OMRequest omRequest) throws OMException {
    return new S3MultipartUploadCommitPartRequest(omRequest,
        BucketLayout.DEFAULT);
  }

  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) throws OMException {
    return new S3InitiateMultipartUploadRequest(initiateMPURequest,
        BucketLayout.DEFAULT);
  }

  protected S3MultipartUploadAbortRequest getS3MultipartUploadAbortReq(
      OMRequest omRequest) throws OMException {
    return new S3MultipartUploadAbortRequest(omRequest, BucketLayout.DEFAULT);
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

}
