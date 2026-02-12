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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.framework;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.audit.AuditLogger;
import org.apache.hadoop.ozone.audit.AuditMessage;
import org.apache.hadoop.ozone.om.IOmMetadataReader;
import org.apache.hadoop.ozone.om.OMConfigKeys;
import org.apache.hadoop.ozone.om.OMMetadataManager;
import org.apache.hadoop.ozone.om.OMMetrics;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.OmMetadataManagerImpl;
import org.apache.hadoop.ozone.om.OmMetadataReader;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.om.ResolvedBucket;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.helpers.KeyValueUtil;
import org.apache.hadoop.ozone.om.request.OMClientRequest;
import org.apache.hadoop.ozone.om.request.OMRequestTestUtils;
import org.apache.hadoop.ozone.om.upgrade.OMLayoutVersionManager;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyArgs;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.KeyLocation;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRequest;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Part;
import org.apache.hadoop.ozone.security.acl.OzoneNativeAuthorizer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ratis.util.function.UncheckedAutoCloseableSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;

/**
 * Base test class for S3 Multipart upload request.
 */
@SuppressWarnings("visibilitymodifier")
public class TestS3MultipartRequest {
  @TempDir
  private Path folder;

  protected OzoneManager ozoneManager;
  protected OMMetrics omMetrics;
  protected OMMetadataManager omMetadataManager;
  protected AuditLogger auditLogger;

  @BeforeEach
  public void setup() throws Exception {
    ozoneManager = mock(OzoneManager.class);
    omMetrics = OMMetrics.create();
    OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
    ozoneConfiguration.set(OMConfigKeys.OZONE_OM_DB_DIRS,
        folder.toAbsolutePath().toString());
    omMetadataManager = new OmMetadataManagerImpl(ozoneConfiguration,
        ozoneManager);
    when(ozoneManager.getMetrics()).thenReturn(omMetrics);
    when(ozoneManager.getMetadataManager()).thenReturn(omMetadataManager);
    auditLogger = mock(AuditLogger.class);
    UncheckedAutoCloseableSupplier<IOmMetadataReader> rcOmMetadataReader =
        mock(UncheckedAutoCloseableSupplier.class);
    when(ozoneManager.getOmMetadataReader()).thenReturn(rcOmMetadataReader);
    // Init OmMetadataReader to let the test pass
    OmMetadataReader omMetadataReader = mock(OmMetadataReader.class);
    when(omMetadataReader.isNativeAuthorizerEnabled()).thenReturn(true);
    when(rcOmMetadataReader.get()).thenReturn(omMetadataReader);
    when(ozoneManager.getAccessAuthorizer())
        .thenReturn(new OzoneNativeAuthorizer());
    when(ozoneManager.getAuditLogger()).thenReturn(auditLogger);
    when(ozoneManager.getDefaultReplicationConfig()).thenReturn(
        ReplicationConfig.getDefault(ozoneConfiguration));
    doNothing().when(auditLogger).logWrite(any(AuditMessage.class));
    when(ozoneManager.resolveBucketLink(any(KeyArgs.class),
        any(OMClientRequest.class)))
        .thenAnswer(inv -> {
          KeyArgs args = (KeyArgs) inv.getArguments()[0];
          return new ResolvedBucket(
              args.getVolumeName(), args.getBucketName(),
              args.getVolumeName(), args.getBucketName(),
              "owner", BucketLayout.DEFAULT);
        });
    OMLayoutVersionManager lvm = mock(OMLayoutVersionManager.class);
    when(lvm.getMetadataLayoutVersion()).thenReturn(0);
    when(ozoneManager.getVersionManager()).thenReturn(lvm);
    when(ozoneManager.getConfiguration()).thenReturn(ozoneConfiguration);
    when(ozoneManager.getConfig()).thenReturn(ozoneConfiguration.getObject(OmConfig.class));
  }

  @AfterEach
  public void stop() {
    omMetrics.unRegister();
    framework().clearInlineMocks();
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
    return doPreExecuteInitiateMPU(volumeName, bucketName, keyName, Collections.emptyMap());
  }

  /**
   * Perform preExecute of Initiate Multipart upload request for given
   * volume, bucket and key name.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param metadata
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteInitiateMPU(
      String volumeName, String bucketName, String keyName,
      Map<String, String> metadata) throws Exception {
    return doPreExecuteInitiateMPU(volumeName, bucketName, keyName, metadata,
        Collections.emptyMap());
  }

  /**
   * Perform preExecute of Initiate Multipart upload request for given
   * volume, bucket and key name.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param metadata
   * @param tags
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteInitiateMPU(
      String volumeName, String bucketName, String keyName,
      Map<String, String> metadata, Map<String, String> tags) throws Exception {
    OMRequest omRequest =
        OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName,
            keyName, metadata, tags);

    S3InitiateMultipartUploadRequest s3InitiateMultipartUploadRequest =
        getS3InitiateMultipartUploadReq(omRequest);

    OMRequest modifiedRequest =
        s3InitiateMultipartUploadRequest.preExecute(ozoneManager);

    assertNotEquals(omRequest, modifiedRequest);
    assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getMultipartUploadID());
    assertThat(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime()).isGreaterThan(0);

    if (metadata != null) {
      Map<String, String> modifiedKeyMetadata = KeyValueUtil.getFromProtobuf(
          modifiedRequest.getInitiateMultiPartUploadRequest()
              .getKeyArgs().getMetadataList());

      assertThat(modifiedKeyMetadata).containsAllEntriesOf(metadata);
    }

    if (tags != null) {
      Map<String, String> modifiedKeyTags = KeyValueUtil.getFromProtobuf(
          modifiedRequest.getInitiateMultiPartUploadRequest()
              .getKeyArgs().getTagsList());

      assertThat(modifiedKeyTags).containsAllEntriesOf(tags);
    }

    return modifiedRequest;
  }

  /**
   * Perform preExecute of Commit Multipart Upload request.
   * @param volumeName volume name.
   * @param bucketName bucket name.
   * @param keyName key name.
   * @param clientID client ID.
   * @param multipartUploadID multipart upload ID.
   * @param partNumber part number.
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteCommitMPU(
      String volumeName, String bucketName, String keyName,
      long clientID, String multipartUploadID, int partNumber)
      throws Exception {
    return doPreExecuteCommitMPU(volumeName, bucketName, keyName, clientID, multipartUploadID,
        partNumber, Collections.emptyList());
  }

  /**
   * Perform preExecute of Commit Multipart Upload request.
   * @param volumeName volume name.
   * @param bucketName bucket name.
   * @param keyName key name.
   * @param clientID client ID.
   * @param multipartUploadID multipart upload ID.
   * @param partNumber part number.
   * @param keyLocations key location info list.
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteCommitMPU(
      String volumeName, String bucketName, String keyName,
      long clientID, String multipartUploadID, int partNumber, List<KeyLocation> keyLocations)
      throws Exception {

    long dataSize = keyLocations.stream().mapToLong(KeyLocation::getLength).sum();
    OMRequest omRequest =
        OMRequestTestUtils.createCommitPartMPURequest(volumeName, bucketName,
            keyName, clientID, dataSize, multipartUploadID, partNumber, keyLocations);
    S3MultipartUploadCommitPartRequest s3MultipartUploadCommitPartRequest =
        getS3MultipartUploadCommitReq(omRequest);

    OMRequest modifiedRequest =
        s3MultipartUploadCommitPartRequest.preExecute(ozoneManager);

    // UserInfo and modification time is set.
    assertNotEquals(omRequest, modifiedRequest);

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
    assertNotEquals(omRequest, modifiedRequest);

    return modifiedRequest;

  }

  protected OMRequest doPreExecuteCompleteMPU(
      String volumeName, String bucketName, String keyName,
      String multipartUploadID, List<Part> partList) throws IOException {

    OMRequest omRequest =
        OMRequestTestUtils.createCompleteMPURequest(volumeName, bucketName,
            keyName, multipartUploadID, partList);

    S3MultipartUploadCompleteRequest s3MultipartUploadCompleteRequest =
        getS3MultipartUploadCompleteReq(omRequest);

    OMRequest modifiedRequest =
        s3MultipartUploadCompleteRequest.preExecute(ozoneManager);

    // UserInfo and modification time is set.
    assertNotEquals(omRequest, modifiedRequest);

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
    return doPreExecuteInitiateMPUWithFSO(volumeName, bucketName, keyName,
        Collections.emptyMap(), Collections.emptyMap());
  }

  /**
   * Perform preExecute of Initiate Multipart upload request for given
   * volume, bucket and key name.
   * @param volumeName
   * @param bucketName
   * @param keyName
   * @param metadata
   * @param tags
   * @return OMRequest - returned from preExecute.
   */
  protected OMRequest doPreExecuteInitiateMPUWithFSO(
      String volumeName, String bucketName, String keyName,
      Map<String, String> metadata, Map<String, String> tags) throws Exception {
    OMRequest omRequest =
        OMRequestTestUtils.createInitiateMPURequest(volumeName, bucketName,
            keyName, metadata, tags);

    S3InitiateMultipartUploadRequestWithFSO
        s3InitiateMultipartUploadRequestWithFSO =
        new S3InitiateMultipartUploadRequestWithFSO(omRequest,
            BucketLayout.FILE_SYSTEM_OPTIMIZED);

    OMRequest modifiedRequest =
        s3InitiateMultipartUploadRequestWithFSO.preExecute(ozoneManager);

    assertNotEquals(omRequest, modifiedRequest);
    assertTrue(modifiedRequest.hasInitiateMultiPartUploadRequest());
    assertNotNull(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getMultipartUploadID());
    assertThat(modifiedRequest.getInitiateMultiPartUploadRequest()
        .getKeyArgs().getModificationTime()).isGreaterThan(0);
    if (metadata != null) {
      Map<String, String> modifiedKeyMetadata = KeyValueUtil.getFromProtobuf(
          modifiedRequest.getInitiateMultiPartUploadRequest()
          .getKeyArgs().getMetadataList());

      assertThat(modifiedKeyMetadata).containsAllEntriesOf(metadata);
    }

    if (tags != null) {
      Map<String, String> modifiedKeyTags = KeyValueUtil.getFromProtobuf(
          modifiedRequest.getInitiateMultiPartUploadRequest()
              .getKeyArgs().getTagsList());

      assertThat(modifiedKeyTags).containsAllEntriesOf(tags);
    }

    return modifiedRequest;
  }

  protected S3MultipartUploadCompleteRequest getS3MultipartUploadCompleteReq(
      OMRequest omRequest) throws IOException {
    S3MultipartUploadCompleteRequest request = new S3MultipartUploadCompleteRequest(omRequest,
        BucketLayout.DEFAULT);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  protected S3MultipartUploadCommitPartRequest getS3MultipartUploadCommitReq(
      OMRequest omRequest) throws IOException {
    S3MultipartUploadCommitPartRequest request = new S3MultipartUploadCommitPartRequest(omRequest,
        BucketLayout.DEFAULT);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  protected S3InitiateMultipartUploadRequest getS3InitiateMultipartUploadReq(
      OMRequest initiateMPURequest) throws IOException {
    S3InitiateMultipartUploadRequest request = new S3InitiateMultipartUploadRequest(initiateMPURequest,
        BucketLayout.DEFAULT);
    request.setUGI(UserGroupInformation.getCurrentUser());
    return request;
  }

  protected S3MultipartUploadAbortRequest getS3MultipartUploadAbortReq(
      OMRequest omRequest) {
    return new S3MultipartUploadAbortRequest(omRequest, BucketLayout.DEFAULT);
  }

  public BucketLayout getBucketLayout() {
    return BucketLayout.DEFAULT;
  }

}
