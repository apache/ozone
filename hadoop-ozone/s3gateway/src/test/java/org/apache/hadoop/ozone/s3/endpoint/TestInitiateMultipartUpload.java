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

package org.apache.hadoop.ozone.s3.endpoint;

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.UNSIGNED_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import jakarta.annotation.Nonnull;
import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class tests Initiate Multipart Upload request.
 */
public class TestInitiateMultipartUpload {

  @Test
  public void testInitiateMultipartUpload() throws Exception {

    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    ObjectEndpoint rest = getObjectEndpoint(client, headers);

    String uploadID = initiateMultipartUpload(rest, bucket, key);

    // Calling again should return different uploadID.
    String nextID = initiateMultipartUpload(rest, bucket, key);
    assertNotEquals(uploadID, nextID);
  }

  @Test
  public void testInitiateMultipartUploadWithECKey() throws Exception {
    String bucket = OzoneConsts.S3_BUCKET;
    String key = OzoneConsts.KEY;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);
    HttpHeaders headers = mock(HttpHeaders.class);
    ObjectEndpoint rest = getObjectEndpoint(client, headers);
    client.getObjectStore().getS3Bucket(bucket)
        .setReplicationConfig(new ECReplicationConfig("rs-3-2-1024K"));
    initiateMultipartUpload(rest, bucket, key);
  }

  @Test
  public void testInitiateMultipartUploadRejectsFileAncestor() throws Exception {
    String bucket = OzoneConsts.S3_BUCKET;
    String key = "file/child";
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);
    ObjectEndpoint rest = spy(getObjectEndpoint(client, mock(HttpHeaders.class)));
    ClientProtocol protocol = spy(rest.getClientProtocol());
    OzoneBucket ozoneBucket = spy(protocol.getBucketDetails(null, bucket));
    doReturn(protocol).when(rest).getClientProtocol();
    doReturn(ozoneBucket).when(protocol).getBucketDetails(null, bucket);
    OMException failure = new OMException("File exists in path", OMException.ResultCodes.NOT_A_FILE);
    doThrow(failure).when(ozoneBucket).initiateMultipartUpload(eq(key), any(), anyMap(), anyMap());

    OS3Exception exception = assertErrorResponse(S3ErrorTable.INVALID_REQUEST,
        () -> rest.initializeMultipartUpload(bucket, key));
    assertSame(failure, exception.getCause());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testUploadPartRejectsFileAncestor(boolean wrapped) throws Exception {
    String bucket = OzoneConsts.S3_BUCKET;
    String key = "file/child";
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);
    ObjectEndpoint rest = spy(getObjectEndpoint(client, mock(HttpHeaders.class)));
    when(rest.getHeaders().getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn(UNSIGNED_PAYLOAD);
    doReturn(false).when(rest).isDatastreamEnabled();
    ClientProtocol protocol = spy(rest.getClientProtocol());
    doReturn(protocol).when(rest).getClientProtocol();
    rest.init();
    OMException failure = new OMException("File exists in path", OMException.ResultCodes.NOT_A_FILE);
    doThrow(wrapped ? new IOException(failure) : failure).when(protocol)
        .createMultipartKey(any(), eq(bucket), eq(key), anyLong(),
            anyInt(), any(), anyBoolean());

    assertErrorResponse(S3ErrorTable.INVALID_REQUEST,
        () -> EndpointTestUtils.put(rest, bucket, key, 1, "upload-id", "content"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"dir1", "dir1/"})
  public void testInitiateMultipartUploadRejectsExistingDirectory(String key) throws Exception {
    String bucket = OzoneConsts.S3_BUCKET;
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucket);
    ObjectEndpoint rest = spy(getObjectEndpoint(client, mock(HttpHeaders.class)));
    ClientProtocol protocol = spy(rest.getClientProtocol());
    OzoneBucket ozoneBucket = spy(protocol.getBucketDetails(null, bucket));
    doReturn(protocol).when(rest).getClientProtocol();
    doReturn(ozoneBucket).when(protocol).getBucketDetails(null, bucket);
    OMException failure = new OMException("Can not write to directory: " + key, OMException.ResultCodes.NOT_A_FILE);
    doThrow(failure).when(ozoneBucket)
        .initiateMultipartUpload(eq(key), any(ReplicationConfig.class), anyMap(), anyMap());

    long failureBefore = rest.getMetrics().getInitMultiPartUploadFailure();
    OS3Exception exception = assertErrorResponse(S3ErrorTable.INVALID_REQUEST,
        () -> rest.initializeMultipartUpload(bucket, key));

    assertEquals("An error occurred (InvalidRequest) " +
        "when calling the PutObject/MPU PartUpload operation: " +
        OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are" +
        " considered as Unix Paths. Path has Violated FS Semantics " +
        "which caused put operation to fail.", exception.getErrorMessage());
    assertEquals(400, exception.getHttpCode());
    assertSame(failure, exception.getCause());
    assertEquals(1, rest.getMetrics().getInitMultiPartUploadFailure() - failureBefore);
  }

  @Nonnull
  private ObjectEndpoint getObjectEndpoint(OzoneClient client,
      HttpHeaders headers) {
    ObjectEndpoint rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
    return rest;
  }
}
