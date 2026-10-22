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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.util.HashMap;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.audit.AuditLogger.PerformanceStringBuilder;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint.ObjectRequestContext;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * Verifies audit logging action for copy operations even if S3 action authorization strings are overridden internally.
 * For example, S3G.COPY_OBJECT must use S3G.COPY_OBJECT as the audit action, even though internally the S3 actions
 * checked are GetObject and PutObject.
 */
public class TestCopyActionsAudit {

  @Test
  public void testCopyObjectAuditActionRemainsCopyObject() throws Exception {
    final String bucketName = OzoneConsts.S3_BUCKET;
    final String srcKey = "src.txt";
    final String destKey = "dest.txt";

    final OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucketName);
    final OzoneBucket bucket = client.getObjectStore().getS3Bucket(bucketName);

    try (OutputStream out = bucket.createKey(
        srcKey, 3, ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
        new HashMap<>())) {
      out.write("src".getBytes(UTF_8));
    }

    final HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(bucketName + "/" + srcKey);
    when(headers.getHeaderString(HttpHeaders.CONTENT_LENGTH)).thenReturn("0");

    final ObjectEndpoint endpoint = newEndpoint(client, headers);
    final AuditingObjectOperationHandler auditing = spy(new AuditingObjectOperationHandler(endpoint));

    final ObjectRequestContext requestContext = endpoint.new ObjectRequestContext(S3GAction.CREATE_KEY, bucketName);

    auditing.handlePutRequest(requestContext, destKey, new ByteArrayInputStream(new byte[0]));

    verify(auditing).auditWriteSuccess(eq(S3GAction.COPY_OBJECT), any(PerformanceStringBuilder.class));
  }

  @Test
  public void testUploadPartCopyAuditActionRemainsCreateMultipartKeyByCopy() throws Exception {
    final String bucketName = OzoneConsts.S3_BUCKET;
    final String srcKey = "src-part.txt";
    final String destKey = "dest-mpu.txt";

    final OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(bucketName);
    final OzoneBucket bucket = client.getObjectStore().getS3Bucket(bucketName);

    try (OutputStream out = bucket.createKey(
        srcKey, 4, ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS, ReplicationFactor.ONE),
        new HashMap<>())) {
      out.write("part".getBytes(UTF_8));
    }

    final HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(bucketName + "/" + srcKey);
    when(headers.getHeaderString(HttpHeaders.CONTENT_LENGTH)).thenReturn("0");

    final ObjectEndpoint endpoint = newEndpoint(client, headers);

    final String uploadId = EndpointTestUtils.initiateMultipartUpload(endpoint, bucketName, destKey);
    assertNotNull(uploadId);

    endpoint.queryParamsForTest().set(S3Consts.QueryParams.UPLOAD_ID, uploadId);
    endpoint.queryParamsForTest().setInt(S3Consts.QueryParams.PART_NUMBER, 1);

    final AuditingObjectOperationHandler auditing = spy(new AuditingObjectOperationHandler(endpoint));
    final ObjectRequestContext requestContext = endpoint.new ObjectRequestContext(S3GAction.CREATE_KEY, bucketName);

    auditing.handlePutRequest(requestContext, destKey, new ByteArrayInputStream(new byte[0]));

    verify(auditing).auditWriteSuccess(eq(S3GAction.CREATE_MULTIPART_KEY_BY_COPY), any(PerformanceStringBuilder.class));
  }

  private static ObjectEndpoint newEndpoint(OzoneClient client, HttpHeaders headers) {
    return EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();
  }
}

