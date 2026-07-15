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

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.UNSIGNED_PAYLOAD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.Test;

/**
 * Verifies bucket-owner-condition verification (source bucket owner lookup) runs under the correct IAM S3 action
 * string for copy-style operations.
 */
public class TestS3ActionOverrideForOwnerVerification {

  private static final String DEST_BUCKET = "dest-bucket";
  private static final String DEST_KEY = "dest-key";
  private static final String SOURCE_BUCKET = "source-bucket";
  private static final String SOURCE_KEY = "source-key";
  private static final String SOURCE_OWNER = "source-owner";
  private static final String DEST_OWNER = "dest-owner";

  @Test
  public void testUploadPartCopyUsesGetObjectActionForSourceBucketOwnerLookup() throws Exception {
    final AtomicReference<String> actionAtSourceBucketOwnerLookup = new AtomicReference<>();
    final ObjectEndpoint endpoint = newEndpoint(actionAtSourceBucketOwnerLookup, true);

    // Trigger UploadPartCopy (MPU part upload with copy header).
    final String uploadId = "upload-id";
    assertThrows(Exception.class, () -> put(endpoint, DEST_BUCKET, DEST_KEY, 1, uploadId, ""));

    assertEquals("GetObject", actionAtSourceBucketOwnerLookup.get());
  }

  @Test
  public void testCopyObjectUsesGetObjectActionForSourceBucketOwnerLookup() throws Exception {
    final AtomicReference<String> actionAtSourceBucketOwnerLookup = new AtomicReference<>();
    final ObjectEndpoint endpoint = newEndpoint(actionAtSourceBucketOwnerLookup, true);

    // Trigger CopyObject (PUT with copy header, no upload ID).
    assertThrows(Exception.class, () -> put(endpoint, DEST_BUCKET, DEST_KEY, ""));

    assertEquals("GetObject", actionAtSourceBucketOwnerLookup.get());
  }

  @Test
  public void testUploadPartCopyDoesNotSetActionWhenStsDisabled() throws Exception {
    final AtomicReference<String> actionAtSourceBucketOwnerLookup = new AtomicReference<>();
    final ObjectEndpoint endpoint = newEndpoint(actionAtSourceBucketOwnerLookup, false);

    // Trigger UploadPartCopy (MPU part upload with copy header).
    final String uploadId = "upload-id";
    assertThrows(Exception.class, () -> put(endpoint, DEST_BUCKET, DEST_KEY, 1, uploadId, ""));

    assertNull(actionAtSourceBucketOwnerLookup.get());
  }

  @Test
  public void testCopyObjectDoesNotSetActionWhenStsDisabled() throws Exception {
    final AtomicReference<String> actionAtSourceBucketOwnerLookup = new AtomicReference<>();
    final ObjectEndpoint endpoint = newEndpoint(actionAtSourceBucketOwnerLookup, false);

    // Trigger CopyObject (PUT with copy header, no upload ID).
    assertThrows(Exception.class, () -> put(endpoint, DEST_BUCKET, DEST_KEY, ""));

    assertNull(actionAtSourceBucketOwnerLookup.get());
  }

  private static ObjectEndpoint newEndpoint(AtomicReference<String> actionAtSourceBucketOwnerLookup,
      boolean isStsEnabled) throws Exception {
    final HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn(UNSIGNED_PAYLOAD);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(SOURCE_BUCKET + "/" + SOURCE_KEY);
    when(headers.getHeaderString(EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn(SOURCE_OWNER);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(DEST_OWNER);

    final SignatureInfo signatureInfo = mock(SignatureInfo.class);
    when(signatureInfo.isSignPayload()).thenReturn(true);
    when(signatureInfo.getStringToSign()).thenReturn("string-to-sign");
    when(signatureInfo.getSignature()).thenReturn("signature");
    when(signatureInfo.getAwsAccessId()).thenReturn("access-id");
    when(signatureInfo.getSessionToken()).thenReturn(null);

    final OzoneClient client = mock(OzoneClient.class);
    final ObjectStore objectStore = mock(ObjectStore.class);
    final ClientProtocol clientProtocol = mock(ClientProtocol.class);
    final OzoneVolume volume = mock(OzoneVolume.class);
    final OzoneBucket destBucket = mock(OzoneBucket.class);
    final OzoneBucket sourceBucket = mock(OzoneBucket.class);

    final AtomicReference<S3Auth> s3AuthRef = new AtomicReference<>();
    doAnswer(invocationOnMock -> {
      s3AuthRef.set(invocationOnMock.getArgument(0));
      return null;
    }).when(clientProtocol).setThreadLocalS3Auth(any(S3Auth.class));
    doNothing().when(clientProtocol).setIsS3Request(true);

    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getProxy()).thenReturn(clientProtocol);
    when(objectStore.getClientProxy()).thenReturn(clientProtocol);
    when(objectStore.getS3Volume()).thenReturn(volume);

    when(volume.getName()).thenReturn("s3Volume");

    when(destBucket.getName()).thenReturn(DEST_BUCKET);
    when(destBucket.getOwner()).thenReturn(DEST_OWNER);
    when(volume.getBucket(DEST_BUCKET)).thenReturn(destBucket);

    when(sourceBucket.getOwner()).thenAnswer(invocationOnMock -> {
      final S3Auth s3Auth = s3AuthRef.get();
      assertNotNull(s3Auth, "S3Auth must be initialized before owner lookup");
      actionAtSourceBucketOwnerLookup.set(s3Auth.getS3Action());
      return SOURCE_OWNER;
    });
    when(volume.getBucket(SOURCE_BUCKET)).thenReturn(sourceBucket);

    // Stop the request after the source-bucket owner check, without needing to set up full copy behavior.
    when(clientProtocol.getKeyDetails(anyString(), eq(SOURCE_BUCKET), eq(SOURCE_KEY)))
        .thenThrow(new OMException("stop-after-owner-check", ResultCodes.KEY_NOT_FOUND));

    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY, isStsEnabled);
    return EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setConfig(conf)
        .setHeaders(headers)
        .setSignatureInfo(signatureInfo)
        .build();
  }
}

