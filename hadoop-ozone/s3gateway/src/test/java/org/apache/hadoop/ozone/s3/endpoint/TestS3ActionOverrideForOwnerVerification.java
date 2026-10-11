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

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.getObjectAttributes;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.S3HeadObjectAttributes;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.om.protocol.S3Auth;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Verifies temporary IAM S3 action overrides used by composite operations.
 */
public class TestS3ActionOverrideForOwnerVerification {

  private static final String DEST_BUCKET = "dest-bucket";
  private static final String DEST_KEY = "dest-key";
  private static final String SOURCE_BUCKET = "source-bucket";
  private static final String SOURCE_KEY = "source-key";
  private static final String SOURCE_OWNER = "source-owner";
  private static final String DEST_OWNER = "dest-owner";

  @BeforeAll
  public static void setUp() {
    S3GatewayMetrics.create(new OzoneConfiguration());
  }

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

  @ParameterizedTest
  @ValueSource(strings = {"ETag", "ObjectParts"})
  public void testGetObjectAttributesSendsSingleOmRequestWithGetObjectAttributesAction(String attribute)
      throws Exception {
    final List<String> actions = new ArrayList<>();
    final ClientProtocol clientProtocol = mock(ClientProtocol.class);
    final ObjectEndpoint endpoint = newGetObjectAttributesEndpoint(actions, true, clientProtocol, false);

    final Response response = getObjectAttributes(endpoint, DEST_BUCKET, DEST_KEY, attribute);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    // OM enforces the dependent GetObject action itself, so S3 Gateway makes exactly one OM request.
    assertEquals(Collections.singletonList("GetObjectAttributes"), actions);
    verifySingleMetadataLookup(clientProtocol, attribute);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ETag", "ObjectParts"})
  public void testGetObjectAttributesSkipsAdditionalCheckWhenStsDisabled(String attribute) throws Exception {
    final List<String> actions = new ArrayList<>();
    final ObjectEndpoint endpoint = newGetObjectAttributesEndpoint(actions, false);

    final Response response = getObjectAttributes(endpoint, DEST_BUCKET, DEST_KEY, attribute);

    assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
    // With STS disabled, no S3 action is sent to OM.
    assertEquals(Collections.singletonList(null), actions);
  }

  @ParameterizedTest
  @ValueSource(strings = {"ETag", "ObjectParts"})
  public void testGetObjectAttributesPermissionDeniedReturnsAccessDenied(String attribute) throws Exception {
    final List<String> actions = new ArrayList<>();
    final ClientProtocol clientProtocol = mock(ClientProtocol.class);
    final ObjectEndpoint endpoint = newGetObjectAttributesEndpoint(actions, true, clientProtocol, true);

    final OS3Exception ex = assertThrows(
        OS3Exception.class, () -> getObjectAttributes(endpoint, DEST_BUCKET, DEST_KEY, attribute));

    assertEquals("AccessDenied", ex.getCode());
    assertEquals(Collections.singletonList("GetObjectAttributes"), actions);
    verifySingleMetadataLookup(clientProtocol, attribute);
  }

  private static void verifySingleMetadataLookup(ClientProtocol clientProtocol, String attribute) throws Exception {
    if ("ObjectParts".equals(attribute)) {
      verify(clientProtocol).headS3ObjectAttributes(DEST_BUCKET, DEST_KEY);
      verify(clientProtocol, never()).headS3Object(DEST_BUCKET, DEST_KEY);
    } else {
      verify(clientProtocol).headS3Object(DEST_BUCKET, DEST_KEY);
      verify(clientProtocol, never()).headS3ObjectAttributes(DEST_BUCKET, DEST_KEY);
    }
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

  private static ObjectEndpoint newGetObjectAttributesEndpoint(List<String> actions, boolean isStsEnabled)
      throws Exception {
    return newGetObjectAttributesEndpoint(actions, isStsEnabled, mock(ClientProtocol.class), false);
  }

  private static ObjectEndpoint newGetObjectAttributesEndpoint(List<String> actions, boolean isStsEnabled,
      ClientProtocol clientProtocol, boolean denyAccess) throws Exception {
    final HttpHeaders headers = mock(HttpHeaders.class);
    final SignatureInfo signatureInfo = mock(SignatureInfo.class);
    when(signatureInfo.isSignPayload()).thenReturn(true);
    when(signatureInfo.getStringToSign()).thenReturn("string-to-sign");
    when(signatureInfo.getSignature()).thenReturn("signature");
    when(signatureInfo.getAwsAccessId()).thenReturn("access-id");

    final OzoneClient client = mock(OzoneClient.class);
    final ObjectStore objectStore = mock(ObjectStore.class);
    final AtomicReference<S3Auth> s3AuthRef = new AtomicReference<>();
    doAnswer(invocationOnMock -> {
      s3AuthRef.set(invocationOnMock.getArgument(0));
      return null;
    }).when(clientProtocol).setThreadLocalS3Auth(any(S3Auth.class));

    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getProxy()).thenReturn(clientProtocol);
    when(objectStore.getClientProxy()).thenReturn(clientProtocol);

    final OzoneKey key = mock(OzoneKey.class);
    when(key.isFile()).thenReturn(true);
    when(key.getModificationTime()).thenReturn(Instant.now());
    final S3HeadObjectAttributes headAttributes = mock(S3HeadObjectAttributes.class);
    when(headAttributes.getKey()).thenReturn(key);
    when(headAttributes.getBucketLayout()).thenReturn(BucketLayout.OBJECT_STORE);

    when(clientProtocol.headS3Object(DEST_BUCKET, DEST_KEY)).thenAnswer(invocationOnMock -> {
      recordS3Action(s3AuthRef, actions);
      return returnOrDeny(key, denyAccess);
    });
    when(clientProtocol.headS3ObjectAttributes(DEST_BUCKET, DEST_KEY)).thenAnswer(invocationOnMock -> {
      recordS3Action(s3AuthRef, actions);
      return returnOrDeny(headAttributes, denyAccess);
    });

    final OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.OZONE_S3G_STS_HTTP_ENABLED_KEY, isStsEnabled);
    return EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setConfig(conf)
        .setHeaders(headers)
        .setSignatureInfo(signatureInfo)
        .build();
  }

  private static <T> T returnOrDeny(T result, boolean denyAccess) throws OMException {
    if (denyAccess) {
      throw new OMException("Permission denied", ResultCodes.PERMISSION_DENIED);
    }
    return result;
  }

  private static void recordS3Action(AtomicReference<S3Auth> s3AuthRef, List<String> actions) {
    final S3Auth s3Auth = s3AuthRef.get();
    assertNotNull(s3Auth, "S3Auth must be initialized before metadata lookup");
    actions.add(s3Auth.getS3Action());
  }
}

