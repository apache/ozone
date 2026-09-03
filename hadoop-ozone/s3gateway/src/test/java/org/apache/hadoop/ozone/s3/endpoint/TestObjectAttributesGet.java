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

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.completeMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.getObjectAttributes;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.initiateMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.uploadPart;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_ARGUMENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneBucketStub;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for GetObjectAttributes.
 */
public class TestObjectAttributesGet {

  private static final String CONTENT = "0123456789";
  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key1";
  private ObjectEndpoint rest;
  private OzoneBucket bucket;

  @BeforeEach
  public void init() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucket = client.getObjectStore().getS3Bucket(BUCKET_NAME);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("UNSIGNED-PAYLOAD");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();
  }

  @Test
  public void testGetObjectAttributesAllSupportedFields() throws IOException, OS3Exception {
    assertSucceeds(() -> put(rest, BUCKET_NAME, KEY_NAME, CONTENT));

    Response response = getObjectAttributes(rest, BUCKET_NAME, KEY_NAME,
        "ETag,ObjectSize,StorageClass");

    assertEquals(HTTP_OK, response.getStatus());
    assertNotNull(response.getHeaderString(HttpHeaders.LAST_MODIFIED));

    GetObjectAttributesResponse attributes = (GetObjectAttributesResponse) response.getEntity();
    assertNotNull(attributes);
    assertNotNull(attributes.getETag());
    assertFalse(attributes.getETag().startsWith("\""));
    assertEquals(CONTENT.length(), attributes.getObjectSize().longValue());
    assertEquals("STANDARD", attributes.getStorageClass());
    assertNull(attributes.getObjectParts());
  }

  @Test
  public void testGetObjectAttributesEtagOnly() throws IOException, OS3Exception {
    assertSucceeds(() -> put(rest, BUCKET_NAME, KEY_NAME, CONTENT));

    Response response = getObjectAttributes(rest, BUCKET_NAME, KEY_NAME, "ETag");

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse attributes = (GetObjectAttributesResponse) response.getEntity();
    assertNotNull(attributes.getETag());
    assertNull(attributes.getObjectSize());
    assertNull(attributes.getStorageClass());
  }

  @Test
  public void testGetObjectAttributesChecksumRequestedButOmitted() throws IOException, OS3Exception {
    assertSucceeds(() -> put(rest, BUCKET_NAME, KEY_NAME, CONTENT));

    Response response = getObjectAttributes(rest, BUCKET_NAME, KEY_NAME, "Checksum,ETag");

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse attributes = (GetObjectAttributesResponse) response.getEntity();
    assertNotNull(attributes.getETag());
  }

  @Test
  public void testGetObjectAttributesMissingHeader() {
    assertErrorResponse(INVALID_ARGUMENT,
        () -> getObjectAttributes(rest, BUCKET_NAME, KEY_NAME, null));
  }

  @Test
  public void testGetObjectAttributesInvalidAttribute() {
    assertErrorResponse(INVALID_ARGUMENT,
        () -> getObjectAttributes(rest, BUCKET_NAME, KEY_NAME, "NotAValidAttribute"));
  }

  @Test
  public void testGetObjectAttributesNoKeyFound() {
    assertErrorResponse(NO_SUCH_KEY,
        () -> getObjectAttributes(rest, BUCKET_NAME, "nonexistent", "ETag"));
  }

  @Test
  public void testGetObjectAttributesNoBucketFound() {
    assertErrorResponse(NO_SUCH_BUCKET,
        () -> getObjectAttributes(rest, "nonexistent", "any", "ETag"));
  }

  @Test
  public void testGetObjectAttributesNonMultipartObjectHasNoParts() throws IOException, OS3Exception {
    assertSucceeds(() -> put(rest, BUCKET_NAME, KEY_NAME, CONTENT));

    Response response = getObjectAttributes(rest, BUCKET_NAME, KEY_NAME, "ObjectParts");

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse attributes = (GetObjectAttributesResponse) response.getEntity();
    assertNull(attributes.getObjectParts());
  }

  @Test
  public void testWhenKeyIsDirectoryAndKeyPathDoesNotEndWithASlash() throws Exception {
    final String keyPath = "keyDir";
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, true);
    rest.setOzoneConfiguration(config);
    bucket.createDirectory(keyPath);

    assertErrorResponse(NO_SUCH_KEY,
        () -> getObjectAttributes(rest, BUCKET_NAME, keyPath, "ETag"));
  }

  @Test
  public void testWhenKeyIsDirectoryAndKeyPathEndsWithASlash() throws Exception {
    final String keyPath = "keyDir/";
    OzoneConfiguration config = new OzoneConfiguration();
    config.setBoolean(OZONE_S3G_FSO_DIRECTORY_CREATION_ENABLED, true);
    rest.setOzoneConfiguration(config);
    bucket.createDirectory(keyPath);

    Response response = getObjectAttributes(rest, BUCKET_NAME, keyPath, "ObjectSize");

    assertEquals(HTTP_OK, response.getStatus());
  }

  @Test
  public void testGetObjectAttributesMultipartObjectPartsListsAllParts() throws IOException, OS3Exception {
    final String key = "mpu-key";
    completeMultipartUploadWithParts(key, "part-one", "part-two", "part-three");

    Response response = getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts");

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse attributes = (GetObjectAttributesResponse) response.getEntity();
    GetObjectAttributesResponse.ObjectParts objectParts = attributes.getObjectParts();
    assertNotNull(objectParts);
    assertEquals(3, objectParts.getPartsCount().intValue());
    assertFalse(objectParts.isTruncated());
    assertEquals(S3Consts.GET_OBJECT_ATTRIBUTES_MAX_PARTS_LIMIT, objectParts.getMaxParts().intValue());
    assertNull(objectParts.getPartNumberMarker());
    assertNull(objectParts.getNextPartNumberMarker());
    assertTrue(objectParts.getParts().isEmpty());
  }

  @Test
  public void testGetObjectAttributesMultipartObjectPartsReturnsPartsWhenChecksumStored()
      throws IOException, OS3Exception {
    final String key = "mpu-with-checksum";
    completeMultipartUploadWithParts(key, "part-one", "part-two");
    ((OzoneBucketStub) bucket).putKeyMetadataForTest(key, "x-amz-checksum-crc32", "dummy");

    Response response = getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts");

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse.ObjectParts objectParts =
        ((GetObjectAttributesResponse) response.getEntity()).getObjectParts();
    assertEquals(2, objectParts.getPartsCount().intValue());
    assertEquals(2, objectParts.getParts().size());
    assertEquals(1, objectParts.getParts().get(0).getPartNumber());
    assertEquals("part-one".length(), objectParts.getParts().get(0).getSize());
    assertEquals(2, objectParts.getParts().get(1).getPartNumber());
    assertEquals("part-two".length(), objectParts.getParts().get(1).getSize());
  }

  @Test
  public void testGetObjectAttributesMultipartObjectPartsPagination() throws IOException, OS3Exception {
    final String key = "mpu-paginated";
    completeMultipartUploadWithParts(key, "part-one", "part-two", "part-three");

    Response response = getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts", 2, 0);

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse.ObjectParts objectParts =
        ((GetObjectAttributesResponse) response.getEntity()).getObjectParts();
    assertEquals(3, objectParts.getPartsCount().intValue());
    assertTrue(objectParts.isTruncated());
    assertEquals(2, objectParts.getMaxParts().intValue());
    assertEquals(0, objectParts.getPartNumberMarker().intValue());
    assertEquals(2, objectParts.getNextPartNumberMarker().intValue());
    assertTrue(objectParts.getParts().isEmpty());
  }

  @Test
  public void testGetObjectAttributesInvalidMaxParts() throws IOException, OS3Exception {
    final String key = "mpu-invalid-max-parts";
    completeMultipartUploadWithParts(key, "part-one", "part-two");

    assertErrorResponse(INVALID_ARGUMENT,
        () -> getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts",
            S3Consts.GET_OBJECT_ATTRIBUTES_MAX_PARTS_LIMIT + 1, null));
  }

  @Test
  public void testGetObjectAttributesNonContiguousMultipartParts() throws IOException, OS3Exception {
    final String key = "mpu-non-contiguous";
    final String partOneContent = "part-one";
    final String partThreeContent = "part-three";
    String uploadID = initiateMultipartUpload(rest, BUCKET_NAME, key);
    List<Part> partsList = new ArrayList<>();
    partsList.add(uploadPart(rest, BUCKET_NAME, key, 1, uploadID, partOneContent));
    partsList.add(uploadPart(rest, BUCKET_NAME, key, 3, uploadID, partThreeContent));
    completeMultipartUpload(rest, BUCKET_NAME, key, uploadID, partsList);

    Response responseWithoutChecksum = getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts");

    assertEquals(HTTP_OK, responseWithoutChecksum.getStatus());
    GetObjectAttributesResponse.ObjectParts objectPartsWithoutChecksum =
        ((GetObjectAttributesResponse) responseWithoutChecksum.getEntity()).getObjectParts();
    assertEquals(2, objectPartsWithoutChecksum.getPartsCount().intValue());
    assertFalse(objectPartsWithoutChecksum.isTruncated());
    assertTrue(objectPartsWithoutChecksum.getParts().isEmpty());

    ((OzoneBucketStub) bucket).putKeyMetadataForTest(key, "x-amz-checksum-crc32", "dummy");

    Response response = getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts");

    assertEquals(HTTP_OK, response.getStatus());
    GetObjectAttributesResponse.ObjectParts objectParts =
        ((GetObjectAttributesResponse) response.getEntity()).getObjectParts();
    assertEquals(2, objectParts.getPartsCount().intValue());
    assertFalse(objectParts.isTruncated());
    assertEquals(2, objectParts.getParts().size());
    assertEquals(1, objectParts.getParts().get(0).getPartNumber());
    assertEquals(partOneContent.length(), objectParts.getParts().get(0).getSize());
    assertEquals(3, objectParts.getParts().get(1).getPartNumber());
    assertEquals(partThreeContent.length(), objectParts.getParts().get(1).getSize());

    Response paginatedResponse = getObjectAttributes(rest, BUCKET_NAME, key, "ObjectParts", 1, 1);

    assertEquals(HTTP_OK, paginatedResponse.getStatus());
    GetObjectAttributesResponse.ObjectParts paginatedParts =
        ((GetObjectAttributesResponse) paginatedResponse.getEntity()).getObjectParts();
    assertEquals(2, paginatedParts.getPartsCount().intValue());
    assertFalse(paginatedParts.isTruncated());
    assertEquals(1, paginatedParts.getPartNumberMarker().intValue());
    assertNull(paginatedParts.getNextPartNumberMarker());
    assertEquals(1, paginatedParts.getParts().size());
    assertEquals(3, paginatedParts.getParts().get(0).getPartNumber());
    assertEquals(partThreeContent.length(), paginatedParts.getParts().get(0).getSize());
  }

  private void completeMultipartUploadWithParts(String key, String... partContents)
      throws IOException, OS3Exception {
    String uploadID = initiateMultipartUpload(rest, BUCKET_NAME, key);
    List<Part> partsList = new ArrayList<>();
    for (int i = 0; i < partContents.length; i++) {
      partsList.add(uploadPart(rest, BUCKET_NAME, key, i + 1, uploadID, partContents[i]));
    }
    completeMultipartUpload(rest, BUCKET_NAME, key, uploadID, partsList);
  }
}
