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

import static java.util.Collections.singletonList;
import static org.apache.hadoop.ozone.client.OzoneClientTestUtils.assertKeyContent;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.completeMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.uploadPart;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.OmConfig;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.HeaderPreprocessor;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class to test Multipart upload end to end.
 */

public class TestMultipartUploadComplete {

  private ObjectEndpoint rest;
  private HttpHeaders headers = mock(HttpHeaders.class);
  private OzoneClient client = new OzoneClientStub();

  @BeforeEach
  public void setUp() throws Exception {

    client.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);

    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("UNSIGNED-PAYLOAD");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setHeaders(headers)
        .setClient(client)
        .build();
  }

  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
    return EndpointTestUtils.initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, key);
  }

  private String initiateMultipartUpload(String key, Map<String, String> metadata) throws IOException,
      OS3Exception {
    MultivaluedMap<String, String> metadataHeaders = new MultivaluedHashMap<>();

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      metadataHeaders.computeIfAbsent(CUSTOM_METADATA_HEADER_PREFIX + entry.getKey(), k -> new ArrayList<>())
          .add(entry.getValue());
    }

    when(headers.getRequestHeaders()).thenReturn(metadataHeaders);

    return EndpointTestUtils.initiateMultipartUpload(rest, OzoneConsts.S3_BUCKET, key);
  }

  @Test
  public void testMultipart() throws Exception {
    String uploadID = initiateMultipartUpload(OzoneConsts.KEY);

    // Upload parts
    List<Part> partsList = new ArrayList<>();
    partsList.add(uploadPart(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 1, uploadID, "Multipart Upload 1"));
    partsList.add(uploadPart(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, 2, uploadID, "Multipart Upload 2"));

    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, OzoneConsts.KEY, uploadID, partsList);
  }

  @ParameterizedTest
  @ValueSource(strings = {"*", "\"some-etag\""})
  public void testMultipartIfMatchMissingKey(String ifMatch) throws Exception {
    String key = OzoneConsts.KEY;
    String content = "Multipart Upload 1";
    String uploadID = initiateMultipartUpload(key);
    Part part = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, content);
    when(headers.getHeaderString(S3Consts.IF_MATCH_HEADER)).thenReturn(ifMatch);

    OS3Exception ex = assertErrorResponse(S3ErrorTable.NO_SUCH_KEY,
        () -> completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part)));
    assertEquals(key, ex.getResource());
    assertThrows(IOException.class, () -> client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET).getKey(key));

    when(headers.getHeaderString(S3Consts.IF_MATCH_HEADER)).thenReturn(null);
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part));
    assertKeyContent(client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET), key, content);
  }

  @Test
  public void testMultipartIfNoneMatchETag() throws Exception {
    String key = OzoneConsts.KEY;
    String uploadID = initiateMultipartUpload(key);
    Part part = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "original-content");
    when(headers.getHeaderString("If-None-Match")).thenReturn("\"different-etag\"");
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part));
    String etag = client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET).getKey(key)
        .getMetadata().get(OzoneConsts.ETAG);

    String replacementUploadID = initiateMultipartUpload(key);
    Part replacementPart = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, replacementUploadID, "new-content");
    when(headers.getHeaderString("If-None-Match")).thenReturn("\"" + etag + "\"");
    assertErrorResponse(S3ErrorTable.PRECOND_FAILED,
        () -> completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, replacementUploadID,
            singletonList(replacementPart)));
    assertKeyContent(client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET), key, "original-content");

    when(headers.getHeaderString("If-None-Match")).thenReturn("\"different-etag\"");
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, replacementUploadID, singletonList(replacementPart));
    assertKeyContent(client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET), key, "new-content");
  }

  @Test
  public void testMultipartRejectsFileInParentPath() throws Exception {
    String key = "file/child";
    String uploadID = initiateMultipartUpload(key);
    Part part = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    ObjectEndpoint endpoint = spy(rest);
    ClientProtocol protocol = spy(rest.getClientProtocol());
    OzoneBucket bucket = spy(client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET));
    doReturn(protocol).when(endpoint).getClientProtocol();
    doReturn(bucket).when(protocol).getBucketDetails(any(), eq(OzoneConsts.S3_BUCKET));
    doThrow(new OMException("File exists in path", OMException.ResultCodes.NOT_A_FILE))
        .when(bucket).completeMultipartUpload(eq(key), eq(uploadID), anyMap(), any(), any(), any());

    OS3Exception ex = assertErrorResponse(S3ErrorTable.INVALID_REQUEST,
        () -> completeMultipartUpload(endpoint, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part)));

    assertEquals(400, ex.getHttpCode());
    assertEquals("An error occurred (InvalidRequest) when calling the CompleteMultipartUpload operation: " +
        OmConfig.Keys.ENABLE_FILESYSTEM_PATHS + " is enabled Keys are considered as Unix Paths. " +
        "Path has Violated FS Semantics which caused failure for MPU", ex.getErrorMessage());
  }

  @Test
  public void testMultipartWithCustomMetadata() throws Exception {
    String key = UUID.randomUUID().toString();

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    String uploadID = initiateMultipartUpload(key, customMetadata);
    Part part1 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part1));

    Response headResponse = rest.head(OzoneConsts.S3_BUCKET, key);

    assertEquals("custom-value1", headResponse.getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + "custom-key1"));
    assertEquals("custom-value2", headResponse.getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + "custom-key2"));
  }

  @Test
  public void testMultipartStoresContentType() throws Exception {
    String key = UUID.randomUUID().toString();

    // Content-Type set on CreateMultipartUpload is preserved as
    // ORIGINAL_CONTENT_TYPE and must survive to the completed object.
    when(headers.getHeaderString(HeaderPreprocessor.ORIGINAL_CONTENT_TYPE))
        .thenReturn("video/webm");

    String uploadID = initiateMultipartUpload(key);
    Part part1 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part1));

    assertEquals("video/webm", rest.head(OzoneConsts.S3_BUCKET, key)
        .getHeaderString(HttpHeaders.CONTENT_TYPE));
  }

  @Test
  public void testMultipartStoresStandardObjectHeaders() throws Exception {
    String key = UUID.randomUUID().toString();
    when(headers.getHeaderString(HttpHeaders.CACHE_CONTROL)).thenReturn("no-cache");
    when(headers.getHeaderString(HttpHeaders.EXPIRES)).thenReturn("Wed, 21 Oct 2015 07:29:00 GMT");
    when(headers.getHeaderString(HttpHeaders.CONTENT_ENCODING)).thenReturn("gzip");
    when(headers.getHeaderString(HttpHeaders.CONTENT_LANGUAGE)).thenReturn("en-CA");
    when(headers.getHeaderString(HttpHeaders.CONTENT_DISPOSITION)).thenReturn("inline");

    String uploadID = initiateMultipartUpload(key);
    Part part1 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, singletonList(part1));

    Response headResponse = rest.head(OzoneConsts.S3_BUCKET, key);
    assertEquals("no-cache", headResponse.getHeaderString(HttpHeaders.CACHE_CONTROL));
    assertEquals("Wed, 21 Oct 2015 07:29:00 GMT",
        headResponse.getHeaderString(HttpHeaders.EXPIRES));
    assertEquals("gzip", headResponse.getHeaderString(HttpHeaders.CONTENT_ENCODING));
    assertEquals("en-CA", headResponse.getHeaderString(HttpHeaders.CONTENT_LANGUAGE));
    assertEquals("inline", headResponse.getHeaderString(HttpHeaders.CONTENT_DISPOSITION));
  }

  @Test
  public void testMultipartInvalidPartOrderError() throws Exception {

    // Initiate multipart upload
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    List<Part> partsList = new ArrayList<>();

    // Upload parts

    Part part1 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    // Change part number
    part1.setPartNumber(3);
    partsList.add(part1);

    partsList.add(uploadPart(rest, OzoneConsts.S3_BUCKET, key, 2, uploadID, "Multipart Upload 2"));

    // complete multipart upload
    OS3Exception ex = assertThrows(OS3Exception.class,
        () -> completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, partsList));
    assertEquals(S3ErrorTable.INVALID_PART_ORDER.getCode(), ex.getCode());
  }

  @Test
  public void testMultipartDuplicatePartNumberError() throws Exception {
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    Part part1 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    Part part2 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 2, uploadID, "Multipart Upload 2");

    assertErrorResponse(S3ErrorTable.INVALID_PART_ORDER,
        () -> completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, Arrays.asList(part1, part1)));
    assertErrorResponse(S3ErrorTable.INVALID_PART_ORDER,
        () -> completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, Arrays.asList(part1, part2, part1)));
    assertThrows(IOException.class, () -> client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET).getKey(key));

    completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, Arrays.asList(part1, part2));
    assertKeyContent(client.getObjectStore().getS3Bucket(OzoneConsts.S3_BUCKET), key,
        "Multipart Upload 1" + "Multipart Upload 2");
  }

  @Test
  public void testMultipartInvalidPartError() throws Exception {

    // Initiate multipart upload
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    Part part1 = uploadPart(rest, OzoneConsts.S3_BUCKET, key, 1, uploadID, "Multipart Upload 1");
    // Change part name.
    part1.setETag("random");
    partsList.add(part1);

    partsList.add(uploadPart(rest, OzoneConsts.S3_BUCKET, key, 2, uploadID, "Multipart Upload 2"));

    // complete multipart upload
    OS3Exception ex = assertThrows(OS3Exception.class,
        () -> completeMultipartUpload(rest, OzoneConsts.S3_BUCKET, key, uploadID, partsList));
    assertEquals(ex.getCode(), S3ErrorTable.INVALID_PART.getCode());
  }

  @Test
  public void testMultipartEmptyPartListError() throws Exception {
    // Initiate multipart upload
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    // A CompleteMultipartUpload request with an empty part list must fail.
    CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest();
    rest.queryParamsForTest().set(S3Consts.QueryParams.UPLOAD_ID, uploadID);

    assertErrorResponse(MALFORMED_XML,
        () -> rest.completeMultipartUpload(OzoneConsts.S3_BUCKET, key, request));
  }
}
