/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.hadoop.ozone.s3.endpoint;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

/**
 * Class to test Multipart upload end to end.
 */

public class TestMultipartUploadComplete {

  private static final ObjectEndpoint REST = new ObjectEndpoint();
  private static final HttpHeaders HEADERS = Mockito.mock(HttpHeaders.class);
  private static final OzoneClient CLIENT = new OzoneClientStub();

  @BeforeAll
  public static void setUp() throws Exception {

    CLIENT.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);


    when(HEADERS.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(HEADERS);
    REST.setClient(CLIENT);
    REST.setOzoneConfiguration(new OzoneConfiguration());
  }

  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
    return initiateMultipartUpload(key, Collections.emptyMap());
  }

  private String initiateMultipartUpload(String key, Map<String, String> metadata) throws IOException,
      OS3Exception {
    MultivaluedMap<String, String> metadataHeaders = new MultivaluedHashMap<>();

    for (Map.Entry<String, String> entry : metadata.entrySet()) {
      metadataHeaders.computeIfAbsent(CUSTOM_METADATA_HEADER_PREFIX + entry.getKey(), k -> new ArrayList<>())
          .add(entry.getValue());
    }

    when(HEADERS.getRequestHeaders()).thenReturn(metadataHeaders);

    Response response = REST.initializeMultipartUpload(OzoneConsts.S3_BUCKET,
        key);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();

    assertEquals(200, response.getStatus());

    return uploadID;
  }

  private Part uploadPart(String key, String uploadID, int partNumber, String
      content) throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = REST.put(OzoneConsts.S3_BUCKET, key, content.length(),
        partNumber, uploadID, body);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));
    Part part = new Part();
    part.setETag(response.getHeaderString(OzoneConsts.ETAG));
    part.setPartNumber(partNumber);

    return part;
  }

  private void completeMultipartUpload(String key,
      CompleteMultipartUploadRequest completeMultipartUploadRequest,
      String uploadID) throws IOException, OS3Exception {
    Response response = REST.completeMultipartUpload(OzoneConsts.S3_BUCKET, key,
        uploadID, completeMultipartUploadRequest);

    assertEquals(200, response.getStatus());

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        (CompleteMultipartUploadResponse) response.getEntity();

    assertEquals(OzoneConsts.S3_BUCKET,
        completeMultipartUploadResponse.getBucket());
    assertEquals(key, completeMultipartUploadResponse.getKey());
    assertEquals(OzoneConsts.S3_BUCKET,
        completeMultipartUploadResponse.getLocation());
    assertNotNull(completeMultipartUploadResponse.getETag());
  }

  @Test
  public void testMultipart() throws Exception {

    // Initiate multipart upload
    String uploadID = initiateMultipartUpload(OzoneConsts.KEY);

    List<Part> partsList = new ArrayList<>();


    // Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part1);

    content = "Multipart Upload 2";
    partNumber = 2;
    Part part2 = uploadPart(OzoneConsts.KEY, uploadID, partNumber, content);
    partsList.add(part2);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);


    completeMultipartUpload(OzoneConsts.KEY, completeMultipartUploadRequest,
        uploadID);

  }

  @Test
  public void testMultipartWithCustomMetadata() throws Exception {
    String key = UUID.randomUUID().toString();

    Map<String, String> customMetadata = new HashMap<>();
    customMetadata.put("custom-key1", "custom-value1");
    customMetadata.put("custom-key2", "custom-value2");

    String uploadID = initiateMultipartUpload(key, customMetadata);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(key, uploadID, partNumber, content);
    partsList.add(part1);

    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);

    completeMultipartUpload(key, completeMultipartUploadRequest, uploadID);

    Response headResponse = REST.head(OzoneConsts.S3_BUCKET, key);

    assertEquals("custom-value1", headResponse.getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + "custom-key1"));
    assertEquals("custom-value2", headResponse.getHeaderString(CUSTOM_METADATA_HEADER_PREFIX + "custom-key2"));
  }


  @Test
  public void testMultipartInvalidPartOrderError() throws Exception {

    // Initiate multipart upload
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(key, uploadID, partNumber, content);
    // Change part number
    part1.setPartNumber(3);
    partsList.add(part1);

    content = "Multipart Upload 2";
    partNumber = 2;

    Part part2 = uploadPart(key, uploadID, partNumber, content);
    partsList.add(part2);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);
    try {
      completeMultipartUpload(key, completeMultipartUploadRequest, uploadID);
      fail("testMultipartInvalidPartOrderError");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.INVALID_PART_ORDER.getCode(), ex.getCode());
    }

  }

  @Test
  public void testMultipartInvalidPartError() throws Exception {

    // Initiate multipart upload
    String key = UUID.randomUUID().toString();
    String uploadID = initiateMultipartUpload(key);

    List<Part> partsList = new ArrayList<>();

    // Upload parts
    String content = "Multipart Upload 1";
    int partNumber = 1;

    Part part1 = uploadPart(key, uploadID, partNumber, content);
    // Change part name.
    part1.setETag("random");
    partsList.add(part1);

    content = "Multipart Upload 2";
    partNumber = 2;

    Part part2 = uploadPart(key, uploadID, partNumber, content);
    partsList.add(part2);

    // complete multipart upload
    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);
    try {
      completeMultipartUpload(key, completeMultipartUploadRequest, uploadID);
      fail("testMultipartInvalidPartError");
    } catch (OS3Exception ex) {
      assertEquals(ex.getCode(), S3ErrorTable.INVALID_PART.getCode());
    }

  }
}
