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

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

/**
 * Class to test Multipart upload end to end.
 */

public class TestMultipartUploadComplete {

  private static final ObjectEndpoint REST = new ObjectEndpoint();
  private static final OzoneClient CLIENT = new OzoneClientStub();

  @BeforeClass
  public static void setUp() throws Exception {

    CLIENT.getObjectStore().createS3Bucket(OzoneConsts.S3_BUCKET);


    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    REST.setHeaders(headers);
    REST.setClient(CLIENT);
  }

  private String initiateMultipartUpload(String key) throws IOException,
      OS3Exception {
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
    assertNotNull(response.getHeaderString("ETag"));
    Part part = new Part();
    part.seteTag(response.getHeaderString("ETag"));
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
    part1.seteTag("random");
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
