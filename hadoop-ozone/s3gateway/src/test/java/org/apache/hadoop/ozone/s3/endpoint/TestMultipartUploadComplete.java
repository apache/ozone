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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.completeMultipartUpload;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.uploadPart;
import static org.apache.hadoop.ozone.s3.util.S3Consts.CUSTOM_METADATA_HEADER_PREFIX;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest.Part;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
        .thenReturn("mockSignature");

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
}
