/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.conf.DefaultConfigManager;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.om.OzoneManager;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadRequest;
import org.apache.hadoop.ozone.s3.endpoint.CompleteMultipartUploadResponse;
import org.apache.hadoop.ozone.s3.endpoint.MultipartUploadInitiateResponse;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Base64;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.MP_PARTS_COUNT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test class for testing 'ranged' GET request for the part
 * specified.
 */
public class TestMultipartObjectGet {

  public static final Logger LOG = LoggerFactory.getLogger(
      TestMultipartObjectGet.class);
  private static OzoneConfiguration conf;
  private static String omServiceId;
  private static String scmServiceId;
  private static final String BUCKET = OzoneConsts.BUCKET;
  private static final String KEY = OzoneConsts.KEY;
  private static MiniOzoneHAClusterImpl cluster;
  private static OzoneClient client;
  private static HttpHeaders headers;
  private static ContainerRequestContext context;

  private static final ObjectEndpoint REST = new ObjectEndpoint();

  @BeforeAll
  public static void init() throws Exception {
    conf = new OzoneConfiguration();
    omServiceId = "om-service-test";
    scmServiceId = "scm-service-test";

    startCluster();
    client = cluster.newClient();
    client.getObjectStore().createS3Bucket(BUCKET);

    headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    context = mock(ContainerRequestContext.class);
    when(context.getUriInfo()).thenReturn(mock(UriInfo.class));
    when(context.getUriInfo().getQueryParameters())
        .thenReturn(new MultivaluedHashMap<>());

    REST.setHeaders(headers);
    REST.setClient(client);
    REST.setOzoneConfiguration(conf);
    REST.setContext(context);
    S3GatewayMetrics.create(conf);
  }

  private static void startCluster()
      throws IOException, TimeoutException, InterruptedException {
    OzoneManager.setTestSecureOmFlag(true);
    MiniOzoneHAClusterImpl.Builder builder = MiniOzoneCluster.newHABuilder(conf)
        .setSCMServiceId(scmServiceId)
        .setOMServiceId(omServiceId)
        .setNumOfStorageContainerManagers(3)
        .setNumOfOzoneManagers(3);
    cluster = builder.build();
    cluster.waitForClusterToBeReady();
  }

  @AfterAll
  public static void stop() {
    IOUtils.close(LOG, client);
    if (cluster != null) {
      cluster.stop();
    }
    DefaultConfigManager.clearDefaultConfigs();
  }

  private String initiateMultipartUpload() throws IOException, OS3Exception {
    Response response = REST.initializeMultipartUpload(BUCKET, KEY);
    MultipartUploadInitiateResponse multipartUploadInitiateResponse =
        (MultipartUploadInitiateResponse) response.getEntity();
    assertNotNull(multipartUploadInitiateResponse.getUploadID());
    String uploadID = multipartUploadInitiateResponse.getUploadID();
    assertEquals(200, response.getStatus());
    return uploadID;
  }

  private CompleteMultipartUploadRequest.Part uploadPart(String uploadID,
                                                         int partNumber,
                                                         String content)
      throws IOException, OS3Exception {
    ByteArrayInputStream body =
        new ByteArrayInputStream(content.getBytes(UTF_8));
    Response response = REST.put(BUCKET, KEY, content.length(),
        partNumber, uploadID, body);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getHeaderString(OzoneConsts.ETAG));

    CompleteMultipartUploadRequest.Part
        part = new CompleteMultipartUploadRequest.Part();
    part.setETag(response.getHeaderString(OzoneConsts.ETAG));
    part.setPartNumber(partNumber);
    return part;
  }

  private void completeMultipartUpload(
      CompleteMultipartUploadRequest completeMultipartUploadRequest,
      String uploadID) throws IOException, OS3Exception {
    Response response = REST.completeMultipartUpload(BUCKET, KEY, uploadID,
        completeMultipartUploadRequest);
    assertEquals(200, response.getStatus());

    CompleteMultipartUploadResponse completeMultipartUploadResponse =
        (CompleteMultipartUploadResponse) response.getEntity();
    assertEquals(BUCKET, completeMultipartUploadResponse.getBucket());
    assertEquals(KEY, completeMultipartUploadResponse.getKey());
    assertEquals(BUCKET, completeMultipartUploadResponse.getLocation());
    assertNotNull(completeMultipartUploadResponse.getETag());
  }

  private void getObjectMultipart(int partNumber, long bytes)
      throws IOException, OS3Exception {
    Response response =
        REST.get(BUCKET, KEY, partNumber, null, 100, null);
    assertEquals(200, response.getStatus());
    assertEquals(bytes, response.getLength());
    assertEquals("3", response.getHeaderString(MP_PARTS_COUNT));
  }

  private void headObjectMultipart() throws IOException, OS3Exception {
    Response response = REST.head(BUCKET, KEY);
    assertEquals(200, response.getStatus());
    assertEquals("3", response.getHeaderString(MP_PARTS_COUNT));
  }

  @Test
  public void testMultipart() throws Exception {
    String uploadID = initiateMultipartUpload();
    List<CompleteMultipartUploadRequest.Part> partsList = new ArrayList<>();

    String content1 = generateRandomContent(5);
    int partNumber = 1;
    CompleteMultipartUploadRequest.Part
        part1 = uploadPart(uploadID, partNumber, content1);
    partsList.add(part1);

    String content2 = generateRandomContent(5);
    partNumber = 2;
    CompleteMultipartUploadRequest.Part
        part2 = uploadPart(uploadID, partNumber, content2);
    partsList.add(part2);

    String content3 = generateRandomContent(1);
    partNumber = 3;
    CompleteMultipartUploadRequest.Part
        part3 = uploadPart(uploadID, partNumber, content3);
    partsList.add(part3);

    CompleteMultipartUploadRequest completeMultipartUploadRequest = new
        CompleteMultipartUploadRequest();
    completeMultipartUploadRequest.setPartList(partsList);
    completeMultipartUpload(completeMultipartUploadRequest, uploadID);

    headObjectMultipart();

    getObjectMultipart(0,
        (content1 + content2 + content3).getBytes(UTF_8).length);
    getObjectMultipart(1, content1.getBytes(UTF_8).length);
    getObjectMultipart(2, content2.getBytes(UTF_8).length);
    getObjectMultipart(3, content3.getBytes(UTF_8).length);
  }

  private static String generateRandomContent(int sizeInMB) {
    int bytesToGenerate = sizeInMB * 1024 * 1024;
    byte[] randomBytes = RandomUtils.nextBytes(bytesToGenerate);
    return Base64.getEncoder().encodeToString(randomBytes);
  }
}
