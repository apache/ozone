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

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

/**
 * Test head object.
 */
public class TestObjectHead {
  private String bucketName = "b1";
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    OzoneClient clientStub = new OzoneClientStub();

    // Create volume and bucket
    clientStub.getObjectStore().createS3Bucket(bucketName);

    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);

    // Create HeadBucket and setClient to OzoneClientStub
    keyEndpoint = new ObjectEndpoint();
    keyEndpoint.setClient(clientStub);
    keyEndpoint.setOzoneConfiguration(new OzoneConfiguration());
  }

  @Test
  public void testHeadObject() throws Exception {
    //GIVEN
    String value = RandomStringUtils.randomAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    //WHEN
    Response response = keyEndpoint.head(bucketName, "key1");

    //THEN
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(value.getBytes(UTF_8).length,
        Long.parseLong(response.getHeaderString("Content-Length")));

    DateTimeFormatter.RFC_1123_DATE_TIME
        .parse(response.getHeaderString("Last-Modified"));

  }

  @Test
  public void testHeadFailByBadName() throws Exception {
    //Head an object that doesn't exist.
    try {
      Response response =  keyEndpoint.head(bucketName, "badKeyName");
      Assert.assertEquals(404, response.getStatus());
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchObject"));
      Assert.assertTrue(ex.getErrorMessage().contains("object does not exist"));
      Assert.assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
    }
  }

  @Test
  public void testHeadWhenKeyIsAFileAndKeyPathDoesNotEndWithASlash()
      throws IOException,
      OS3Exception {
    // GIVEN
    final String keyPath = "dir";
    final ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());
    final OzoneClient ozoneClient = Mockito.mock(OzoneClient.class);
    objectEndpoint.setClient(ozoneClient);
    final ClientProtocol clientProtocol = Mockito.mock(ClientProtocol.class);
    final OzoneKey ozoneKey = Mockito.mock(OzoneKey.class);

    // WHEN
    when(ozoneClient.getProxy()).thenReturn(clientProtocol);
    when(clientProtocol.headS3Object(bucketName, keyPath)).thenReturn(ozoneKey);
    when(ozoneKey.getModificationTime())
        .thenReturn(Instant.ofEpochMilli(System.currentTimeMillis()));
    when(ozoneKey.isFile()).thenReturn(true);

    final Response response = objectEndpoint.head(bucketName, keyPath);

    // THEN
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());
  }

  @Test
  public void testHeadWhenKeyIsNotAFileAndKeyPathDoesNotEndWithASlash()
      throws IOException,
      OS3Exception {
    // GIVEN
    final String keyPath = "dir";
    final ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());
    final OzoneClient ozoneClient = Mockito.mock(OzoneClient.class);
    objectEndpoint.setClient(ozoneClient);
    final ClientProtocol clientProtocol = Mockito.mock(ClientProtocol.class);
    final OzoneKey ozoneKey = Mockito.mock(OzoneKey.class);

    // WHEN
    when(ozoneClient.getProxy()).thenReturn(clientProtocol);
    when(clientProtocol.headS3Object(bucketName, keyPath)).thenReturn(ozoneKey);
    when(ozoneKey.isFile()).thenReturn(false);

    final Response response = objectEndpoint.head(bucketName, keyPath);

    // THEN
    Assertions.assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatus());
  }

  @Test
  public void testHeadWhenKeyIsNotAFileAndKeyPathEndsWithASlash()
      throws IOException,
      OS3Exception {
    // GIVEN
    final String keyPath = "dir/";
    final ObjectEndpoint objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());
    final OzoneClient ozoneClient = Mockito.mock(OzoneClient.class);
    objectEndpoint.setClient(ozoneClient);
    final ClientProtocol clientProtocol = Mockito.mock(ClientProtocol.class);
    final OzoneKey ozoneKey = Mockito.mock(OzoneKey.class);

    // WHEN
    when(ozoneClient.getProxy()).thenReturn(clientProtocol);
    when(clientProtocol.headS3Object(bucketName, keyPath)).thenReturn(ozoneKey);
    when(ozoneKey.isFile()).thenReturn(false);
    when(ozoneKey.getModificationTime())
        .thenReturn(Instant.ofEpochMilli(System.currentTimeMillis()));

    final Response response = objectEndpoint.head(bucketName, keyPath);

    // THEN
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());
  }
}
