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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Utils.urlEncode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

/**
 * Test put object.
 */
public class TestObjectPut {
  public static final String CONTENT = "0123456789";
  private String bucketName = "b1";
  private String keyName = "key=value/1";
  private String destBucket = "b2";
  private String destkey = "key=value/2";
  private String nonexist = "nonexist";
  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;

  @Before
  public void setup() throws IOException {
    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(bucketName);
    clientStub.getObjectStore().createS3Bucket(destBucket);

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = new ObjectEndpoint();
    objectEndpoint.setClient(clientStub);
    objectEndpoint.setOzoneConfiguration(new OzoneConfiguration());
  }

  @Test
  public void testPutObject() throws IOException, OS3Exception {
    //GIVEN
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);

    //WHEN
    Response response = objectEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);


    //THEN
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getS3Bucket(bucketName)
            .readKey(keyName);
    String keyContent =
        IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);
  }

  @Test
  public void testPutObjectWithECReplicationConfig()
      throws IOException, OS3Exception {
    //GIVEN
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);
    ECReplicationConfig ecReplicationConfig =
        new ECReplicationConfig("rs-3-2-1024K");
    clientStub.getObjectStore().getS3Bucket(bucketName)
        .setReplicationConfig(ecReplicationConfig);
    Response response = objectEndpoint.put(bucketName, keyName, CONTENT
        .length(), 1, null, body);

    Assert.assertEquals(ecReplicationConfig,
        clientStub.getObjectStore().getS3Bucket(bucketName).getKey(keyName)
            .getReplicationConfig());
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getS3Bucket(bucketName)
            .readKey(keyName);
    String keyContent =
        IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);
  }

  @Test
  public void testPutObjectWithSignedChunks() throws IOException, OS3Exception {
    //GIVEN
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    objectEndpoint.setHeaders(headers);

    String chunkedContent = "0a;chunk-signature=signature\r\n"
        + "1234567890\r\n"
        + "05;chunk-signature=signature\r\n"
        + "abcde\r\n";

    when(headers.getHeaderString("x-amz-content-sha256"))
        .thenReturn("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    //WHEN
    Response response = objectEndpoint.put(bucketName, keyName,
        chunkedContent.length(), 1, null,
        new ByteArrayInputStream(chunkedContent.getBytes(UTF_8)));

    //THEN
    OzoneInputStream ozoneInputStream =
        clientStub.getObjectStore().getS3Bucket(bucketName)
            .readKey(keyName);
    String keyContent = IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals("1234567890abcde", keyContent);
  }

  @Test
  public void testCopyObject() throws IOException, OS3Exception {
    // Put object in to source bucket
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);
    keyName = "sourceKey";

    Response response = objectEndpoint.put(bucketName, keyName,
        CONTENT.length(), 1, null, body);

    OzoneInputStream ozoneInputStream = clientStub.getObjectStore()
        .getS3Bucket(bucketName)
        .readKey(keyName);

    String keyContent = IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);


    // Add copy header, and then call put
    when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
        bucketName  + "/" + urlEncode(keyName));

    response = objectEndpoint.put(destBucket, destkey, CONTENT.length(), 1,
        null, body);

    // Check destination key and response
    ozoneInputStream = clientStub.getObjectStore().getS3Bucket(destBucket)
        .readKey(destkey);

    keyContent = IOUtils.toString(ozoneInputStream, UTF_8);

    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(CONTENT, keyContent);

    // source and dest same
    try {
      objectEndpoint.put(bucketName, keyName, CONTENT.length(), 1, null, body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getErrorMessage().contains("This copy request is " +
          "illegal"));
    }

    // source bucket not found
    try {
      when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          nonexist + "/"  + urlEncode(keyName));
      objectEndpoint.put(destBucket, destkey, CONTENT.length(), 1, null,
          body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

    // dest bucket not found
    try {
      when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          bucketName + "/" + urlEncode(keyName));
      objectEndpoint.put(nonexist, destkey, CONTENT.length(), 1, null, body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

    //Both source and dest bucket not found
    try {
      when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          nonexist + "/" + urlEncode(keyName));
      objectEndpoint.put(nonexist, destkey, CONTENT.length(), 1, null, body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

    // source key not found
    try {
      when(headers.getHeaderString(COPY_SOURCE_HEADER)).thenReturn(
          bucketName + "/" + urlEncode(nonexist));
      objectEndpoint.put("nonexistent", keyName, CONTENT.length(), 1,
          null, body);
      fail("test copy object failed");
    } catch (OS3Exception ex) {
      Assert.assertTrue(ex.getCode().contains("NoSuchBucket"));
    }

  }

  @Test
  public void testInvalidStorageType() throws IOException {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);
    keyName = "sourceKey";
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("random");

    try {
      objectEndpoint.put(bucketName, keyName,
          CONTENT.length(), 1, null, body);
      fail("testInvalidStorageType");
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.INVALID_ARGUMENT.getErrorMessage(),
          ex.getErrorMessage());
      assertEquals("random", ex.getResource());
    }
  }

  @Test
  public void testEmptyStorageType() throws IOException, OS3Exception {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    ByteArrayInputStream body =
        new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    objectEndpoint.setHeaders(headers);
    keyName = "sourceKey";
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("");

    objectEndpoint.put(bucketName, keyName, CONTENT
            .length(), 1, null, body);
    OzoneKeyDetails key =
        clientStub.getObjectStore().getS3Bucket(bucketName)
            .getKey(keyName);


    //default type is set
    Assert.assertEquals(ReplicationType.RATIS, key.getReplicationType());
  }

  @Test
  public void testDirectoryCreation() throws IOException,
      OS3Exception {
    // GIVEN
    final String path = "dir";
    final long length = 0L;
    final int partNumber = 0;
    final String uploadId = "";
    final InputStream body = null;
    final HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    final ObjectEndpoint objEndpoint = new ObjectEndpoint();
    objEndpoint.setOzoneConfiguration(new OzoneConfiguration());
    objEndpoint.setHeaders(headers);
    final OzoneClient client = Mockito.mock(OzoneClient.class);
    objEndpoint.setClient(client);
    final ObjectStore objectStore = Mockito.mock(ObjectStore.class);
    final OzoneVolume volume = Mockito.mock(OzoneVolume.class);
    final OzoneBucket bucket = Mockito.mock(OzoneBucket.class);
    final ClientProtocol protocol = Mockito.mock(ClientProtocol.class);

    // WHEN
    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getObjectStore().getS3Volume()).thenReturn(volume);
    when(volume.getBucket(bucketName)).thenReturn(bucket);
    when(bucket.getBucketLayout())
        .thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(client.getProxy()).thenReturn(protocol);
    final Response response = objEndpoint.put(bucketName, path, length,
        partNumber, uploadId, body);

    // THEN
    Assertions.assertEquals(HttpStatus.SC_OK, response.getStatus());
    Mockito.verify(protocol).createDirectory(any(), eq(bucketName), eq(path));
  }

  @Test
  public void testDirectoryCreationOverFile() throws IOException {
    // GIVEN
    final String path = "key";
    final long length = 0L;
    final int partNumber = 0;
    final String uploadId = "";
    final ByteArrayInputStream body =
        new ByteArrayInputStream("content".getBytes(UTF_8));
    final HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    final ObjectEndpoint objEndpoint = new ObjectEndpoint();
    objEndpoint.setOzoneConfiguration(new OzoneConfiguration());
    objEndpoint.setHeaders(headers);
    final OzoneClient client = Mockito.mock(OzoneClient.class);
    objEndpoint.setClient(client);
    final ObjectStore objectStore = Mockito.mock(ObjectStore.class);
    final OzoneVolume volume = Mockito.mock(OzoneVolume.class);
    final OzoneBucket bucket = Mockito.mock(OzoneBucket.class);
    final ClientProtocol protocol = Mockito.mock(ClientProtocol.class);

    // WHEN
    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getObjectStore().getS3Volume()).thenReturn(volume);
    when(volume.getBucket(bucketName)).thenReturn(bucket);
    when(bucket.getBucketLayout())
        .thenReturn(BucketLayout.FILE_SYSTEM_OPTIMIZED);
    when(client.getProxy()).thenReturn(protocol);
    doThrow(new OMException(OMException.ResultCodes.FILE_ALREADY_EXISTS))
        .when(protocol)
        .createDirectory(any(), any(), any());

    // THEN
    final OS3Exception exception = Assertions.assertThrows(OS3Exception.class,
        () -> objEndpoint
            .put(bucketName, path, length, partNumber, uploadId, body));
    Assertions.assertEquals("Conflict", exception.getCode());
    Assertions.assertEquals(409, exception.getHttpCode());
    Mockito.verify(protocol, times(1)).createDirectory(any(), any(), any());
  }
}
