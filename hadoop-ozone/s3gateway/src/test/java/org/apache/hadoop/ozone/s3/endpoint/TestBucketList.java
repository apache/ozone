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

import java.io.IOException;

import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Testing basic object list browsing.
 * Note: delimiter with '/' will call shallow list logic,
 * just list immediate subdir of prefix.
 */
public class TestBucketList {

  @Test
  public void listRoot() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient client = createClientWithKeys("file1", "dir1/file2");

    getBucket.setClient(client);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, 100, "",
                null, null, null, null, null)
            .getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(1, getBucketResponse.getContents().size());
    assertEquals("file1",
        getBucketResponse.getContents().get(0).getKey().getName());

  }

  @Test
  public void listDir() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient client = createClientWithKeys("dir1/file2", "dir1/dir2/file2");

    getBucket.setClient(client);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, 100,
            "dir1", null, null, null, null, null).getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(0, getBucketResponse.getContents().size());

  }

  @Test
  public void listSubDir() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket
            .get("b1", "/", null, null, 100, "dir1/", null,
                null, null, null, null)
            .getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/dir2/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(1, getBucketResponse.getContents().size());
    assertEquals("dir1/file2",
        getBucketResponse.getContents().get(0).getKey().getName());

  }


  @Test
  public void listWithPrefixAndDelimiter() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, 100,
            "dir1", null, null, null, null, null).getEntity();

    assertEquals(3, getBucketResponse.getCommonPrefixes().size());

  }

  @Test
  public void listWithPrefixAndDelimiter1() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, 100,
            "", null, null, null, null, null).getEntity();

    assertEquals(3, getBucketResponse.getCommonPrefixes().size());
    assertEquals("file2", getBucketResponse.getContents().get(0)
        .getKey().getName());

  }

  @Test
  public void listWithPrefixAndDelimiter2() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, 100, "dir1bh",
            null, "dir1/dir2/file2", null, null, null).getEntity();

    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

  }

  @Test
  public void listWithPrefixAndEmptyStrDelimiter()
      throws OS3Exception, IOException {
    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/", "dir1/dir2/", "dir1/dir2/file1",
          "dir1/dir2/file2");

    getBucket.setClient(ozoneClient);

    // Should behave the same if delimiter is null
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "", null, null, 100, "dir1/",
          null, null, null, null, null).getEntity();

    assertEquals(0, getBucketResponse.getCommonPrefixes().size());
    assertEquals(4, getBucketResponse.getContents().size());
    assertEquals("dir1/",
        getBucketResponse.getContents().get(0).getKey().getName());
    assertEquals("dir1/dir2/",
        getBucketResponse.getContents().get(1).getKey().getName());
    assertEquals("dir1/dir2/file1",
        getBucketResponse.getContents().get(2).getKey().getName());
    assertEquals("dir1/dir2/file2",
        getBucketResponse.getContents().get(3).getKey().getName());

  }

  @Test
  public void listWithContinuationToken() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    getBucket.setClient(ozoneClient);

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", null, null, null, maxKeys,
            "", null, null, null, null, null).getEntity();

    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getContents().size());

    // 2nd time
    String continueToken = getBucketResponse.getNextToken();
    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", null, null, null, maxKeys,
            "", continueToken, null, null, null, null).getEntity();
    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getContents().size());


    continueToken = getBucketResponse.getNextToken();

    //3rd time
    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", null, null, null, maxKeys,
            "", continueToken, null, null, null, null).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(1, getBucketResponse.getContents().size());

  }

  @Test
  public void listWithContinuationTokenDirBreak()
      throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys(
            "test/dir1/file1",
            "test/dir1/file2",
            "test/dir1/file3",
            "test/dir2/file4",
            "test/dir2/file5",
            "test/dir2/file6",
            "test/dir3/file7",
            "test/file8");

    getBucket.setClient(ozoneClient);

    int maxKeys = 2;

    ListObjectResponse getBucketResponse;

    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, maxKeys,
            "test/", null, null, null, null, null).getEntity();

    assertEquals(0, getBucketResponse.getContents().size());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());
    assertEquals("test/dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());
    assertEquals("test/dir2/",
        getBucketResponse.getCommonPrefixes().get(1).getPrefix().getName());

    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, maxKeys,
            "test/", getBucketResponse.getNextToken(), null, null, null,
            null).getEntity();
    assertEquals(1, getBucketResponse.getContents().size());
    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("test/dir3/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());
    assertEquals("test/file8",
        getBucketResponse.getContents().get(0).getKey().getName());

  }

  /**
   * This test is with prefix and delimiter and verify continuation-token
   * behavior.
   */
  @Test
  public void listWithContinuationToken1() throws OS3Exception, IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file1", "dir1bh/file1",
            "dir1bha/file1", "dir0/file1", "dir2/file1");

    getBucket.setClient(ozoneClient);

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, maxKeys,
            "dir", null, null, null, null, null).getEntity();

    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

    // 2nd time
    String continueToken = getBucketResponse.getNextToken();
    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, maxKeys,
            "dir", continueToken, null, null, null, null).getEntity();
    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

    //3rd time
    continueToken = getBucketResponse.getNextToken();
    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", "/", null, null, maxKeys,
            "dir", continueToken, null, null, null, null).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(1, getBucketResponse.getCommonPrefixes().size());

  }

  @Test
  public void listWithContinuationTokenFail() throws IOException {

    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "dir1", "dir2", "dir3");

    getBucket.setClient(ozoneClient);

    OS3Exception e = assertThrows(OS3Exception.class, () -> getBucket.get("b1",
        "/", null, null, 2, "dir", "random", null, null, null, null)
        .getEntity(), "listWithContinuationTokenFail");
    assertEquals("random", e.getResource());
    assertEquals("Invalid Argument", e.getErrorMessage());
  }


  @Test
  public void testStartAfter() throws IOException, OS3Exception {
    BucketEndpoint getBucket = new BucketEndpoint();

    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file1", "dir1bh/file1",
            "dir1bha/file1", "dir0/file1", "dir2/file1");

    getBucket.setClient(ozoneClient);

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", null, null, null, 1000,
            null, null, null, null, null, null).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(5, getBucketResponse.getContents().size());

    //As our list output is sorted, after seeking to startAfter, we shall
    // have 4 keys.
    String startAfter = "dir0/file1";

    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", null, null, null,
            1000, null, null, startAfter, null, null, null).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(4, getBucketResponse.getContents().size());

    getBucketResponse =
        (ListObjectResponse) getBucket.get("b1", null, null, null,
            1000, null, null, "random", null, null, null).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(0, getBucketResponse.getContents().size());


  }

  @Test
  public void testEncodingType() throws IOException, OS3Exception {
    /*
    * OP1 -> Create key "data=1970" and "data==1970" in a bucket
    * OP2 -> List Object, if encodingType == url the result will be like blow:

        <?xml version="1.0" encoding="UTF-8"?>
          <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
              ...
              <Prefix>data%3D</Prefix>
              <StartAfter>data%3D</StartAfter>
              <Delimiter>%3D</Delimiter>
              <EncodingType>url</EncodingType>
              ...
              <Contents>
                  <Key>data%3D1970</Key>
                  ....
              </Contents>
              <CommonPrefixes>
                  <Prefix>data%3D%3D</Prefix>
              </CommonPrefixes>
          </ListBucketResult>

      if encodingType == null , the = will not be encoded to "%3D"
    * */

    BucketEndpoint getBucket = new BucketEndpoint();
    OzoneClient ozoneClient =
        createClientWithKeys("data=1970", "data==1970");
    getBucket.setClient(ozoneClient);

    String delimiter = "=";
    String prefix = "data=";
    String startAfter = "data=";
    String encodingType = ENCODING_TYPE;

    ListObjectResponse response = (ListObjectResponse) getBucket.get(
        "b1", delimiter, encodingType, null, 1000, prefix,
        null, startAfter, null, null, null).getEntity();

    // Assert encodingType == url.
    // The Object name will be encoded by ObjectKeyNameAdapter
    // if encodingType == url
    assertEncodingTypeObject(delimiter, encodingType, response.getDelimiter());
    assertEncodingTypeObject(prefix, encodingType, response.getPrefix());
    assertEncodingTypeObject(startAfter, encodingType,
        response.getStartAfter());
    assertNotNull(response.getCommonPrefixes());
    assertNotNull(response.getContents());
    assertEncodingTypeObject(prefix + delimiter, encodingType,
        response.getCommonPrefixes().get(0).getPrefix());
    assertEquals(encodingType,
        response.getContents().get(0).getKey().getEncodingType());

    response = (ListObjectResponse) getBucket.get(
        "b1", delimiter, null, null, 1000, prefix,
        null, startAfter, null, null, null).getEntity();

    // Assert encodingType == null.
    // The Object name will not be encoded by ObjectKeyNameAdapter
    // if encodingType == null
    assertEncodingTypeObject(delimiter, null, response.getDelimiter());
    assertEncodingTypeObject(prefix, null, response.getPrefix());
    assertEncodingTypeObject(startAfter, null, response.getStartAfter());
    assertNotNull(response.getCommonPrefixes());
    assertNotNull(response.getContents());
    assertEncodingTypeObject(prefix + delimiter, null,
        response.getCommonPrefixes().get(0).getPrefix());
    assertNull(response.getContents().get(0).getKey().getEncodingType());

  }

  @Test
  public void testEncodingTypeException() throws IOException {
    BucketEndpoint getBucket = new BucketEndpoint();
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("b1");
    getBucket.setClient(client);

    OS3Exception e = assertThrows(OS3Exception.class, () -> getBucket.get(
        "b1", null, "unSupportType", null, 1000, null,
        null, null, null, null, null).getEntity());
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), e.getCode());
  }

  private void assertEncodingTypeObject(
      String exceptName, String exceptEncodingType, EncodingTypeObject object) {
    assertEquals(exceptName, object.getName());
    assertEquals(exceptEncodingType, object.getEncodingType());
  }

  private OzoneClient createClientWithKeys(String... keys) throws IOException {
    OzoneClient client = new OzoneClientStub();

    client.getObjectStore().createS3Bucket("b1");
    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");
    for (String key : keys) {
      bucket.createKey(key, 0).close();
    }
    return client;
  }
}
