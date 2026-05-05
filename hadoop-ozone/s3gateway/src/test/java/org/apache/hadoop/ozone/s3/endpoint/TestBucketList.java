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

import static org.apache.hadoop.ozone.s3.S3GatewayConfigKeys.OZONE_S3G_LIST_MAX_KEYS_LIMIT;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointBuilder.newBucketEndpointBuilder;
import static org.apache.hadoop.ozone.s3.util.S3Consts.ENCODING_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;

/**
 * Testing basic object list browsing.
 * Note: delimiter with '/' will call shallow list logic,
 * just list immediate subdir of prefix.
 */
public class TestBucketList {

  @Test
  public void listRoot() throws OS3Exception, IOException {
    OzoneClient client = createClientWithKeys("file1", "dir1/file2");
    BucketEndpoint endpoint = newBucketEndpointBuilder()
        .setClient(client)
        .build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(1, getBucketResponse.getContents().size());
    assertEquals("file1",
        getBucketResponse.getContents().get(0).getKey().getName());
  }

  @Test
  public void listDir() throws OS3Exception, IOException {
    OzoneClient client = createClientWithKeys("dir1/file2", "dir1/dir2/file2");
    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(client).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(0, getBucketResponse.getContents().size());
  }

  @Test
  public void listSubDir() throws OS3Exception, IOException {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1/");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/dir2/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(1, getBucketResponse.getContents().size());
    assertEquals("dir1/file2",
        getBucketResponse.getContents().get(0).getKey().getName());
  }

  @Test
  public void listObjectOwner() throws OS3Exception, IOException {
    UserGroupInformation user1 = UserGroupInformation
        .createUserForTesting("user1", new String[] {"user1"});
    UserGroupInformation user2 = UserGroupInformation
        .createUserForTesting("user2", new String[] {"user2"});

    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("b1");
    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");

    UserGroupInformation.setLoginUser(user1);
    bucket.createKey("key1", 0).close();
    UserGroupInformation.setLoginUser(user2);
    bucket.createKey("key2", 0).close();

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(client).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "key");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(2, getBucketResponse.getContents().size());
    assertEquals(user1.getShortUserName(),
        getBucketResponse.getContents().get(0).getOwner().getDisplayName());
    assertEquals(user2.getShortUserName(),
        getBucketResponse.getContents().get(1).getOwner().getDisplayName());
  }

  @Test
  public void listWithPrefixAndDelimiter() throws OS3Exception, IOException {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(3, getBucketResponse.getCommonPrefixes().size());
  }

  @Test
  public void listWithPrefixAndDelimiter1() throws OS3Exception, IOException {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(3, getBucketResponse.getCommonPrefixes().size());
    assertEquals("file2", getBucketResponse.getContents().get(0)
        .getKey().getName());
  }

  @Test
  public void listWithPrefixAndDelimiter2() throws OS3Exception, IOException {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1bh");
    endpoint.queryParamsForTest().set(QueryParams.START_AFTER, "dir1/dir2/file2");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(2, getBucketResponse.getCommonPrefixes().size());
  }

  @Test
  public void listWithPrefixAndEmptyStrDelimiter()
      throws OS3Exception, IOException {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/", "dir1/dir2/", "dir1/dir2/file1",
          "dir1/dir2/file2");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    // Should behave the same if delimiter is null
    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1/");
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

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
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "file2");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    endpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, maxKeys);
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getContents().size());

    // 2nd time
    String value1 = getBucketResponse.getNextToken();
    endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value1);
    getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();
    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getContents().size());

    //3rd time
    String value = getBucketResponse.getNextToken();
    endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value);
    getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(1, getBucketResponse.getContents().size());
  }

  @Test
  public void listWithContinuationTokenDirBreak()
      throws OS3Exception, IOException {
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

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    int maxKeys = 2;

    ListObjectResponse getBucketResponse;

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "test/");
    endpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, maxKeys);
    getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertEquals(0, getBucketResponse.getContents().size());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());
    assertEquals("test/dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());
    assertEquals("test/dir2/",
        getBucketResponse.getCommonPrefixes().get(1).getPrefix().getName());

    String value = getBucketResponse.getNextToken();
    endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value);
    getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();
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
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file1", "dir1bh/file1",
            "dir1bha/file1", "dir0/file1", "dir2/file1");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir");
    endpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, maxKeys);
    ListObjectResponse getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

    // 2nd time
    String value1 = getBucketResponse.getNextToken();
    endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value1);
    getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();
    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

    //3rd time
    String value = getBucketResponse.getNextToken();
    endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value);
    getBucketResponse = (ListObjectResponse) endpoint.get("b1").getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
  }

  @Test
  public void listWithContinuationTokenFail() throws IOException {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file2", "dir1/dir2/file2", "dir1bh/file",
            "dir1bha/file2", "dir1", "dir2", "dir3");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir");
    endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, "random");
    endpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 2);
    OS3Exception e = assertThrows(OS3Exception.class, () -> endpoint.get("b1").getEntity());
    assertEquals("random", e.getResource());
    assertEquals("Invalid Argument", e.getErrorMessage());
  }

  @Test
  public void testStartAfter() throws IOException, OS3Exception {
    OzoneClient ozoneClient =
        createClientWithKeys("dir1/file1", "dir1bh/file1",
            "dir1bha/file1", "dir0/file1", "dir2/file1");

    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) endpoint.get("b1").getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(5, getBucketResponse.getContents().size());

    //As our list output is sorted, after seeking to startAfter, we shall
    // have 4 keys.
    String startAfter = "dir0/file1";

    endpoint.queryParamsForTest().set(QueryParams.START_AFTER, startAfter);
    getBucketResponse =
        (ListObjectResponse) endpoint.get("b1").getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(4, getBucketResponse.getContents().size());

    endpoint.queryParamsForTest().set(QueryParams.START_AFTER, "random");
    getBucketResponse =
        (ListObjectResponse) endpoint.get("b1").getEntity();

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

    OzoneClient ozoneClient =
        createClientWithKeys("data=1970", "data==1970");
    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(ozoneClient).build();

    String delimiter = "=";
    String prefix = "data=";
    String startAfter = "data=";
    String encodingType = ENCODING_TYPE;

    endpoint.queryParamsForTest().set(QueryParams.DELIMITER, delimiter);
    endpoint.queryParamsForTest().set(QueryParams.PREFIX, prefix);
    endpoint.queryParamsForTest().set(QueryParams.ENCODING_TYPE, encodingType);
    endpoint.queryParamsForTest().set(QueryParams.START_AFTER, startAfter);
    ListObjectResponse response = (ListObjectResponse) endpoint.get("b1").getEntity();

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

    endpoint.queryParamsForTest().unset(QueryParams.ENCODING_TYPE);
    response = (ListObjectResponse) endpoint.get("b1").getEntity();

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
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("b1");
    BucketEndpoint endpoint = newBucketEndpointBuilder().setClient(client).build();

    endpoint.queryParamsForTest().set(QueryParams.ENCODING_TYPE, "unSupportType");
    OS3Exception e = assertThrows(OS3Exception.class, () -> endpoint.get("b1").getEntity());
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), e.getCode());
  }

  @Test
  public void testListObjectsWithNegativeMaxKeys() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("bucket");
    BucketEndpoint bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .build();

    // maxKeys < 0 should throw InvalidArgument
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, -1);
    OS3Exception e1 = assertThrows(OS3Exception.class, () -> bucketEndpoint.get("bucket"));
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), e1.getCode());
  }

  @Test
  public void testListObjectsWithZeroMaxKeys() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("bucket");
    BucketEndpoint bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .build();

    // maxKeys = 0, should return empty list and not throw.
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 0);
    ListObjectResponse response = (ListObjectResponse) bucketEndpoint.get("bucket").getEntity();

    assertEquals(0, response.getContents().size());
    assertFalse(response.isTruncated());
  }

  @Test
  public void testListObjectsWithZeroMaxKeysInNonEmptyBucket() throws Exception {
    OzoneClient client = createClientWithKeys("file1", "file2", "file3", "file4", "file5");
    BucketEndpoint bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .build();

    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 0);
    ListObjectResponse response = (ListObjectResponse) bucketEndpoint.get("b1").getEntity();

    // Should return empty list and not throw.
    assertEquals(0, response.getContents().size());
    assertFalse(response.isTruncated());

    bucketEndpoint.queryParamsForTest().unset(QueryParams.MAX_KEYS);
    ListObjectResponse fullResponse = (ListObjectResponse) bucketEndpoint.get("b1").getEntity();
    assertEquals(5, fullResponse.getContents().size());
  }

  @Test
  public void testListObjectsRespectsConfiguredMaxKeysLimit() throws Exception {
    // Arrange: Create a bucket with 1001 keys
    String[] keys = IntStream.range(0, 1001).mapToObj(i -> "file" + i).toArray(String[]::new);
    OzoneClient client = createClientWithKeys(keys);

    // Arrange: Set the max-keys limit in the configuration
    OzoneConfiguration config = new OzoneConfiguration();
    final String configuredMaxKeysLimit = "900";
    config.set(OZONE_S3G_LIST_MAX_KEYS_LIMIT, configuredMaxKeysLimit);

    // Arrange: Build and initialize the BucketEndpoint with the config
    BucketEndpoint bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .setConfig(config)
        .build();

    // Assert: Ensure the config value is correctly set in the endpoint
    assertEquals(configuredMaxKeysLimit,
        bucketEndpoint.getOzoneConfiguration().get(OZONE_S3G_LIST_MAX_KEYS_LIMIT));

    // Act: Request more keys than the configured max-keys limit
    final int requestedMaxKeys = Integer.parseInt(configuredMaxKeysLimit) + 1;
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, requestedMaxKeys);
    ListObjectResponse response = (ListObjectResponse) bucketEndpoint.get("b1").getEntity();

    // Assert: The number of returned keys should be capped at the configured limit
    assertEquals(Integer.parseInt(configuredMaxKeysLimit), response.getContents().size());
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
