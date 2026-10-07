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
import java.io.StringWriter;
import java.util.stream.IntStream;
import javax.xml.bind.JAXB;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.commontypes.EncodingTypeObject;
import org.apache.hadoop.ozone.s3.commontypes.ObjectKeyNameAdapter;
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

  private static final String BUCKET_NAME = "b1";

  private OzoneClient client;
  private BucketEndpoint bucketEndpoint;

  private void setup(String... keys) throws IOException {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);
    createKeys(client, keys);
    bucketEndpoint = newBucketEndpointBuilder().setClient(client).build();
  }

  @Test
  public void listRoot() throws OS3Exception, IOException {
    setup("file1", "dir1/file2");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertNotNull(getBucketResponse.getPrefix());
    assertEquals("", getBucketResponse.getPrefix().getName());

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(1, getBucketResponse.getContents().size());
    assertEquals("file1",
        getBucketResponse.getContents().get(0).getKey().getName());
  }

  @Test
  public void listDir() throws OS3Exception, IOException {
    setup("dir1/file2", "dir1/dir2/file2");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
    assertEquals("dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());

    assertEquals(0, getBucketResponse.getContents().size());
  }

  @Test
  public void listSubDir() throws OS3Exception, IOException {
    setup("dir1/file2", "dir1/dir2/file2", "dir1bh/file", "dir1bha/file2");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1/");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

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

    setup();
    OzoneBucket bucket = client.getObjectStore().getS3Bucket(BUCKET_NAME);

    UserGroupInformation.setLoginUser(user1);
    bucket.createKey("key1", 0).close();
    UserGroupInformation.setLoginUser(user2);
    bucket.createKey("key2", 0).close();

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "key");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(2, getBucketResponse.getContents().size());
    assertEquals(user1.getShortUserName(),
        getBucketResponse.getContents().get(0).getOwner().getDisplayName());
    assertEquals(user2.getShortUserName(),
        getBucketResponse.getContents().get(1).getOwner().getDisplayName());
  }

  @Test
  public void listWithDelimiterAndPrefixMatchingNoKeys() throws OS3Exception, IOException {
    setup("b/a/r", "b/a/c", "b/a/g", "g");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "d");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "/");
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(0, response.getContents().size());
    assertEquals(0, response.getCommonPrefixes().size());
  }

  @Test
  public void listWithPrefixAndDelimiter() throws OS3Exception, IOException {
    setup("dir1/file2", "dir1/dir2/file2", "dir1bh/file", "dir1bha/file2", "file2");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(3, getBucketResponse.getCommonPrefixes().size());
  }

  @Test
  public void listWithPrefixAndDelimiter1() throws OS3Exception, IOException {
    setup("dir1/file2", "dir1/dir2/file2", "dir1bh/file", "dir1bha/file2", "file2");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(3, getBucketResponse.getCommonPrefixes().size());
    assertEquals("file2", getBucketResponse.getContents().get(0)
        .getKey().getName());
  }

  @Test
  public void listWithPrefixAndDelimiter2() throws OS3Exception, IOException {
    setup("dir1/file2", "dir1/dir2/file2", "dir1bh/file", "dir1bha/file2", "file2");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1bh");
    bucketEndpoint.queryParamsForTest().set(QueryParams.START_AFTER, "dir1/dir2/file2");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(2, getBucketResponse.getCommonPrefixes().size());
  }

  @Test
  public void listWithPrefixAndEmptyStrDelimiter()
      throws OS3Exception, IOException {
    setup("dir1/", "dir1/dir2/", "dir1/dir2/file1", "dir1/dir2/file2");

    // Should behave the same if delimiter is null
    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir1/");
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(0, getBucketResponse.getCommonPrefixes().size());
    assertEquals(4, getBucketResponse.getContents().size());
    assertNull(getBucketResponse.getDelimiter());
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
    setup("dir1/file2", "dir1/dir2/file2", "dir1bh/file", "dir1bha/file2", "file2");

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, maxKeys);
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getContents().size());

    // 2nd time
    String value1 = getBucketResponse.getNextToken();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value1);
    getBucketResponse = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();
    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getContents().size());

    //3rd time
    String value = getBucketResponse.getNextToken();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value);
    getBucketResponse = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(1, getBucketResponse.getContents().size());
  }

  @Test
  public void listWithContinuationTokenDirBreak()
      throws OS3Exception, IOException {
    setup(
        "test/dir1/file1",
        "test/dir1/file2",
        "test/dir1/file3",
        "test/dir2/file4",
        "test/dir2/file5",
        "test/dir2/file6",
        "test/dir3/file7",
        "test/file8");

    int maxKeys = 2;

    ListObjectResponse getBucketResponse;

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "test/");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, maxKeys);
    getBucketResponse = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(0, getBucketResponse.getContents().size());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());
    assertEquals("test/dir1/",
        getBucketResponse.getCommonPrefixes().get(0).getPrefix().getName());
    assertEquals("test/dir2/",
        getBucketResponse.getCommonPrefixes().get(1).getPrefix().getName());

    String value = getBucketResponse.getNextToken();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value);
    getBucketResponse = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();
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
    setup("dir1/file1", "dir1bh/file1", "dir1bha/file1", "dir0/file1", "dir2/file1");

    int maxKeys = 2;
    // As we have 5 keys, with max keys 2 we should call list 3 times.

    // First time
    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, maxKeys);
    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

    // 2nd time
    String value1 = getBucketResponse.getNextToken();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value1);
    getBucketResponse = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();
    assertTrue(getBucketResponse.isTruncated());
    assertEquals(2, getBucketResponse.getCommonPrefixes().size());

    //3rd time
    String value = getBucketResponse.getNextToken();
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, value);
    getBucketResponse = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(1, getBucketResponse.getCommonPrefixes().size());
  }

  @Test
  public void listWithContinuationTokenFail() throws IOException {
    setup("dir1/file2", "dir1/dir2/file2", "dir1bh/file", "dir1bha/file2", "dir1", "dir2", "dir3");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "dir");
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, "random");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 2);
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get(BUCKET_NAME).getEntity());
    assertEquals("random", e.getResource());
    assertEquals("Invalid Argument", e.getErrorMessage());
  }

  @Test
  public void testStartAfter() throws IOException, OS3Exception {
    setup("dir1/file1", "dir1bh/file1", "dir1bha/file1", "dir0/file1", "dir2/file1");

    ListObjectResponse getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(5, getBucketResponse.getContents().size());

    //As our list output is sorted, after seeking to startAfter, we shall
    // have 4 keys.
    String startAfter = "dir0/file1";

    bucketEndpoint.queryParamsForTest().set(QueryParams.START_AFTER, startAfter);
    getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(getBucketResponse.isTruncated());
    assertEquals(4, getBucketResponse.getContents().size());

    bucketEndpoint.queryParamsForTest().set(QueryParams.START_AFTER, "random");
    getBucketResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

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

      if encodingType == null , the = will not be encoded to "%3D
    * */

    setup("data=1970", "data==1970");

    String delimiter = "=";
    String prefix = "data=";
    String startAfter = "data=";
    String encodingType = ENCODING_TYPE;

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, delimiter);
    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, prefix);
    bucketEndpoint.queryParamsForTest().set(QueryParams.ENCODING_TYPE, encodingType);
    bucketEndpoint.queryParamsForTest().set(QueryParams.START_AFTER, startAfter);
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

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

    bucketEndpoint.queryParamsForTest().unset(QueryParams.ENCODING_TYPE);
    response = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

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
    setup();

    bucketEndpoint.queryParamsForTest().set(QueryParams.ENCODING_TYPE, "unSupportType");
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get(BUCKET_NAME).getEntity());
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), e.getCode());
  }

  @Test
  public void testListObjectsWithNonIntegerMaxKeys() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("bucket");
    bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .build();

    bucketEndpoint.queryParamsForTest().set(QueryParams.MAX_KEYS, "blah");
    OS3Exception e = assertThrows(OS3Exception.class, () -> bucketEndpoint.get("bucket"));
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), e.getCode());
  }

  @Test
  public void testListObjectsWithNegativeMaxKeys() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("bucket");
    bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .build();

    // maxKeys < 0 should throw InvalidArgument
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, -1);
    OS3Exception e1 = assertThrows(OS3Exception.class, () -> bucketEndpoint.get("bucket"));
    assertEquals(S3ErrorTable.INVALID_ARGUMENT.getCode(), e1.getCode());
  }

  @Test
  public void testListObjectsWithZeroMaxKeys() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("bucket");
    bucketEndpoint = newBucketEndpointBuilder()
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
    setup("file1", "file2", "file3", "file4", "file5");

    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 0);
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    // Should return empty list and not throw.
    assertEquals(0, response.getContents().size());
    assertFalse(response.isTruncated());

    bucketEndpoint.queryParamsForTest().unset(QueryParams.MAX_KEYS);
    ListObjectResponse fullResponse =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();
    assertEquals(5, fullResponse.getContents().size());
  }

  @Test
  public void testListObjectsRespectsConfiguredMaxKeysLimit() throws Exception {
    // Arrange: Create a bucket with 1001 keys
    String[] keys = IntStream.range(0, 1001).mapToObj(i -> "file" + i).toArray(String[]::new);
    setup(keys);

    // Arrange: Set the max-keys limit in the configuration
    OzoneConfiguration config = new OzoneConfiguration();
    final String configuredMaxKeysLimit = "900";
    config.set(OZONE_S3G_LIST_MAX_KEYS_LIMIT, configuredMaxKeysLimit);

    // Arrange: Build and initialize the BucketEndpoint with the config
    bucketEndpoint = newBucketEndpointBuilder()
        .setClient(client)
        .setConfig(config)
        .build();

    // Assert: Ensure the config value is correctly set in the endpoint
    assertEquals(configuredMaxKeysLimit,
        bucketEndpoint.getOzoneConfiguration().get(OZONE_S3G_LIST_MAX_KEYS_LIMIT));

    // Act: Request more keys than the configured max-keys limit
    final int requestedMaxKeys = Integer.parseInt(configuredMaxKeysLimit) + 1;
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, requestedMaxKeys);
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    // Assert: The number of returned keys should be capped at the configured limit
    assertEquals(Integer.parseInt(configuredMaxKeysLimit), response.getContents().size());
  }

  @Test
  public void testListObjectsUrlEncodingUsesPercentTwentyForSpaces()
      throws Exception {
    setup("foo+1/bar", "foo/bar/xyzzy", "quux ab/thud", "asdf+b");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "/");
    bucketEndpoint.queryParamsForTest().set(QueryParams.ENCODING_TYPE, ENCODING_TYPE);
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    ObjectKeyNameAdapter adapter = new ObjectKeyNameAdapter();
    assertEquals("asdf%2Bb", adapter.marshal(response.getContents().get(0).getKey()));
    assertEquals(3, response.getCommonPrefixes().size());
    assertEquals("foo%2B1/", adapter.marshal(response.getCommonPrefixes().get(0).getPrefix()));
    assertEquals("foo/", adapter.marshal(response.getCommonPrefixes().get(1).getPrefix()));
    assertEquals("quux%20ab/", adapter.marshal(response.getCommonPrefixes().get(2).getPrefix()));
  }

  @Test
  public void testListObjectsOmitsDelimiterWhenEmpty() throws Exception {
    setup("bar", "baz", "cab", "foo");

    bucketEndpoint.queryParamsForTest().set(QueryParams.DELIMITER, "");
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertNull(response.getDelimiter());
    assertEquals(4, response.getContents().size());
    assertEquals(0, response.getCommonPrefixes().size());
  }

  private void assertEncodingTypeObject(
      String exceptName, String exceptEncodingType, EncodingTypeObject object) {
    assertEquals(exceptName, object.getName());
    assertEquals(exceptEncodingType, object.getEncodingType());
  }

  /**
   * An empty continuation token must be treated as no token: list from the
   * start, not truncated, and echo the empty token back (AWS S3 semantics).
   */
  @Test
  public void listWithEmptyContinuationToken() throws OS3Exception, IOException {
    setup("bar", "baz", "foo", "quxx");

    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, "");
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(response.isTruncated());
    assertEquals(4, response.getContents().size());
    // Echoed back verbatim (empty), so botocore populates ContinuationToken=''.
    assertEquals("", response.getContinueToken());
  }

  /**
   * A supplied continuation token must be echoed back in the response so that
   * clients can read response['ContinuationToken'].
   */
  @Test
  public void listEchoesContinuationToken() throws OS3Exception, IOException {
    setup("bar", "baz", "foo", "quxx");

    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 1);
    ListObjectResponse first = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();
    assertTrue(first.isTruncated());
    String token = first.getNextToken();
    assertNotNull(token);

    bucketEndpoint.queryParamsForTest().unset(QueryParams.MAX_KEYS);
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, token);
    ListObjectResponse second = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(second.isTruncated());
    // The request continuation token is echoed back verbatim.
    assertEquals(token, second.getContinueToken());
    assertEquals(3, second.getContents().size());
  }

  /**
   * With both StartAfter and a continuation token, the token drives the
   * listing position while both StartAfter and ContinuationToken are echoed.
   */
  @Test
  public void listContinuationTokenWithStartAfter()
      throws OS3Exception, IOException {
    setup("bar", "baz", "foo", "quxx");

    bucketEndpoint.queryParamsForTest().set(QueryParams.PREFIX, "");
    bucketEndpoint.queryParamsForTest().set(QueryParams.START_AFTER, "bar");
    bucketEndpoint.queryParamsForTest().setInt(QueryParams.MAX_KEYS, 1);
    ListObjectResponse first = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();
    assertTrue(first.isTruncated());
    String token = first.getNextToken();
    assertNotNull(token);

    bucketEndpoint.queryParamsForTest().unset(QueryParams.MAX_KEYS);
    bucketEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN, token);
    ListObjectResponse second = (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertFalse(second.isTruncated());
    assertEquals(token, second.getContinueToken());
    assertEquals("bar", second.getStartAfter().getName());
    assertEquals(2, second.getContents().size());
  }

  /**
   * The echoed request continuation token must be serialized as the AWS S3
   * element name {@code ContinuationToken} (not {@code continueToken}).
   */
  @Test
  public void continuationTokenXmlElementName() throws Exception {
    ListObjectResponse response = new ListObjectResponse();
    response.setContinueToken("token-value");

    StringWriter writer = new StringWriter();
    JAXB.marshal(response, writer);
    String xml = writer.toString();

    assertTrue(xml.contains("<ContinuationToken>token-value</ContinuationToken>"),
        "expected <ContinuationToken> element, got: " + xml);
    assertFalse(xml.contains("continueToken"),
        "response must not use the non-AWS <continueToken> element");
  }

  @Test
  public void listObjectOwnerOmittedForListV2ByDefault() throws OS3Exception, IOException {
    setup("key1", "key2");

    bucketEndpoint.queryParamsForTest().setInt(QueryParams.LIST_TYPE, 2);
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertEquals(2, response.getContents().size());
    assertNull(response.getContents().get(0).getOwner());
    assertNull(response.getContents().get(1).getOwner());
  }

  @Test
  public void listObjectOwnerOmittedForListV2WhenFetchOwnerFalse() throws OS3Exception, IOException {
    setup("key1");

    bucketEndpoint.queryParamsForTest().setInt(QueryParams.LIST_TYPE, 2);
    bucketEndpoint.queryParamsForTest().set(QueryParams.FETCH_OWNER, "false");
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertNull(response.getContents().get(0).getOwner());
  }

  @Test
  public void listObjectOwnerIncludedForListV2WhenFetchOwnerTrue() throws OS3Exception, IOException {
    setup("key1");

    bucketEndpoint.queryParamsForTest().setInt(QueryParams.LIST_TYPE, 2);
    bucketEndpoint.queryParamsForTest().set(QueryParams.FETCH_OWNER, "true");
    ListObjectResponse response =
        (ListObjectResponse) bucketEndpoint.get(BUCKET_NAME).getEntity();

    assertNotNull(response.getContents().get(0).getOwner());
  }

  private void createKeys(OzoneClient ozoneClient, String... keys) throws IOException {
    OzoneBucket bucket = ozoneClient.getObjectStore().getS3Bucket(BUCKET_NAME);
    for (String key : keys) {
      bucket.createKey(key, 0).close();
    }
  }
}
