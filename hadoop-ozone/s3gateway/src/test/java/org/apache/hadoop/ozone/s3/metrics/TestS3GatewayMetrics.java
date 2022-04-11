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
package org.apache.hadoop.ozone.s3.metrics;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.endpoint.BucketEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.ObjectEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.RootEndpoint;
import org.apache.hadoop.ozone.s3.endpoint.TestBucketAcl;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for {@link S3GatewayMetrics}.
 */
public class TestS3GatewayMetrics {

  private String bucketName = OzoneConsts.BUCKET;
  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;
  private RootEndpoint rootEndpoint;
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;
  private HttpHeaders headers;
  private static final String ACL_MARKER = "acl";
  private S3GatewayMetrics metrics;


  @Before
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);

    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);

    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);

    keyEndpoint = new ObjectEndpoint();
    keyEndpoint.setClient(clientStub);
    keyEndpoint.setOzoneConfiguration(new OzoneConfiguration());

    headers = Mockito.mock(HttpHeaders.class);
    metrics = bucketEndpoint.getMetrics();
  }

  @Test
  public void testHeadBucket() throws Exception {

    long oriMetric = metrics.getHeadBucketSuccess();

    bucketEndpoint.head(bucketName);

    long curMetric = metrics.getHeadBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testListBucket() throws Exception {

    long oriMetric = metrics.getListS3BucketsSuccess();

    rootEndpoint.get().getEntity();

    long curMetric = metrics.getListS3BucketsSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testHeadObject() throws Exception {
    String value = RandomStringUtils.randomAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length, ReplicationType.RATIS,
        ReplicationFactor.ONE, new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    long oriMetric = metrics.getHeadKeySuccess();

    keyEndpoint.head(bucketName, "key1");

    long curMetric = metrics.getHeadKeySuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetBucketSuccess() throws Exception {
    long oriMetric = metrics.getGetBucketSuccess();

    clientStub = createClientWithKeys("file1");
    bucketEndpoint.setClient(clientStub);
    bucketEndpoint.get(bucketName, null,
        null, null, 1000, null,
        null, "random", null,
        null, null).getEntity();

    long curMetric = metrics.getGetBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetBucketFailure() throws Exception {
    long oriMetric = metrics.getGetBucketFailure();

    try {
      // Searching for a bucket that does not exist
      bucketEndpoint.get("newBucket", null,
          null, null, 1000, null,
          null, "random", null,
          null, null);
      fail();
    } catch (OS3Exception e) {
    }

    long curMetric = metrics.getGetBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketSuccess() throws Exception {

    long oriMetric = metrics.getCreateBucketSuccess();

    bucketEndpoint.put(bucketName, null,
        null, null);
    long curMetric = metrics.getCreateBucketSuccess();

    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testCreateBucketFailure() throws Exception {
    // Creating an error by trying to create a bucket that already exists
    long oriMetric = metrics.getCreateBucketFailure();

    bucketEndpoint.put(bucketName, null, null, null);

    long curMetric = metrics.getCreateBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteBucketSuccess() throws Exception {
    long oriMetric = metrics.getDeleteBucketSuccess();

    bucketEndpoint.delete(bucketName);

    long curMetric = metrics.getDeleteBucketSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testDeleteBucketFailure() throws Exception {
    long oriMetric = metrics.getDeleteBucketFailure();
    bucketEndpoint.delete(bucketName);
    try {
      // Deleting a bucket that does not exist will result in delete failure
      bucketEndpoint.delete(bucketName);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }

    long curMetric = metrics.getDeleteBucketFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclSuccess() throws Exception {
    long oriMetric = metrics.getGetAclSuccess();

    Response response =
        bucketEndpoint.get(bucketName, null, null,
            null, 0, null, null,
            null, null, "acl", null);
    long curMetric = metrics.getGetAclSuccess();
    assertEquals(HTTP_OK, response.getStatus());
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testGetAclFailure() throws Exception {
    long oriMetric = metrics.getGetAclFailure();
    try {
      // Failing the getACL endpoint by applying ACL on a non-Existent Bucket
      bucketEndpoint.get("random_bucket", null,
          null, null, 0, null,
          null, null, null, "acl", null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getCode(), ex.getCode());
      assertEquals(S3ErrorTable.NO_SUCH_BUCKET.getErrorMessage(),
          ex.getErrorMessage());
    }
    long curMetric = metrics.getGetAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutAclSuccess() throws Exception {
    long oriMetric = metrics.getPutAclSuccess();

    clientStub.getObjectStore().createS3Bucket("b1");
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");

    bucketEndpoint.put("b1", ACL_MARKER, headers, inputBody);
    inputBody.close();
    long curMetric = metrics.getPutAclSuccess();
    assertEquals(1L, curMetric - oriMetric);
  }

  @Test
  public void testPutAclFailure() throws Exception {
    // Failing the putACL endpoint by applying ACL on a non-Existent Bucket
    long oriMetric = metrics.getPutAclFailure();

    clientStub.getObjectStore().createS3Bucket("b1");
    InputStream inputBody = TestBucketAcl.class.getClassLoader()
        .getResourceAsStream("userAccessControlList.xml");

    try {
      bucketEndpoint.put("unknown_bucket", ACL_MARKER, headers, inputBody);
      fail();
    } catch (OS3Exception ex) {
    }
    inputBody.close();
    long curMetric = metrics.getPutAclFailure();
    assertEquals(1L, curMetric - oriMetric);
  }

  private OzoneClient createClientWithKeys(String... keys) throws IOException {
    OzoneBucket bkt = clientStub.getObjectStore().getS3Bucket(bucketName);
    for (String key : keys) {
      bkt.createKey(key, 0).close();
    }
    return clientStub;
  }
}