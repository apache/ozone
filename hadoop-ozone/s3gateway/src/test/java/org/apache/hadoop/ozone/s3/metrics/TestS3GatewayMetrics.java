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
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;

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
}
