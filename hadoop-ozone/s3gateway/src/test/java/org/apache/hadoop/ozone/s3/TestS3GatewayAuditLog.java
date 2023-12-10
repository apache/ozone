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
package org.apache.hadoop.ozone.s3;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hadoop.hdds.client.ReplicationConfig;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for S3Gateway Audit Log.
 */
public class TestS3GatewayAuditLog {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestS3GatewayAuditLog.class.getName());

  static {
    System.setProperty("log4j.configurationFile", "auditlog.properties");
    System.setProperty("log4j2.contextSelector",
        "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
  }

  private String bucketName = OzoneConsts.BUCKET;
  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;
  private RootEndpoint rootEndpoint;
  private ObjectEndpoint keyEndpoint;
  private OzoneBucket bucket;
  private Map<String, String> parametersMap = new HashMap<>();

  @BeforeEach
  public void setup() throws Exception {

    parametersMap.clear();
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(bucketName);
    bucket = clientStub.getObjectStore().getS3Bucket(bucketName);

    bucketEndpoint = new BucketEndpoint() {
      @Override
      protected Map<String, String> getAuditParameters() {
        return parametersMap;
      }
    };
    bucketEndpoint.setClient(clientStub);

    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);

    keyEndpoint = new ObjectEndpoint() {
      @Override
      protected Map<String, String> getAuditParameters() {
        return parametersMap;
      }
    };
    keyEndpoint.setClient(clientStub);
    keyEndpoint.setOzoneConfiguration(new OzoneConfiguration());

  }

  @AfterAll
  public static void tearDown() {
    File file = new File("audit.log");
    if (FileUtils.deleteQuietly(file)) {
      LOG.info("{} has been deleted as all tests have completed.",
          file.getName());
    } else {
      LOG.info("audit.log could not be deleted.");
    }
  }

  @Test
  public void testHeadBucket() throws Exception {
    parametersMap.put("bucket", "[bucket]");

    bucketEndpoint.head(bucketName);
    String expected = "INFO  | S3GAudit | ? | user=null | ip=null | " +
        "op=HEAD_BUCKET {bucket=[bucket]} | ret=SUCCESS";
    verifyLog(expected);
  }

  @Test
  public void testListBucket() throws Exception {

    rootEndpoint.get().getEntity();
    String expected = "INFO  | S3GAudit | ? | user=null | ip=null | " +
        "op=LIST_S3_BUCKETS {} | ret=SUCCESS";
    verifyLog(expected);
  }

  @Test
  public void testHeadObject() throws Exception {
    String value = RandomStringUtils.randomAlphanumeric(32);
    OzoneOutputStream out = bucket.createKey("key1",
        value.getBytes(UTF_8).length,
        ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
        ReplicationFactor.ONE), new HashMap<>());
    out.write(value.getBytes(UTF_8));
    out.close();

    parametersMap.put("bucket", "[bucket]");
    parametersMap.put("path", "[key1]");

    keyEndpoint.head(bucketName, "key1");
    String expected = "INFO  | S3GAudit | ? | user=null | ip=null | " +
        "op=HEAD_KEY {bucket=[bucket], path=[key1]} | ret=SUCCESS";
    verifyLog(expected);

  }

  private void verifyLog(String expectedString) throws IOException {
    File file = new File("audit.log");
    List<String> lines = FileUtils.readLines(file, (String)null);
    final int retry = 5;
    int i = 0;
    while (lines.isEmpty() && i < retry) {
      lines = FileUtils.readLines(file, (String)null);
      try {
        Thread.sleep(500 * (i + 1));
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        break;
      }
      i++;
    }
    assertEquals(lines.get(0), expectedString);

    //empty the file
    lines.clear();
    FileUtils.writeLines(file, lines, false);
  }

}
