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

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;

import org.apache.hadoop.ozone.s3.SignatureProcessor;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.apache.hadoop.ozone.s3.AWSSignatureProcessor.DATE_FORMATTER;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_HEADER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

/**
 * This class test Create Bucket functionality.
 */
public class TestBucketPut {

  private String bucketName = OzoneConsts.BUCKET;
  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;

  @Before
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create HeadBucket and setClient to OzoneClientStub
    bucketEndpoint = new BucketEndpoint();
    bucketEndpoint.setClient(clientStub);
    bucketEndpoint.setSignatureProcessor(new SignatureProcessor() {
      @Override
      public String getStringToSign() throws Exception {
        return null;
      }

      @Override
      public String getSignature() {
        return null;
      }

      @Override
      public String getAwsAccessId() {
        return OzoneConsts.OZONE;
      }

      @Override
      public Exception getException() {
        return null;
      }
    });
  }

  @Test
  public void testBucketFailWithAuthHeaderMissing() throws Exception {

    try {
      bucketEndpoint.put(bucketName, null);
    } catch (OS3Exception ex) {
      Assert.assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      Assert.assertEquals(MALFORMED_HEADER.getCode(), ex.getCode());
    }
  }

  @Test
  public void testBucketPut() throws Exception {
    String auth = generateAuthHeader();

    Response response = bucketEndpoint.put(bucketName, null);
    assertEquals(200, response.getStatus());
    assertNotNull(response.getLocation());
  }

  @Test
  public void testBucketFailWithInvalidHeader() throws Exception {
    try {
      bucketEndpoint.put(bucketName, null);
    } catch (OS3Exception ex) {
      Assert.assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      Assert.assertEquals(MALFORMED_HEADER.getCode(), ex.getCode());
    }
  }

  /**
   * Generate dummy auth header.
   * @return auth header.
   */
  private String generateAuthHeader() {
    LocalDate now = LocalDate.now();
    String curDate = DATE_FORMATTER.format(now);
    return  "AWS4-HMAC-SHA256 " +
        "Credential=ozone/" + curDate + "/us-east-1/s3/aws4_request, " +
        "SignedHeaders=host;range;x-amz-date, " +
        "Signature=fe5f80f77d5fa3beca038a248ff027";
  }

}
