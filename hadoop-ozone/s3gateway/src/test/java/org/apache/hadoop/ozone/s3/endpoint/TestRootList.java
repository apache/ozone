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

import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.ozone.s3.SignatureProcessor;
import org.junit.Before;
import org.junit.Test;

/**
 * This class test HeadBucket functionality.
 */
public class TestRootList {

  private OzoneClient clientStub;
  private RootEndpoint rootEndpoint;

  @Before
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create HeadBucket and setClient to OzoneClientStub
    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);


  }

  @Test
  public void testListBucket() throws Exception {

    rootEndpoint.setSignatureProcessor(new SignatureProcessor() {
      @Override
      public String getStringToSign() {
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
    // List operation should succeed even there is no bucket.
    ListBucketResponse response =
        (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(0, response.getBucketsNum());

    String bucketBaseName = "bucket-" + getClass().getName();
    for(int i = 0; i < 10; i++) {
      clientStub.getObjectStore().createS3Bucket(bucketBaseName + i);
    }
    response = (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(10, response.getBucketsNum());
  }

}
