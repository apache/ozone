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

import javax.ws.rs.core.HttpHeaders;

import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;

import org.apache.commons.lang3.RandomStringUtils;
import static org.junit.Assert.assertEquals;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.mockito.Mockito.when;

/**
 * This class test HeadBucket functionality.
 */
public class TestRootList {

  private String volumeName = "vol1";
  private OzoneClientStub clientStub;
  private ObjectStore objectStoreStub;
  private OzoneVolume volumeStub;
  private RootEndpoint rootEndpoint;

  @Before
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();
    objectStoreStub = clientStub.getObjectStore();
    objectStoreStub.createVolume("s3key");
    volumeStub = objectStoreStub.getVolume("s3key");

    // Create HeadBucket and setClient to OzoneClientStub
    rootEndpoint = new RootEndpoint();
    rootEndpoint.setClient(clientStub);
  }

  @Test
  public void testListBucket() throws Exception {
    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    when(headers.getHeaderString("Authorization")).thenReturn("AWS key:secret");

    // List operation should success even there is no bucket.
    ListBucketResponse response = rootEndpoint.get(headers);
    assertEquals(0, response.getBucketsNum());

    String bucketBaseName = "bucket-";
    for(int i = 0; i < 10; i++) {
      volumeStub.createBucket(
          bucketBaseName + RandomStringUtils.randomNumeric(3));
    }
    response = rootEndpoint.get(headers);
    assertEquals(10, response.getBucketsNum());
  }

}
