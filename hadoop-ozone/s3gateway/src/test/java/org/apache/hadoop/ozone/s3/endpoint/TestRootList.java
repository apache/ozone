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

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class test HeadBucket functionality.
 */
public class TestRootList {

  private OzoneClient clientStub;
  private RootEndpoint rootEndpoint;

  private static final String DEFAULT_VOLUME = OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;

  @BeforeEach
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create HeadBucket and setClient to OzoneClientStub
    rootEndpoint = EndpointBuilder.newRootEndpointBuilder()
        .setClient(clientStub)
        .build();

    clientStub.getObjectStore().createVolume(DEFAULT_VOLUME);

  }

  @Test
  public void testListBucket() throws Exception {

    // List operation should succeed even there is no bucket.
    ListBucketResponse response =
        (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(0, response.getBucketsNum());

    String bucketBaseName = "bucket-" + getClass().getName();
    for (int i = 0; i < 10; i++) {
      clientStub.getObjectStore().createS3Bucket(bucketBaseName + i);
    }
    response = (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(10, response.getBucketsNum());
    assertEquals("root", response.getOwner().getDisplayName());
    assertEquals(S3Owner.DEFAULT_S3OWNER_ID, response.getOwner().getId());
  }

}
