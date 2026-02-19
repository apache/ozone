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

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertStatus;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.delete;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;

/**
 * Test delete object.
 */
public class TestObjectDelete {

  @Test
  void testDelete() throws IOException, OS3Exception {
    //GIVEN
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket("b1");

    OzoneBucket bucket =
        client.getObjectStore().getS3Bucket("b1");

    bucket.createKey("key1", 0).close();

    ObjectEndpoint rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .build();

    //WHEN
    assertStatus(HttpStatus.SC_NO_CONTENT, () -> delete(rest, "b1", "key1"));

    //THEN
    assertFalse(bucket.listKeys("").hasNext(),
        "Bucket Should not contain any key after delete");
  }
}
