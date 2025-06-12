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

import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test delete object.
 */
public class TestObjectDelete {

  private static ObjectEndpoint rest = new ObjectEndpoint();
  private static OzoneBucket bucket;
  private static final String BUCKET_NAME = "b1";
  private static final String KEY = "key1";

  @BeforeAll
  public static void setUp() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);
    bucket = client.getObjectStore().getS3Bucket("b1");
    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .build();
  }

  @Test
  public void delete() throws IOException, OS3Exception {
    //GIVEN
    bucket.createKey(KEY, 0).close();

    //WHEN
    rest.delete(BUCKET_NAME, KEY, null, null);

    //THEN
    assertFalse(bucket.listKeys("").hasNext(),
        "Bucket Should not contain any key after delete");
  }

  @Test
  public void testBucketOwnerCondition() throws Exception {
    HttpHeaders headers = mock(HttpHeaders.class);

    // Use wrong bucket owner header to fail bucket owner condition verification
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("wrongOwner");
    rest.setHeaders(headers);

    OS3Exception exception =
        assertThrows(OS3Exception.class, () -> rest.delete(BUCKET_NAME, KEY, null, null));

    assertEquals(ACCESS_DENIED.getMessage(), exception.getMessage());

    // use correct bucket owner header to pass bucket owner condition verification
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("defaultOwner");

    Response response = rest.delete(BUCKET_NAME, KEY, null, null);

    assertEquals(204, response.getStatus());
  }
}
