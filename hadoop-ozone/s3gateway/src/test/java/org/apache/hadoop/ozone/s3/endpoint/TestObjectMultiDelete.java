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

import static java.util.Collections.singleton;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteRequest.DeleteObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test object multi delete.
 */
public class TestObjectMultiDelete {

  public static final String BUCKET_NAME = "b1";
  private static OzoneBucket bucket;
  private static BucketEndpoint rest;

  @BeforeAll
  public static void setUp() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    bucket = client.getObjectStore().getS3Bucket(BUCKET_NAME);
    bucket.createKey("key1", 0).close();
    bucket.createKey("key2", 0).close();
    bucket.createKey("key3", 0).close();

    rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();
  }

  @Test
  public void delete() throws IOException, OS3Exception, JAXBException {
    //GIVEN
    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(new DeleteObject("key2"));
    mdr.getObjects().add(new DeleteObject("key4"));

    //WHEN
    MultiDeleteResponse response = rest.multiDelete(BUCKET_NAME, "", mdr, null);

    //THEN
    Set<String> keysAtTheEnd = Sets.newHashSet(bucket.listKeys("")).stream()
        .map(OzoneKey::getName)
        .collect(Collectors.toSet());

    Set<String> expectedResult = new HashSet<>();
    expectedResult.add("key3");

    //THEN
    assertEquals(expectedResult, keysAtTheEnd);
    assertEquals(3, response.getDeletedObjects().size());
    assertEquals(0, response.getErrors().size());
  }

  @Test
  public void deleteQuiet() throws IOException, OS3Exception, JAXBException {
    //GIVEN
    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.setQuiet(true);
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(new DeleteObject("key2"));
    mdr.getObjects().add(new DeleteObject("key4"));

    //WHEN
    MultiDeleteResponse response = rest.multiDelete(BUCKET_NAME, "", mdr, null);

    //THEN
    Set<String> keysAtTheEnd = Sets.newHashSet(bucket.listKeys("")).stream()
        .map(OzoneKey::getName)
        .collect(Collectors.toSet());

    //THEN
    assertEquals(singleton("key3"), keysAtTheEnd);
    assertEquals(0, response.getDeletedObjects().size());
    assertEquals(0, response.getErrors().size());
  }

  @Test
  public void testBucketOwnerCondition() {
    HttpHeaders headers = mock(HttpHeaders.class);

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(new DeleteObject("key2"));

    // use wrong bucket owner header to test access denied
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("wrongOwner");

    OS3Exception exception =
        assertThrows(OS3Exception.class, () -> rest.multiDelete(BUCKET_NAME, "", mdr, headers));

    assertEquals(ACCESS_DENIED.getMessage(), exception.getMessage());

    // use correct bucket owner header to pass bucket owner condition verification
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER))
        .thenReturn("defaultOwner");

    assertDoesNotThrow(() -> rest.multiDelete(BUCKET_NAME, "", mdr, headers));
  }
}
