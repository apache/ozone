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

import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.getBucketTagging;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.putBucketTagging;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_TAG_SET;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.S3Tagging.Tag;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for GetBucketTagging.
 */
public class TestBucketTaggingGet {

  private static final String BUCKET_NAME = "b1";
  private BucketEndpoint rest;

  private static String twoTagsBody() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag><Key>tag1</Key><Value>value1</Value></Tag>" +
            "      <Tag><Key>tag2</Key><Value>value2</Value></Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  @BeforeEach
  public void init() throws Exception {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("UNSIGNED-PAYLOAD");
    Mockito.when(headers.getHeaderString(STORAGE_CLASS_HEADER))
        .thenReturn("STANDARD");

    rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    assertSucceeds(() -> putBucketTagging(rest, BUCKET_NAME, twoTagsBody()));
  }

  @Test
  public void testGetBucketTagging() throws IOException, OS3Exception {
    Response response = getBucketTagging(rest, BUCKET_NAME);

    assertEquals(HTTP_OK, response.getStatus());
    S3Tagging s3Tagging = (S3Tagging) response.getEntity();
    assertNotNull(s3Tagging);
    assertNotNull(s3Tagging.getTagSet());
    assertEquals(2, s3Tagging.getTagSet().getTags().size());
    for (Tag tag : s3Tagging.getTagSet().getTags()) {
      if (tag.getKey().equals("tag1")) {
        assertEquals("value1", tag.getValue());
      } else if (tag.getKey().equals("tag2")) {
        assertEquals("value2", tag.getValue());
      } else {
        fail("Unknown tag found");
      }
    }
  }

  @Test
  public void testGetBucketTaggingNoTagSet() throws Exception {
    OzoneClient client = new OzoneClientStub();
    String emptyBucket = "empty-tags-bucket";
    client.getObjectStore().createS3Bucket(emptyBucket);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("UNSIGNED-PAYLOAD");
    Mockito.when(headers.getHeaderString(STORAGE_CLASS_HEADER))
        .thenReturn("STANDARD");

    BucketEndpoint endpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    assertErrorResponse(NO_SUCH_TAG_SET, () -> getBucketTagging(endpoint, emptyBucket));
  }

  @Test
  public void testGetBucketTaggingNoBucketFound() {
    assertErrorResponse(NO_SUCH_BUCKET, () -> getBucketTagging(rest, "nonexistent"));
  }
}
