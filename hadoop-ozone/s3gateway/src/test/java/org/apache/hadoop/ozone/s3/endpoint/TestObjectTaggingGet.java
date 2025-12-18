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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.endpoint.S3Tagging.Tag;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for GetObjectTagging.
 */
public class TestObjectTaggingGet {

  private static final String CONTENT = "0123456789";
  private static final String BUCKET_NAME = "b1";
  private static final String KEY_WITH_TAG = "keyWithTag";
  private ObjectEndpoint rest;

  @BeforeEach
  public void init() throws OS3Exception, IOException {
    //GIVEN
    OzoneConfiguration config = new OzoneConfiguration();
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setConfig(config)
        .setHeaders(headers)
        .build();

    ByteArrayInputStream body = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    // Create a key with object tags
    Mockito.when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");
    rest.put(BUCKET_NAME, KEY_WITH_TAG, CONTENT.length(),
        1, null, null, null, body);
  }

  @Test
  public void testGetTagging() throws IOException, OS3Exception {
    //WHEN
    Response response = rest.get(BUCKET_NAME, KEY_WITH_TAG,  0, null, 0, null, "");

    assertEquals(HTTP_OK, response.getStatus());
    S3Tagging s3Tagging = (S3Tagging) response.getEntity();
    assertNotNull(s3Tagging);
    assertNotNull(s3Tagging.getTagSet());
    assertEquals(2, s3Tagging.getTagSet().getTags().size());
    for (Tag tag: s3Tagging.getTagSet().getTags()) {
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
  public void testGetTaggingNoKeyFound() throws Exception {
    try {
      rest.get(BUCKET_NAME, "nonexistent",  0, null, 0, null, "");
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_KEY.getCode(), ex.getCode());
    }
  }

  @Test
  public void testGetTaggingNoBucketFound() throws Exception {
    try {
      rest.get("nonexistent", "nonexistent", 0, null, 0, null, "");
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }
}
