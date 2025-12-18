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
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests for DeleteObjectTagging.
 */
public class TestObjectTaggingDelete {

  private static final String CONTENT = "0123456789";
  private static final String BUCKET_NAME = "b1";
  private static final String KEY_WITH_TAG = "keyWithTag";
  private ObjectEndpoint rest;
  private OzoneClient client;

  @BeforeEach
  public void init() throws OS3Exception, IOException {
    //GIVEN
    OzoneConfiguration config = new OzoneConfiguration();
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setConfig(config)
        .setHeaders(headers)
        .build();

    ByteArrayInputStream body = new ByteArrayInputStream(CONTENT.getBytes(UTF_8));
    // Create a key with object tags
    Mockito.when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");
    rest.put(BUCKET_NAME, KEY_WITH_TAG, CONTENT.length(),
        1, null, null, null, body);
  }

  @Test
  public void testDeleteTagging() throws IOException, OS3Exception {
    Response response = rest.delete(BUCKET_NAME, KEY_WITH_TAG, null,  "");
    assertEquals(HTTP_NO_CONTENT, response.getStatus());

    assertTrue(client.getObjectStore().getS3Bucket(BUCKET_NAME)
        .getKey(KEY_WITH_TAG).getTags().isEmpty());
  }

  @Test
  public void testDeleteTaggingNoKeyFound() throws Exception {
    try {
      rest.delete(BUCKET_NAME, "nonexistent", null,  "");
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_KEY.getCode(), ex.getCode());
    }
  }

  @Test
  public void testDeleteTaggingNoBucketFound() throws Exception {
    try {
      rest.delete("nonexistent", "nonexistent", null,  "");
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }

  @Test
  public void testDeleteObjectTaggingNotImplemented() throws Exception {
    OzoneClient mockClient = mock(OzoneClient.class);
    ObjectStore mockObjectStore = mock(ObjectStore.class);
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    OzoneBucket mockBucket = mock(OzoneBucket.class);

    when(mockClient.getObjectStore()).thenReturn(mockObjectStore);
    when(mockObjectStore.getS3Volume()).thenReturn(mockVolume);
    when(mockVolume.getBucket("fsoBucket")).thenReturn(mockBucket);

    ObjectEndpoint endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(mockClient)
        .build();
    doThrow(new OMException("DeleteObjectTagging is not currently supported for FSO directory",
        ResultCodes.NOT_SUPPORTED_OPERATION)).when(mockBucket).deleteObjectTagging("dir/");

    try {
      endpoint.delete("fsoBucket", "dir/", null, "");
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_IMPLEMENTED, ex.getHttpCode());
      assertEquals(NOT_IMPLEMENTED.getCode(), ex.getCode());
    }
  }
}
