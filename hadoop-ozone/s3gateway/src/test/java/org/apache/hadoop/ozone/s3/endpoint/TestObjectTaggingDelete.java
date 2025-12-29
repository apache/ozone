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

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.deleteTagging;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
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
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = Mockito.mock(HttpHeaders.class);
    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();

    // Create a key with object tags
    Mockito.when(headers.getHeaderString(TAG_HEADER)).thenReturn("tag1=value1&tag2=value2");
    Mockito.when(headers.getHeaderString(X_AMZ_CONTENT_SHA256))
        .thenReturn("mockSignature");
    put(rest, BUCKET_NAME, KEY_WITH_TAG, CONTENT);
  }

  @Test
  public void testDeleteTagging() throws IOException, OS3Exception {
    Response response = deleteTagging(rest, BUCKET_NAME, KEY_WITH_TAG);
    assertEquals(HTTP_NO_CONTENT, response.getStatus());

    assertTrue(client.getObjectStore().getS3Bucket(BUCKET_NAME)
        .getKey(KEY_WITH_TAG).getTags().isEmpty());
  }

  @Test
  public void testDeleteTaggingNoKeyFound() {
    assertErrorResponse(NO_SUCH_KEY, () -> deleteTagging(rest, BUCKET_NAME, "nonexistent"));
  }

  @Test
  public void testDeleteTaggingNoBucketFound() {
    assertErrorResponse(NO_SUCH_BUCKET, () -> deleteTagging(rest, "nonexistent", "any"));
  }

  @Test
  public void testDeleteObjectTaggingNotImplemented() throws Exception {
    OzoneClient mockClient = mock(OzoneClient.class);
    ObjectStore mockObjectStore = mock(ObjectStore.class);
    OzoneVolume mockVolume = mock(OzoneVolume.class);
    OzoneBucket mockBucket = mock(OzoneBucket.class);

    when(mockClient.getObjectStore()).thenReturn(mockObjectStore);
    when(mockObjectStore.getS3Volume()).thenReturn(mockVolume);
    when(mockObjectStore.getClientProxy()).thenReturn(mock(ClientProtocol.class));
    when(mockVolume.getBucket("fsoBucket")).thenReturn(mockBucket);

    ObjectEndpoint endpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(mockClient)
        .build();
    doThrow(new OMException("DeleteObjectTagging is not currently supported for FSO directory",
        ResultCodes.NOT_SUPPORTED_OPERATION)).when(mockBucket).deleteObjectTagging("dir/");

    assertErrorResponse(NOT_IMPLEMENTED, () -> deleteTagging(endpoint, "fsoBucket", "dir/"));
  }
}
