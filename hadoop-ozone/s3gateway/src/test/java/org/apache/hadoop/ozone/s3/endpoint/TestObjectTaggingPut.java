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

import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertSucceeds;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.put;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.putTagging;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKeyDetails;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for PutObjectTagging.
 */
public class TestObjectTaggingPut {

  private OzoneClient clientStub;
  private ObjectEndpoint objectEndpoint;

  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key=value/1";
  private static final Map<String, String> TAGS = ImmutableMap.of("tag1", "val1", "tag2", "val2");

  @BeforeEach
  void setup() throws Exception {
    clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setHeaders(headers)
        .build();

    assertSucceeds(() -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, ""));
  }

  @Test
  public void testPutObjectTaggingWithEmptyBody() {
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, ""));
  }

  @Test
  public void testPutValidObjectTagging() throws Exception {
    assertSucceeds(() -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, twoTags()));
    OzoneKeyDetails keyDetails =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    assertThat(keyDetails.getTags())
        .containsExactlyEntriesOf(TAGS);
  }

  @Test
  public void testPutInvalidObjectTagging() {
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, emptyBody()));
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, invalidXmlStructure()));
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, noTagSet()));
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, emptyTags()));
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, tagKeyNotSpecified()));
    assertErrorResponse(MALFORMED_XML, () -> putTagging(objectEndpoint, BUCKET_NAME, KEY_NAME, tagValueNotSpecified()));
  }

  @Test
  public void testPutObjectTaggingNoKeyFound() {
    assertErrorResponse(NO_SUCH_KEY, () -> putTagging(objectEndpoint, BUCKET_NAME, "nonexistent", twoTags()));
  }

  @Test
  public void testPutObjectTaggingNoBucketFound() {
    assertErrorResponse(NO_SUCH_BUCKET, () -> putTagging(objectEndpoint, "nonexistent", "any", twoTags()));
  }

  @Test
  public void testPutObjectTaggingNotImplemented() throws Exception {
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

    doThrow(new OMException("PutObjectTagging is not currently supported for FSO directory",
        ResultCodes.NOT_SUPPORTED_OPERATION)).when(mockBucket).putObjectTagging("dir/", TAGS);

    assertErrorResponse(NOT_IMPLEMENTED, () -> putTagging(endpoint, "fsoBucket", "dir/", twoTags()));
  }

  private String emptyBody() {
    return null;
  }

  private String invalidXmlStructure() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "   </Ta" +
            "Tagging>";
  }

  private String twoTags() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Key>tag1</Key>" +
            "         <Value>val1</Value>" +
            "      </Tag>" +
            "      <Tag>" +
            "         <Key>tag2</Key>" +
            "         <Value>val2</Value>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  private String noTagSet() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "</Tagging>";
  }

  private String emptyTags() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  public String tagKeyNotSpecified() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Value>val1</Value>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  public String tagValueNotSpecified() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Key>tag1</Key>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }
}

