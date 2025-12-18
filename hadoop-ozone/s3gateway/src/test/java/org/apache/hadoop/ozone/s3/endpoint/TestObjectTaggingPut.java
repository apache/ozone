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
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
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

  @BeforeEach
  void setup() throws Exception {
    OzoneConfiguration config = new OzoneConfiguration();

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create bucket
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");

    // Create PutObject and setClient to OzoneClientStub
    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .setConfig(config)
        .setHeaders(headers)
        .build();

    assertSucceeds(() -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, ""));
  }

  @Test
  public void testPutObjectTaggingWithEmptyBody() {
    objectEndpoint.queryParamsForTest().set(S3Consts.QueryParams.TAGGING, "");
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, ""));
  }

  @Test
  public void testPutValidObjectTagging() throws Exception {
    objectEndpoint.queryParamsForTest().set(S3Consts.QueryParams.TAGGING, "");
    assertSucceeds(() -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, twoTags()));
    OzoneKeyDetails keyDetails =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    assertEquals(2, keyDetails.getTags().size());
    assertEquals("val1", keyDetails.getTags().get("tag1"));
    assertEquals("val2", keyDetails.getTags().get("tag2"));
  }

  @Test
  public void testPutInvalidObjectTagging() {
    objectEndpoint.queryParamsForTest().set(S3Consts.QueryParams.TAGGING, "");
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, emptyBody()));
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, invalidXmlStructure()));
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, noTagSet()));
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, emptyTags()));
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, tagKeyNotSpecified()));
    assertErrorResponse(MALFORMED_XML, () -> put(objectEndpoint, BUCKET_NAME, KEY_NAME, tagValueNotSpecified()));
  }

  @Test
  public void testPutObjectTaggingNoKeyFound() {
    objectEndpoint.queryParamsForTest().set(S3Consts.QueryParams.TAGGING, "");
    assertErrorResponse(NO_SUCH_KEY, () -> put(objectEndpoint, BUCKET_NAME, "nonexistent", tagValueNotSpecified()));
  }

  @Test
  public void testPutObjectTaggingNoBucketFound() {
    objectEndpoint.queryParamsForTest().set(S3Consts.QueryParams.TAGGING, "");
    assertErrorResponse(NO_SUCH_BUCKET, () -> put(objectEndpoint, "nonexistent", "any", twoTags()));
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
    Map<String, String> twoTagsMap = new HashMap<>();
    twoTagsMap.put("tag1", "val1");
    twoTagsMap.put("tag2", "val2");


    doThrow(new OMException("PutObjectTagging is not currently supported for FSO directory",
        ResultCodes.NOT_SUPPORTED_OPERATION)).when(mockBucket).putObjectTagging("dir/", twoTagsMap);

    endpoint.queryParamsForTest().set(S3Consts.QueryParams.TAGGING, "");
    assertErrorResponse(NOT_IMPLEMENTED, () -> put(endpoint, "fsoBucket", "dir/", twoTags()));
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

