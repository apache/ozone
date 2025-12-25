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

import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NOT_IMPLEMENTED;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_KEY;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
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
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
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
  void setup() throws IOException, OS3Exception {
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


    ByteArrayInputStream body =
        new ByteArrayInputStream("".getBytes(UTF_8));

    objectEndpoint.put(BUCKET_NAME, KEY_NAME, 0, 1, null, null, null, body);
  }

  @Test
  public void testPutObjectTaggingWithEmptyBody() throws Exception {
    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, 0, 1, null, "", null,
          null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
      assertEquals(MALFORMED_XML.getCode(), ex.getCode());
    }
  }

  @Test
  public void testPutValidObjectTagging() throws Exception {
    assertEquals(HTTP_OK, objectEndpoint.put(BUCKET_NAME, KEY_NAME, 0, 1, null,
         "", null, twoTags()).getStatus());
    OzoneKeyDetails keyDetails =
        clientStub.getObjectStore().getS3Bucket(BUCKET_NAME).getKey(KEY_NAME);
    assertEquals(2, keyDetails.getTags().size());
    assertEquals("val1", keyDetails.getTags().get("tag1"));
    assertEquals("val2", keyDetails.getTags().get("tag2"));
  }

  @Test
  public void testPutInvalidObjectTagging() throws Exception {
    testInvalidObjectTagging(this::emptyBody, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidObjectTagging(this::invalidXmlStructure, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidObjectTagging(this::noTagSet, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidObjectTagging(this::emptyTags, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidObjectTagging(this::tagKeyNotSpecified, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
    testInvalidObjectTagging(this::tagValueNotSpecified, HTTP_BAD_REQUEST, MALFORMED_XML.getCode());
  }

  private void testInvalidObjectTagging(Supplier<InputStream> inputStream,
                                        int expectedHttpCode, String expectedErrorCode) throws Exception {
    try {
      objectEndpoint.put(BUCKET_NAME, KEY_NAME, 0, 1, null, "", null,
          inputStream.get());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(expectedHttpCode, ex.getHttpCode());
      assertEquals(expectedErrorCode, ex.getCode());
    }
  }

  @Test
  public void testPutObjectTaggingNoKeyFound() throws Exception {
    try {
      objectEndpoint.put(BUCKET_NAME, "nonexistent", 0, 1,
          null, "", null, twoTags());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_KEY.getCode(), ex.getCode());
    }
  }

  @Test
  public void testPutObjectTaggingNoBucketFound() throws Exception {
    try {
      objectEndpoint.put("nonexistent", "nonexistent", 0, 1,
          null, "", null, twoTags());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
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

    try {
      endpoint.put("fsoBucket", "dir/", 0, 1, null, "",
          null, twoTags());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_IMPLEMENTED, ex.getHttpCode());
      assertEquals(NOT_IMPLEMENTED.getCode(), ex.getCode());
    }
  }

  private InputStream emptyBody() {
    return null;
  }

  private InputStream invalidXmlStructure() {
    String xml =
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "   </Ta" +
            "Tagging>";

    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }

  private InputStream twoTags() {
    String xml =
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

    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }

  private InputStream noTagSet() {
    String xml =
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "</Tagging>";
    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }

  private InputStream emptyTags() {
    String xml =
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "   </TagSet>" +
            "</Tagging>";

    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }

  public InputStream tagKeyNotSpecified() {
    String xml =
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Value>val1</Value>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";

    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }

  public InputStream tagValueNotSpecified() {
    String xml =
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Key>tag1</Key>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";

    return new ByteArrayInputStream(xml.getBytes(UTF_8));
  }
}

