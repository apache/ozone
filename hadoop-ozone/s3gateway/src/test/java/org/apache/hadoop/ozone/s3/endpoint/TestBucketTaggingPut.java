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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.putBucketTagging;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_TAG;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_BUCKET_NUM_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_KEY_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.TAG_VALUE_LENGTH_LIMIT;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for PutBucketTagging.
 */
public class TestBucketTaggingPut {

  private static final String BUCKET_NAME = "b1";
  private BucketEndpoint rest;

  @BeforeEach
  public void init() throws OS3Exception, IOException {
    OzoneClient client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(BUCKET_NAME);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("UNSIGNED-PAYLOAD");
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .build();
  }

  @Test
  public void testPutBucketTaggingWithEmptyBody() {
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, ""));
  }

  @Test
  public void testPutValidBucketTagging() throws Exception {
    assertSucceeds(() -> putBucketTagging(rest, BUCKET_NAME, twoTagsBody()));
    Map<String, String> tags =
        rest.getClient().getObjectStore().getS3Bucket(BUCKET_NAME).getBucketTagging();
    assertThat(tags).containsExactlyInAnyOrderEntriesOf(
        ImmutableMap.of("tag1", "value1", "tag2", "value2"));
  }

  @Test
  public void testPutInvalidBucketTaggingXml() {
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, null));
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, invalidXmlStructure()));
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, noTagSet()));
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, emptyTags()));
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, tagKeyNotSpecified()));
    assertErrorResponse(MALFORMED_XML, () -> putBucketTagging(rest, BUCKET_NAME, tagValueNotSpecified()));
  }

  @Test
  public void testPutBucketTaggingDuplicateKeys() {
    assertErrorResponse(INVALID_TAG, () -> putBucketTagging(rest, BUCKET_NAME, duplicateTagKeys()));
  }

  @Test
  public void testPutBucketTaggingAwsPrefixKey() {
    assertErrorResponse(INVALID_TAG, () -> putBucketTagging(rest, BUCKET_NAME, awsPrefixTag()));
  }

  @Test
  public void testPutBucketTaggingKeyTooLong() {
    String longKey = StringUtils.repeat("k", TAG_KEY_LENGTH_LIMIT + 1);
    assertErrorResponse(INVALID_TAG, () -> putBucketTagging(rest, BUCKET_NAME, singleTag(longKey, "v")));
  }

  @Test
  public void testPutBucketTaggingValueTooLong() {
    String longValue = StringUtils.repeat("v", TAG_VALUE_LENGTH_LIMIT + 1);
    assertErrorResponse(INVALID_TAG, () -> putBucketTagging(rest, BUCKET_NAME, singleTag("k", longValue)));
  }

  @Test
  public void testPutBucketTaggingExceedsLimit() {
    StringBuilder tags = new StringBuilder("<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\"><TagSet>");
    for (int i = 0; i < TAG_BUCKET_NUM_LIMIT + 1; i++) {
      tags.append("<Tag><Key>tag").append(i).append("</Key><Value>value").append(i)
          .append("</Value></Tag>");
    }
    tags.append("</TagSet></Tagging>");
    assertErrorResponse(INVALID_TAG, () -> putBucketTagging(rest, BUCKET_NAME, tags.toString()));
  }

  @Test
  public void testPutBucketTaggingNoBucketFound() {
    assertErrorResponse(NO_SUCH_BUCKET, () -> putBucketTagging(rest, "nonexistent", twoTagsBody()));
  }

  private static String twoTagsBody() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Key>tag1</Key>" +
            "         <Value>value1</Value>" +
            "      </Tag>" +
            "      <Tag>" +
            "         <Key>tag2</Key>" +
            "         <Value>value2</Value>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  private static String singleTag(String key, String value) {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag>" +
            "         <Key>" + key + "</Key>" +
            "         <Value>" + value + "</Value>" +
            "      </Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  private static String duplicateTagKeys() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "      <Tag><Key>dup</Key><Value>v1</Value></Tag>" +
            "      <Tag><Key>dup</Key><Value>v2</Value></Tag>" +
            "   </TagSet>" +
            "</Tagging>";
  }

  private static String awsPrefixTag() {
    return singleTag("aws:reserved", "value");
  }

  private static String invalidXmlStructure() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet>" +
            "   </Ta" +
            "Tagging>";
  }

  private static String noTagSet() {
    return "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\"></Tagging>";
  }

  private static String emptyTags() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet></TagSet>" +
            "</Tagging>";
  }

  private static String tagKeyNotSpecified() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet><Tag><Value>val1</Value></Tag></TagSet>" +
            "</Tagging>";
  }

  private static String tagValueNotSpecified() {
    return
        "<Tagging xmlns=\"" + S3Consts.S3_XML_NAMESPACE + "\">" +
            "   <TagSet><Tag><Key>tag1</Key></Tag></TagSet>" +
            "</Tagging>";
  }
}
