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
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.ACCESS_DENIED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_REQUEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.client.BucketArgs;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Testing for PutBucketLifecycleConfiguration.
 */
public class TestS3LifecycleConfigurationPut {

  private OzoneClient clientStub;
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {
    clientStub = new OzoneClientStub();
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
    ObjectStore objectStore = clientStub.getObjectStore();
    BucketArgs bucketArgs = BucketArgs.newBuilder().setOwner("owner").build();
    objectStore.createVolume("s3v");
    objectStore.getS3Volume().createBucket("bucket1", bucketArgs);
  }

  @Test
  public void testLifecycleConfigurationFailWithEmptyBody() throws Exception {
    try {
      bucketEndpoint.put("bucket1", null, "", null, null);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
      assertEquals(MALFORMED_XML.getCode(), ex.getCode());
    }
  }

  @Test
  public void testLifecycleConfigurationFailWithNonExistentBucket()
      throws Exception {
    try {
      bucketEndpoint.put("nonexistentbucket", null, "", null, onePrefix());
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_BUCKET.getCode(), ex.getCode());
    }
  }

  @Test
  public void testPutInvalidLifecycleConfiguration() throws Exception {
    testInvalidLifecycleConfiguration(TestS3LifecycleConfigurationPut::withoutAction,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(TestS3LifecycleConfigurationPut::withoutFilter,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::useDuplicateTagInAndOperator,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::usePrefixTagWithoutAndOperatorInFilter,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::usePrefixAndOperatorCoExistInFilter,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::usePrefixFilterCoExist,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::useAndOperatorOnlyOnePrefix,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::useAndOperatorOnlyOneTag,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
    testInvalidLifecycleConfiguration(this::useEmptyAndOperator,
        HTTP_BAD_REQUEST, INVALID_REQUEST.getCode());
  }

  @Test
  public void testPutLifecycleConfigurationWithoutStatus() throws Exception {
    try {
      bucketEndpoint.put("bucket1", null, "", null, withoutStatus());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
      assertEquals(MALFORMED_XML.getCode(), ex.getCode());
    }
  }

  private void testInvalidLifecycleConfiguration(Supplier<InputStream> inputStream,
      int expectedHttpCode, String expectedErrorCode) throws Exception {
    try {
      bucketEndpoint.put("bucket1", null, "", null, inputStream.get());
      fail("Expected an OS3Exception to be thrown");
    } catch (OS3Exception ex) {
      assertEquals(expectedHttpCode, ex.getHttpCode());
      assertEquals(expectedErrorCode, ex.getCode());
    }
  }

  @Test
  public void testPutInvalidExpirationDateLCC() throws Exception {
    try {
      String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
          ".com/doc/2006-03-01/\">" +
          "<Rule>" +
          "<ID>remove logs after 30 days</ID>" +
          "<Prefix>prefix/</Prefix>" +
          "<Status>Enabled</Status>" +
          "<Expiration><Date>2023-03-03</Date></Expiration>" +
          "</Rule>" +
          "</LifecycleConfiguration>");

      bucketEndpoint.put("bucket1", null, "", null,
          new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_BAD_REQUEST, ex.getHttpCode());
      assertEquals(INVALID_REQUEST.getCode(), ex.getCode());
    }
  }

  @Test
  public void testPutValidLifecycleConfiguration() throws Exception {
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, onePrefix()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, emptyPrefix()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, oneTag()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, twoTagsInAndOperator()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, onePrefixTwoTagsInAndOperator()).getStatus());
    assertEquals(HTTP_OK, bucketEndpoint.put(
        "bucket1", null, "", null, onePrefixTwoTags()).getStatus());
  }

  @Test
  public void testPutLifecycleConfigurationFailsWithNonBucketOwner()
      throws Exception {
    HttpHeaders httpHeaders = Mockito.mock(HttpHeaders.class);
    when(httpHeaders.getHeaderString("x-amz-expected-bucket-owner"))
        .thenReturn("anotheruser");

    try {
      bucketEndpoint.put("bucket1", null, "", httpHeaders, onePrefix());
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_FORBIDDEN, ex.getHttpCode());
      assertEquals(ACCESS_DENIED.getCode(), ex.getCode());
    }
  }

  private static InputStream onePrefix() {
    String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Status>Enabled</Status>" +
        "<Expiration><Days>30</Days></Expiration>" +
        "</Rule>" +
        "</LifecycleConfiguration>");

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream withoutAction() {
    String xml = (
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Status>Enabled</Status>" +
        "</Rule>" +
        "</LifecycleConfiguration>");
    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream withoutStatus() {
    String xml = (
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Expiration><Days>30</Days></Expiration>" +
        "</Rule>" +
        "</LifecycleConfiguration>");
    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }


  private static InputStream withoutFilter() {
    String xml =
            "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream twoTagsInAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream onePrefixTwoTagsInAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix></Prefix>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream onePrefixTwoTags() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix>key-prefix</Prefix>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream emptyPrefix() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Prefix></Prefix>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private static InputStream oneTag() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Tag>" +
            "                 <Key>key1</Key>" +
            "                 <Value>value1</Value>" +
            "             </Tag>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream usePrefixTagWithoutAndOperatorInFilter() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Prefix>key-prefix</Prefix>" +
            "             <Tag>" +
            "                 <Key>key2</Key>" +
            "                 <Value>value1</Value>" +
            "             </Tag>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream usePrefixAndOperatorCoExistInFilter() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <Prefix>key-prefix</Prefix>" +
            "             <And>" +
                "             <Prefix>key-prefix</Prefix>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream useAndOperatorOnlyOnePrefix() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix>key-prefix</Prefix>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream useAndOperatorOnlyOneTag() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Tag>" +
            "                     <Key>key2</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream useEmptyAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream usePrefixFilterCoExist() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Prefix>key-prefix</Prefix>" +
            "         <Filter>" +
            "             <Tag>" +
            "                 <Key>key1</Key>" +
            "                 <Value>value1</Value>" +
            "             </Tag>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }

  private InputStream useDuplicateTagInAndOperator() {
    String xml =
        "<LifecycleConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">" +
            "     <Rule>" +
            "         <Expiration>" +
            "             <Date>2044-01-19T00:00:00+00:00</Date>" +
            "         </Expiration>" +
            "         <ID>12334</ID>" +
            "         <Filter>" +
            "             <And>" +
            "                 <Prefix>key-prefix</Prefix>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value1</Value>" +
            "                 </Tag>" +
            "                 <Tag>" +
            "                     <Key>key1</Key>" +
            "                     <Value>value2</Value>" +
            "                 </Tag>" +
            "             </And>" +
            "         </Filter>" +
            "         <Status>Enabled</Status>" +
            "     </Rule>" +
            "</LifecycleConfiguration>";

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }



}
