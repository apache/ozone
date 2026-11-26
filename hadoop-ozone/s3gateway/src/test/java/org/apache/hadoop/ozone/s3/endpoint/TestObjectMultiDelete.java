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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BAD_DIGEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.BUCKET_OWNER_MISMATCH;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INTERNAL_ERROR;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.INVALID_DIGEST;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NOT_IMPLEMENTED;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_BUCKET;
import static org.apache.hadoop.ozone.s3.util.S3Consts.EXPECTED_BUCKET_OWNER_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.NULL_VERSION_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteRequest.DeleteObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.DeletedObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test object multi delete.
 */
public class TestObjectMultiDelete {

  @Test
  public void multiDeleteSkipsVolumeAndBucketLookup() throws Exception {
    BucketEndpoint endpoint = newEndpointFor(mock(OzoneBucket.class), new HashMap<>());

    endpoint.multiDelete("b1", "", threeKeyRequest());

    verify(endpoint.getClient().getObjectStore(), never()).getS3Volume();
    verify(endpoint.getClient().getObjectStore().getClientProxy(), never()).getBucketDetails(any(), anyString());
    verify(endpoint.getClient().getObjectStore().getClientProxy())
        .deleteKeys(null, "b1", asList("key1", "key2", "key3"), true);
  }

  @Test
  public void multiDeleteRejectsWrongExpectedOwnerBeforeDeleting() throws Exception {
    BucketEndpoint endpoint = newEndpointWithExpectedOwner("other-owner");

    assertErrorResponse(BUCKET_OWNER_MISMATCH, () -> endpoint.multiDelete("b1", "", threeKeyRequest()));
    verify(endpoint.getClient().getObjectStore().getClientProxy(), never())
        .deleteKeys(any(), anyString(), any(), anyBoolean());
  }

  @Test
  public void multiDeleteWithExpectedOwnerUsesBucketVolume() throws Exception {
    BucketEndpoint endpoint = newEndpointWithExpectedOwner("owner");

    assertThat(endpoint.multiDelete("b1", "", threeKeyRequest()).getDeletedObjects()).hasSize(3);

    verify(endpoint.getClient().getObjectStore(), never()).getS3Volume();
    verify(endpoint.getClient().getObjectStore().getClientProxy())
        .deleteKeys("tenant-volume", "b1", asList("key1", "key2", "key3"), true);
  }

  @Test
  public void multiDeleteAuditsExpectedOwnerBucketLookupFailure() throws Exception {
    Map<String, String> auditParams = new HashMap<>();
    BucketEndpoint endpoint = newEndpointFor(mock(OzoneBucket.class), auditParams);
    when(endpoint.getClient().getObjectStore().getClientProxy().getBucketDetails(isNull(), anyString()))
        .thenThrow(new OMException("Bucket not found", ResultCodes.BUCKET_NOT_FOUND));
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("owner");
    endpoint.setHeaders(headers);
    long failures = endpoint.getMetrics().getDeleteKeyFailure();

    assertErrorResponse(NO_SUCH_BUCKET, () -> endpoint.multiDelete("b1", "", threeKeyRequest()));
    assertEquals("[key1, key2, key3]", auditParams.get("failedDeletes"));
    assertEquals(failures + 1, endpoint.getMetrics().getDeleteKeyFailure());
  }

  @Test
  public void multiDeleteEmptyListRequiresBucket() throws Exception {
    OzoneClient client = new OzoneClientStub();
    BucketEndpoint endpoint = EndpointBuilder.newBucketEndpointBuilder().setClient(client).build();

    assertErrorResponse(NO_SUCH_BUCKET, () -> endpoint.multiDelete("missing", "", new MultiDeleteRequest()));
  }

  @Test
  public void delete() throws IOException, OS3Exception, JAXBException {
    //GIVEN
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(new DeleteObject("key2"));
    mdr.getObjects().add(new DeleteObject("key4"));

    //WHEN
    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

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
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.setQuiet(true);
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(new DeleteObject("key2"));
    mdr.getObjects().add(new DeleteObject("key4"));

    //WHEN
    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

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
  public void multiDeleteRejectsMoreThanMaxKeysPerRequest() throws Exception {
    OzoneClient client = new OzoneClientStub();
    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    for (int i = 0; i < S3Consts.S3_DELETE_OBJECTS_MAX_KEYS + 1; i++) {
      mdr.getObjects().add(new DeleteObject("key-" + i));
    }

    assertErrorResponse(MALFORMED_XML, () -> rest.multiDelete("b1", "", mdr));
  }

  @Test
  public void multiDeleteAllowsMaxKeysPerRequest() throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initTestData(client);
    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.setQuiet(true);
    for (int i = 0; i < S3Consts.S3_DELETE_OBJECTS_MAX_KEYS; i++) {
      mdr.getObjects().add(new DeleteObject("missing-" + i));
    }

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);
    assertEquals(0, response.getDeletedObjects().size());
    assertEquals(0, response.getErrors().size());

    assertEquals(3, Sets.newHashSet(bucket.listKeys("")).size());
  }

  @Test
  public void multiDeleteAuditsOnlyFailedKeys() throws Exception {
    OzoneBucket bucket = mock(OzoneBucket.class);
    Map<String, ErrorInfo> undeletedKeys = new HashMap<>();
    undeletedKeys.put("key2", new ErrorInfo("ACCESS_DENIED", "ACL check failed"));
    undeletedKeys.put("key3", new ErrorInfo(ResultCodes.KEY_NOT_FOUND.name(), "Key does not exist"));
    when(bucket.deleteKeys(any(), anyBoolean())).thenReturn(undeletedKeys);

    Map<String, String> auditParams = new HashMap<>();
    MultiDeleteResponse response =
        newEndpointFor(bucket, auditParams).multiDelete("b1", "", threeKeyRequest());

    assertEquals(asList("key1", "key3"), response.getDeletedObjects().stream()
        .map(DeletedObject::getKey)
        .collect(Collectors.toList()));
    assertEquals(1, response.getErrors().size());
    assertEquals("key2", response.getErrors().get(0).getKey());
    assertEquals("[key2]", auditParams.get("failedDeletes"));
  }

  @Test
  public void multiDeleteAuditsAllKeysWhenDeleteFails() throws Exception {
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.deleteKeys(any(), anyBoolean())).thenThrow(new IOException("Ozone Manager is unavailable"));

    Map<String, String> auditParams = new HashMap<>();
    BucketEndpoint endpoint = newEndpointFor(bucket, auditParams);

    assertErrorResponse(INTERNAL_ERROR, () -> endpoint.multiDelete("b1", "", threeKeyRequest()));
    assertEquals("[key1, key2, key3]", auditParams.get("failedDeletes"));
  }

  @Test
  public void multiDeleteWithVersionIdIsNotImplemented() throws Exception {
    final OzoneClient client = new OzoneClientStub();
    final OzoneBucket bucket = initTestData(client);
    final BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    final DeleteObject versioned = new DeleteObject("key2");
    versioned.setVersionId("nonexistent");
    final MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(versioned);

    assertErrorResponse(NOT_IMPLEMENTED, () -> rest.multiDelete("b1", "", mdr));

    // the whole request is rejected, including the keys without VersionId
    final Set<String> keysAtTheEnd = Sets.newHashSet(bucket.listKeys("")).stream()
        .map(OzoneKey::getName)
        .collect(Collectors.toSet());
    assertEquals(Sets.newHashSet("key1", "key2", "key3"), keysAtTheEnd);
  }

  @Test
  public void multiDeleteWithNullVersionIdDeletesCurrentObject() throws Exception {
    final OzoneClient client = new OzoneClientStub();
    final OzoneBucket bucket = initTestData(client);
    final BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder().setClient(client).build();

    final DeleteObject nullVersion = new DeleteObject("key2");
    nullVersion.setVersionId(NULL_VERSION_ID);
    final MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(nullVersion);

    final MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertThat(response.getErrors()).isEmpty();
    // VersionId is only returned for the keys it was requested for
    assertThat(response.getDeletedObjects())
        .extracting(DeletedObject::getKey, DeletedObject::getVersionId)
        .containsExactly(tuple("key1", null), tuple("key2", NULL_VERSION_ID));
    assertThat(bucket.listKeys("")).toIterable().extracting(OzoneKey::getName).containsExactly("key3");
  }

  @Test
  public void multiDeleteReturnsVersionIdOfFailedKey() throws Exception {
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.deleteKeys(any(), anyBoolean()))
        .thenReturn(singletonMap("key1", new ErrorInfo("ACCESS_DENIED", "ACL check failed")));
    final DeleteObject nullVersion = new DeleteObject("key1");
    nullVersion.setVersionId(NULL_VERSION_ID);
    final MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(nullVersion);

    final MultiDeleteResponse response = newEndpointFor(bucket, new HashMap<>()).multiDelete("b1", "", mdr);

    assertThat(response.getDeletedObjects()).isEmpty();
    assertThat(response.getErrors())
        .extracting(MultiDeleteResponse.Error::getKey, MultiDeleteResponse.Error::getVersionId)
        .containsExactly(tuple("key1", NULL_VERSION_ID));
  }

  @Test
  public void multiDeleteWithMatchingContentMD5DeletesKey() throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initTestData(client);
    byte[] body = deleteKey1Body();
    MultiDeleteRequest request = unmarshall(body);

    MultiDeleteResponse response = endpoint(client, contentMd5(body)).multiDelete("b1", "", request);

    assertThat(response.getErrors()).isEmpty();
    assertThat(response.getDeletedObjects()).extracting(DeletedObject::getKey).containsExactly("key1");
    assertThat(bucket.listKeys("")).toIterable().extracting(OzoneKey::getName).containsExactly("key2", "key3");
  }

  @ParameterizedTest
  @MethodSource("wrongContentMD5Provider")
  public void multiDeleteWithWrongContentMD5LeavesKey(String contentMD5, S3ErrorTable error) throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initTestData(client);
    MultiDeleteRequest request = unmarshall(deleteKey1Body());

    assertErrorResponse(error, () -> endpoint(client, contentMD5).multiDelete("b1", "", request));
    assertThat(bucket.listKeys("")).toIterable().extracting(OzoneKey::getName)
        .containsExactly("key1", "key2", "key3");
  }

  public static Stream<Arguments> wrongContentMD5Provider() throws NoSuchAlgorithmException {
    byte[] wrongMd5 = MessageDigest.getInstance("MD5").digest("wrong".getBytes(UTF_8));
    return Stream.of(
        Arguments.arguments(Base64.getEncoder().encodeToString(wrongMd5), BAD_DIGEST),
        Arguments.arguments("invalid-base64", INVALID_DIGEST));
  }

  private static byte[] deleteKey1Body() {
    return "<Delete><Object><Key>key1</Key></Object></Delete>".getBytes(UTF_8);
  }

  private static String contentMd5(byte[] body) throws NoSuchAlgorithmException {
    return Base64.getEncoder().encodeToString(MessageDigest.getInstance("MD5").digest(body));
  }

  private static MultiDeleteRequest unmarshall(byte[] body) {
    return new MultiDeleteRequestUnmarshaller()
        .readFrom(null, null, null, null, null, new ByteArrayInputStream(body));
  }

  private static BucketEndpoint endpoint(OzoneClient client, String contentMD5) {
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(S3Consts.CHECKSUM_HEADER)).thenReturn(contentMD5);
    return EndpointBuilder.newBucketEndpointBuilder().setClient(client).setHeaders(headers).build();
  }

  private MultiDeleteRequest threeKeyRequest() {
    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1"));
    mdr.getObjects().add(new DeleteObject("key2"));
    mdr.getObjects().add(new DeleteObject("key3"));
    return mdr;
  }

  private BucketEndpoint newEndpointFor(OzoneBucket bucket, Map<String, String> auditParams) throws IOException {
    OzoneConfiguration conf = new OzoneConfiguration();
    S3GatewayMetrics.create(conf);

    OzoneClient client = mock(OzoneClient.class);
    ObjectStore objectStore = mock(ObjectStore.class);
    ClientProtocol clientProtocol = mock(ClientProtocol.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getConfiguration()).thenReturn(conf);
    when(client.getProxy()).thenReturn(clientProtocol);
    when(objectStore.getClientProxy()).thenReturn(clientProtocol);
    when(clientProtocol.getBucketDetails(isNull(), anyString())).thenReturn(bucket);
    when(clientProtocol.deleteKeys(any(), anyString(), any(), anyBoolean()))
        .thenAnswer(invocation -> bucket.deleteKeys(invocation.getArgument(2), invocation.getArgument(3)));

    return EndpointBuilder.newBucketEndpointBuilder()
        .setBase(new BucketEndpoint() {
          @Override
          protected Map<String, String> getAuditParameters() {
            return auditParams;
          }
        })
        .setClient(client)
        .build();
  }

  private BucketEndpoint newEndpointWithExpectedOwner(String expectedOwner) throws IOException {
    OzoneBucket bucket = mock(OzoneBucket.class);
    when(bucket.getOwner()).thenReturn("owner");
    when(bucket.getVolumeName()).thenReturn("tenant-volume");
    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(expectedOwner);
    return EndpointBuilder.newBucketEndpointBuilder()
        .setClient(newEndpointFor(bucket, new HashMap<>()).getClient()).setHeaders(headers).build();
  }

  private OzoneBucket initTestData(OzoneClient client) throws IOException {
    client.getObjectStore().createS3Bucket("b1");

    OzoneBucket bucket =
        client.getObjectStore().getS3Bucket("b1");

    bucket.createKey("key1", 0).close();
    bucket.createKey("key2", 0).close();
    bucket.createKey("key3", 0).close();
    return bucket;
  }
}
