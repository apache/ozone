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

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.apache.hadoop.ozone.OzoneConsts.ETAG;
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.assertErrorResponse;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.MALFORMED_XML;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.PRECOND_FAILED;
import static org.apache.hadoop.ozone.s3.util.S3Utils.parseETag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.bind.JAXBException;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.exceptions.OMException.ResultCodes;
import org.apache.hadoop.ozone.om.helpers.ErrorInfo;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteRequest.DeleteObject;
import org.apache.hadoop.ozone.s3.endpoint.MultiDeleteResponse.DeletedObject;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.metrics.S3GatewayMetrics;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.Test;

/**
 * Test object multi delete.
 */
public class TestObjectMultiDelete {

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
  public void conditionalDeleteMatchingEtag() throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initConditionalDeleteTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    MultiDeleteRequest.DeleteObject obj1 = new MultiDeleteRequest.DeleteObject("key1");
    obj1.setETag("\"match-1\"");
    mdr.getObjects().add(obj1);
    MultiDeleteRequest.DeleteObject obj2 = new MultiDeleteRequest.DeleteObject("key2");
    obj2.setETag("match-2");
    mdr.getObjects().add(obj2);

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertEquals(2, response.getDeletedObjects().size());
    assertEquals(0, response.getErrors().size());
    assertEquals(singleton("key3"), listKeyNames(bucket));
  }

  @Test
  public void conditionalDeleteMatchingIfMatch() throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initConditionalDeleteTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1", "\"match-1\""));
    mdr.getObjects().add(new DeleteObject("key2", "match-2"));

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertEquals(2, response.getDeletedObjects().size());
    assertEquals(0, response.getErrors().size());
    assertEquals(singleton("key3"), listKeyNames(bucket));
  }

  @Test
  public void conditionalDeleteMismatchedIfMatch() throws Exception {
    OzoneClient client = new OzoneClientStub();
    initConditionalDeleteTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1", "\"wrong-match\""));

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertEquals(0, response.getDeletedObjects().size());
    assertEquals(1, response.getErrors().size());
    assertEquals(PRECOND_FAILED.getCode(), response.getErrors().get(0).getCode());
    assertEquals("key1", response.getErrors().get(0).getKey());
  }

  @Test
  public void conditionalDeleteMissingKeyFails() throws Exception {
    OzoneClient client = new OzoneClientStub();
    initConditionalDeleteTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("missing-key", "\"match-1\""));

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertEquals(0, response.getDeletedObjects().size());
    assertEquals(1, response.getErrors().size());
    assertEquals(PRECOND_FAILED.getCode(), response.getErrors().get(0).getCode());
  }

  @Test
  public void conditionalDeleteWildcardIfMatch() throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initConditionalDeleteTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1", "*"));
    mdr.getObjects().add(new DeleteObject("missing-key", "*"));

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertEquals(1, response.getDeletedObjects().size());
    assertEquals("key1", response.getDeletedObjects().get(0).getKey());
    assertEquals(1, response.getErrors().size());
    assertEquals("missing-key", response.getErrors().get(0).getKey());
    assertEquals(PRECOND_FAILED.getCode(), response.getErrors().get(0).getCode());
    assertTrue(listKeyNames(bucket).contains("key2"));
    assertTrue(listKeyNames(bucket).contains("key3"));
  }

  @Test
  public void conditionalDeleteMixedWithUnconditional() throws Exception {
    OzoneClient client = new OzoneClientStub();
    OzoneBucket bucket = initConditionalDeleteTestData(client);

    BucketEndpoint rest = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(client)
        .build();

    MultiDeleteRequest mdr = new MultiDeleteRequest();
    mdr.getObjects().add(new DeleteObject("key1", "\"match-1\""));
    mdr.getObjects().add(new DeleteObject("key2"));
    mdr.getObjects().add(new DeleteObject("missing-unconditional"));

    MultiDeleteResponse response = rest.multiDelete("b1", "", mdr);

    assertEquals(3, response.getDeletedObjects().size());
    assertEquals(0, response.getErrors().size());
    assertEquals(singleton("key3"), listKeyNames(bucket));
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
    MultiDeleteResponse response =
        newEndpointFor(bucket, auditParams).multiDelete("b1", "", threeKeyRequest());

    assertEquals(0, response.getDeletedObjects().size());
    assertEquals(1, response.getErrors().size());
    assertEquals("ALL", response.getErrors().get(0).getKey());
    assertEquals("InternalError", response.getErrors().get(0).getCode());
    assertEquals("[key1, key2, key3]", auditParams.get("failedDeletes"));
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
    OzoneVolume volume = mock(OzoneVolume.class);
    when(client.getObjectStore()).thenReturn(objectStore);
    when(client.getConfiguration()).thenReturn(conf);
    when(objectStore.getClientProxy()).thenReturn(mock(ClientProtocol.class));
    when(objectStore.getS3Volume()).thenReturn(volume);
    when(volume.getBucket(anyString())).thenReturn(bucket);

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

  private OzoneBucket initTestData(OzoneClient client) throws IOException {
    client.getObjectStore().createS3Bucket("b1");

    OzoneBucket bucket =
        client.getObjectStore().getS3Bucket("b1");

    bucket.createKey("key1", 0).close();
    bucket.createKey("key2", 0).close();
    bucket.createKey("key3", 0).close();
    return bucket;
  }

  private OzoneBucket initConditionalDeleteTestData(OzoneClient client) throws IOException {
    client.getObjectStore().createS3Bucket("b1");

    OzoneBucket bucket = client.getObjectStore().getS3Bucket("b1");
    createKeyWithIfMatchTarget(bucket, "key1", "\"match-1\"");
    createKeyWithIfMatchTarget(bucket, "key2", "match-2");
    createKeyWithIfMatchTarget(bucket, "key3", "match-3");
    return bucket;
  }

  private static void createKeyWithIfMatchTarget(OzoneBucket bucket, String keyName,
      String ifMatch) throws IOException {
    Map<String, String> metadata = new HashMap<>();
    metadata.put(ETAG, parseETag(ifMatch));
    try (OzoneOutputStream out = bucket.createKey(keyName, 1,
        bucket.getReplicationConfig(), metadata, emptyMap())) {
      out.write('x');
    }
  }

  private static Set<String> listKeyNames(OzoneBucket bucket) throws IOException {
    return Sets.newHashSet(bucket.listKeys("")).stream()
        .map(OzoneKey::getName)
        .collect(Collectors.toSet());
  }
}
