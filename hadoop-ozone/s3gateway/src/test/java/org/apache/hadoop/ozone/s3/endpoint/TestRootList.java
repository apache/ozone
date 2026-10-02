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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.apache.hadoop.ozone.client.protocol.ClientProtocol;
import org.apache.hadoop.ozone.om.helpers.BucketLayout;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.signature.SignatureInfo;
import org.apache.hadoop.ozone.s3.util.ContinueToken;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * This class test HeadBucket functionality.
 */
public class TestRootList {

  private OzoneClient clientStub;
  private RootEndpoint rootEndpoint;

  private static final String DEFAULT_VOLUME = OzoneConfigKeys.OZONE_S3_VOLUME_NAME_DEFAULT;

  @BeforeEach
  public void setup() throws Exception {

    //Create client stub and object store stub.
    clientStub = new OzoneClientStub();

    // Create HeadBucket and setClient to OzoneClientStub
    rootEndpoint = EndpointBuilder.newRootEndpointBuilder()
        .setClient(clientStub)
        .build();

    clientStub.getObjectStore().createVolume(DEFAULT_VOLUME);

  }

  @Test
  public void testListBucket() throws Exception {

    // List operation should succeed even there is no bucket.
    ListBucketResponse response =
        (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(0, response.getBucketsNum());

    String bucketBaseName = "bucket-" + getClass().getName();
    for (int i = 0; i < 10; i++) {
      clientStub.getObjectStore().createS3Bucket(bucketBaseName + i);
    }
    response = (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(10, response.getBucketsNum());
    assertEquals("root", response.getOwner().getDisplayName());
    assertEquals(S3Owner.DEFAULT_S3OWNER_ID, response.getOwner().getId());
  }

  @Test
  public void testListAllBucketsPaginated() throws Exception {
    ListBucketResponse response = listWithMaxBuckets(1);
    assertEquals(0, response.getBucketsNum());
    assertNull(response.getContinuationToken());

    clientStub.getObjectStore().createS3Bucket("bucket-a");
    response = listWithMaxBuckets(1);
    assertEquals(1, response.getBucketsNum());
    assertEquals("bucket-a", response.getBuckets().get(0).getName());
    assertNull(response.getContinuationToken());

    clientStub.getObjectStore().createS3Bucket("bucket-b");
    response = listWithMaxBuckets(1);
    assertEquals(1, response.getBucketsNum());
    assertEquals("bucket-a", response.getBuckets().get(0).getName());
    assertNotNull(response.getContinuationToken());

    rootEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN,
        response.getContinuationToken());
    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_BUCKETS, 1);
    response = (ListBucketResponse) rootEndpoint.get().getEntity();
    assertEquals(1, response.getBucketsNum());
    assertEquals("bucket-b", response.getBuckets().get(0).getName());
    assertNull(response.getContinuationToken());
  }

  @Test
  public void testListAllBucketsPaginationMultiplePages() throws Exception {
    String bucketBaseName = "bucket-" + getClass().getName();
    for (int i = 0; i < 5; i++) {
      clientStub.getObjectStore().createS3Bucket(bucketBaseName + i);
    }

    ListBucketResponse response = listWithMaxBuckets(2);

    assertEquals(2, response.getBucketsNum());
    assertEquals(bucketBaseName + 0, response.getBuckets().get(0).getName());
    assertEquals(bucketBaseName + 1, response.getBuckets().get(1).getName());
    assertNotNull(response.getContinuationToken());

    rootEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN,
        response.getContinuationToken());
    response = (ListBucketResponse) rootEndpoint.get().getEntity();

    assertEquals(2, response.getBucketsNum());
    assertEquals(bucketBaseName + 2, response.getBuckets().get(0).getName());
    assertEquals(bucketBaseName + 3, response.getBuckets().get(1).getName());
    assertNotNull(response.getContinuationToken());

    rootEndpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN,
        response.getContinuationToken());
    response = (ListBucketResponse) rootEndpoint.get().getEntity();

    assertEquals(1, response.getBucketsNum());
    assertEquals(bucketBaseName + 4, response.getBuckets().get(0).getName());
    assertNull(response.getContinuationToken());
  }

  @Test
  public void testListAllBucketsInvalidMaxBuckets() {
    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_BUCKETS, 0);
    assertThrows(OS3Exception.class, () -> rootEndpoint.get());

    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_BUCKETS, -1);
    assertThrows(OS3Exception.class, () -> rootEndpoint.get());
  }

  @Test
  public void testListAllBucketsUnpaginatedReturnsAll() throws Exception {
    for (int i = 0; i < 3; i++) {
      clientStub.getObjectStore().createS3Bucket("unpaginated-bucket-" + i);
    }

    rootEndpoint.queryParamsForTest().unset(QueryParams.MAX_BUCKETS);
    rootEndpoint.queryParamsForTest().unset(QueryParams.CONTINUATION_TOKEN);
    ListBucketResponse response =
        (ListBucketResponse) rootEndpoint.get().getEntity();

    assertEquals(3, response.getBucketsNum());
    assertNull(response.getContinuationToken());
  }

  @ParameterizedTest(name = "directory={0}, buckets={1}, limit={2}, previous={3}")
  @CsvSource({
      // directory, total buckets, response limit, previous index, expected names, continuation token, RPC calls
      "false, 0, 2, -1, '', false, 1",
      "false, 1, 2, -1, bucket-0, false, 2",
      "false, 2, 2, -1, bucket-0;bucket-1, false, 2",
      "false, 3, 2, -1, bucket-0;bucket-1, true, 2",
      "false, 2, 1, -1, bucket-0, true, 1",
      "false, 3, 5, -1, bucket-0;bucket-1;bucket-2, false, 3",
      "false, 3, 2, 1, bucket-2, false, 2",
      "false, 3, , -1, bucket-0;bucket-1;bucket-2, false, 3",
      "true, 0, 2, -1, '', false, 1",
      "true, 1, 2, -1, bucket-0, false, 2",
      "true, 2, 2, -1, bucket-0, false, 2",
      "true, 3, 2, -1, bucket-0;bucket-2, false, 3",
      "true, 4, 2, -1, bucket-0;bucket-2, true, 2",
      "true, 2, 1, -1, bucket-0, true, 1",
      "true, 3, 5, -1, bucket-0;bucket-2, false, 3",
      "true, 3, 0, -1, '', false, 1",
      "true, 5, 2, 2, bucket-4, false, 2",
      "true, 2, 2, 0, '', false, 2"
  })
  void testListBucketsRpcCount(boolean directory, int total, Integer limit, int previous,
      String expectedNames, boolean hasToken, int expectedCalls) throws Exception {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setInt(OzoneConfigKeys.OZONE_CLIENT_LIST_CACHE_SIZE, 2);
    ClientProtocol proxy = mock(ClientProtocol.class);
    List<OzoneBucket> buckets = new ArrayList<>();
    for (int i = 0; i < total; i++) {
      buckets.add(OzoneBucket.newBuilder(conf, proxy).setVolumeName(DEFAULT_VOLUME).setName("bucket-" + i)
          .setBucketLayout(i % 2 == 0 ? BucketLayout.FILE_SYSTEM_OPTIMIZED : BucketLayout.OBJECT_STORE).build());
    }
    when(proxy.listBuckets(eq(DEFAULT_VOLUME), isNull(), nullable(String.class), eq(2), eq(false)))
        .thenAnswer(invocation -> {
          String marker = invocation.getArgument(2);
          return buckets.stream()
              .filter(bucket -> marker == null || bucket.getName().compareTo(marker) > 0)
              .limit(2).collect(Collectors.toList());
        });
    OzoneVolume volume = OzoneVolume.newBuilder(conf, proxy).setName(DEFAULT_VOLUME).setOwner("root")
        .setAcls(Collections.emptyList()).build();
    ObjectStore store = mock(ObjectStore.class);
    when(store.getS3Volume()).thenReturn(volume);
    when(store.getClientProxy()).thenReturn(proxy);
    OzoneClient client = mock(OzoneClient.class);
    when(client.getObjectStore()).thenReturn(store);
    when(client.getProxy()).thenReturn(proxy);
    RootEndpoint endpoint = EndpointBuilder.newRootEndpointBuilder().setClient(client)
        .setSignatureInfo(new SignatureInfo.Builder(SignatureInfo.Version.V4)
            .setCredentialScope("20260101/us-west-2/" + (directory ? "s3express" : "s3") + "/aws4_request").build())
        .build();
    if (limit != null) {
      endpoint.queryParamsForTest().setInt(
          directory ? QueryParams.MAX_DIRECTORY_BUCKETS : QueryParams.MAX_BUCKETS, limit);
    }
    if (previous >= 0) {
      endpoint.queryParamsForTest().set(QueryParams.CONTINUATION_TOKEN,
          new ContinueToken("bucket-" + previous, null).encodeToString());
    }
    try (Response httpResponse = endpoint.get()) {
      Object entity = httpResponse.getEntity();
      List<String> names;
      String token;
      if (directory) {
        ListDirectoryBucketsResponse response = (ListDirectoryBucketsResponse) entity;
        names = response.getBuckets().stream().map(bucket -> bucket.getName()).collect(Collectors.toList());
        token = response.getContinuationToken();
      } else {
        ListBucketResponse response = (ListBucketResponse) entity;
        names = response.getBuckets().stream().map(bucket -> bucket.getName()).collect(Collectors.toList());
        token = response.getContinuationToken();
        assertThat(response.getOwner().getDisplayName()).isEqualTo("root");
      }
      assertThat(names).containsExactly(StringUtils.split(expectedNames, ';'));
      if (hasToken) {
        assertThat(token).isNotNull();
        assertThat(ContinueToken.decodeFromString(token).getLastKey()).isEqualTo(names.get(names.size() - 1));
      } else {
        assertThat(token).isNull();
      }
    }
    verify(proxy, times(expectedCalls))
        .listBuckets(eq(DEFAULT_VOLUME), isNull(), nullable(String.class), eq(2), eq(false));
  }

  private ListBucketResponse listWithMaxBuckets(int maxBuckets) throws Exception {
    rootEndpoint.queryParamsForTest().unset(QueryParams.CONTINUATION_TOKEN);
    rootEndpoint.queryParamsForTest().setInt(QueryParams.MAX_BUCKETS, maxBuckets);
    return (ListBucketResponse) rootEndpoint.get().getEntity();
  }

}
