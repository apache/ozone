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
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_FS_DATASTREAM_AUTO_THRESHOLD;
import static org.apache.hadoop.ozone.s3.util.S3Consts.COPY_SOURCE_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.STORAGE_CLASS_HEADER;
import static org.apache.hadoop.ozone.s3.util.S3Consts.X_AMZ_CONTENT_SHA256;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.conf.StorageUnit;
import org.apache.hadoop.ozone.OzoneConfigKeys;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * This class tests Upload request.
 */
public class TestUploadWithStream {

  private static final String S3BUCKET = "streamb1";
  private static final String S3KEY = "testkey";
  private static final String S3_COPY_EXISTING_KEY = "test_copy_existing_key";
  private static final String S3_COPY_EXISTING_KEY_CONTENT =
      "test_copy_existing_key_content";

  private ObjectEndpoint rest;

  private OzoneClient client;

  @BeforeEach
  public void setUp() throws Exception {
    client = new OzoneClientStub();
    client.getObjectStore().createS3Bucket(S3BUCKET);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(X_AMZ_CONTENT_SHA256)).thenReturn("mockSignature");
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn("STANDARD");

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OzoneConfigKeys.HDDS_CONTAINER_RATIS_DATASTREAM_ENABLED,
        true);
    conf.setStorageSize(OZONE_FS_DATASTREAM_AUTO_THRESHOLD, 1,
        StorageUnit.BYTES);

    rest = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(client)
        .setHeaders(headers)
        .setConfig(conf)
        .build();
  }

  @Test
  public void testEnableStream() {
    assertTrue(rest.isDatastreamEnabled());
  }

  @Test
  public void testUpload() throws Exception {
    byte[] keyContent = S3_COPY_EXISTING_KEY_CONTENT.getBytes(UTF_8);
    ByteArrayInputStream body =
        new ByteArrayInputStream(keyContent);
    Response response = rest.put(S3BUCKET, S3KEY, 0, 0, null, null, null, body);

    assertEquals(200, response.getStatus());
  }

  @Test
  public void testUploadWithCopy() throws Exception {
    OzoneBucket bucket =
        client.getObjectStore().getS3Bucket(S3BUCKET);

    byte[] keyContent = S3_COPY_EXISTING_KEY_CONTENT.getBytes(UTF_8);
    try (OutputStream stream = bucket
        .createStreamKey(S3_COPY_EXISTING_KEY, keyContent.length,
            ReplicationConfig.fromTypeAndFactor(ReplicationType.RATIS,
                ReplicationFactor.THREE), new HashMap<>())) {
      stream.write(keyContent);
    }

    final long dataSize = bucket.getKey(S3_COPY_EXISTING_KEY).getDataSize();
    assertEquals(dataSize, keyContent.length);


    Map<String, String> additionalHeaders = new HashMap<>();
    additionalHeaders
        .put(COPY_SOURCE_HEADER, S3BUCKET + "/" + S3_COPY_EXISTING_KEY);

    HttpHeaders headers = mock(HttpHeaders.class);
    when(headers.getHeaderString(STORAGE_CLASS_HEADER)).thenReturn(
        "STANDARD");

    additionalHeaders
        .forEach((k, v) -> when(headers.getHeaderString(k)).thenReturn(v));
    rest.setHeaders(headers);

    Response response = rest.put(S3BUCKET, S3KEY, 0, 0, null, null, null, null);

    assertEquals(200, response.getStatus());

    final long newDataSize = bucket.getKey(S3KEY).getDataSize();
    assertEquals(dataSize, newDataSize);
  }
}
