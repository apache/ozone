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
import static org.apache.hadoop.ozone.s3.endpoint.EndpointTestUtils.get;

import java.io.IOException;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts.QueryParams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for {@code GET /{bucket}/{key}?torrent} ({@code GetObjectTorrent}). */
public class TestObjectGetTorrent {

  private static final String BUCKET_NAME = "b1";
  private static final String KEY_NAME = "key1";
  private ObjectEndpoint objectEndpoint;

  @BeforeEach
  public void setup() throws IOException {
    final OzoneClient clientStub = new OzoneClientStub();
    clientStub.getObjectStore().createS3Bucket(BUCKET_NAME);

    objectEndpoint = EndpointBuilder.newObjectEndpointBuilder()
        .setClient(clientStub)
        .build();
  }

  @Test
  public void getObjectTorrentIsNotImplemented() {
    objectEndpoint.queryParamsForTest().set(QueryParams.TORRENT, "");

    assertErrorResponse(S3ErrorTable.NOT_IMPLEMENTED, () -> get(objectEndpoint, BUCKET_NAME, KEY_NAME));
  }
}
