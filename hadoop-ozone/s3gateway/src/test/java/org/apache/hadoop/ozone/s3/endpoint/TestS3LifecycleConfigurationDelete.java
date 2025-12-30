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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.apache.hadoop.ozone.s3.exception.S3ErrorTable.NO_SUCH_LIFECYCLE_CONFIGURATION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientStub;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Testing for DeleteBucketLifecycleConfiguration.
 */
public class TestS3LifecycleConfigurationDelete {
  private BucketEndpoint bucketEndpoint;

  @BeforeEach
  public void setup() throws Exception {
    OzoneClient clientStub = new OzoneClientStub();
    bucketEndpoint = EndpointBuilder.newBucketEndpointBuilder()
        .setClient(clientStub)
        .build();
    ObjectStore objectStore = clientStub.getObjectStore();
    objectStore.createS3Bucket("bucket1");
    bucketEndpoint.queryParamsForTest().set(S3Consts.QueryParams.LIFECYCLE, "");
  }

  @Test
  public void testDeleteNonExistentLifecycleConfiguration()
      throws Exception {
    try {
      bucketEndpoint.delete("bucket1");
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_LIFECYCLE_CONFIGURATION.getCode(),
              ex.getCode());
    }
  }

  @Test
  public void testDeleteLifecycleConfiguration() throws Exception {
    String bucketName = "bucket1";
    bucketEndpoint.put(bucketName, getBody());
    Response r = bucketEndpoint.delete(bucketName);

    assertEquals(HTTP_NO_CONTENT, r.getStatus());

    try {
      // Make sure it was deleted.
      bucketEndpoint.get(bucketName);
      fail();
    } catch (OS3Exception ex) {
      assertEquals(HTTP_NOT_FOUND, ex.getHttpCode());
      assertEquals(NO_SUCH_LIFECYCLE_CONFIGURATION.getCode(),
          ex.getCode());
    }
  }

  private static InputStream getBody() {
    String xml = ("<LifecycleConfiguration xmlns=\"http://s3.amazonaws" +
        ".com/doc/2006-03-01/\">" +
        "<Rule>" +
        "<ID>remove logs after 30 days</ID>" +
        "<Prefix>prefix/</Prefix>" +
        "<Expiration><Days>30</Days></Expiration>" +
        "<Status>Enabled</Status>" +
        "</Rule>" +
        "</LifecycleConfiguration>");

    return new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
  }
}
