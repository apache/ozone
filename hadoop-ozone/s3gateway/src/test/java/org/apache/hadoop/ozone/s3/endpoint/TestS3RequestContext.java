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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.ozone.audit.S3GAction;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.apache.hadoop.ozone.client.OzoneVolume;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests bucket reuse within a request. */
class TestS3RequestContext {
  private EndpointBase endpoint;
  private OzoneVolume volume;
  private OzoneBucket bucket;
  private S3RequestContext context;

  @BeforeEach
  void setup() throws IOException {
    endpoint = mock(EndpointBase.class);
    volume = mock(OzoneVolume.class);
    bucket = mock(OzoneBucket.class);
    when(bucket.getName()).thenReturn("bucket1");
    when(endpoint.getVolume()).thenReturn(volume);
    when(volume.getBucket("bucket1")).thenReturn(bucket);
    context = new S3RequestContext(endpoint, S3GAction.PUT_BUCKET_LIFECYCLE);
  }

  @Test
  void reusesBucketWithinRequest() throws IOException {
    assertThat(context.getBucket("bucket1")).isSameAs(bucket);
    assertThat(context.getBucket("bucket1")).isSameAs(bucket);
    verify(volume).getBucket("bucket1");
    verify(endpoint).getVolume();
  }

  @Test
  void rejectsDifferentBucket() throws IOException {
    context.getBucket("bucket1");
    assertThrows(IllegalStateException.class, () -> context.getBucket("bucket2"));
    verify(volume, never()).getBucket("bucket2");
    assertThat(context.getBucket("bucket1")).isSameAs(bucket);
    verify(volume).getBucket("bucket1");
  }

  @Test
  void retriesFailedLookup() throws IOException {
    when(volume.getBucket("bucket1")).thenThrow(new IOException("Lookup failed")).thenReturn(bucket);
    assertThrows(IOException.class, () -> context.getBucket("bucket1"));
    assertThat(context.getBucket("bucket1")).isSameAs(bucket);
    assertThat(context.getBucket("bucket1")).isSameAs(bucket);
    verify(volume, times(2)).getBucket("bucket1");
  }

  @Test
  void doesNotShareBucketBetweenRequests() throws IOException {
    S3RequestContext second = new S3RequestContext(endpoint, S3GAction.PUT_BUCKET_LIFECYCLE);
    assertThat(context.getBucket("bucket1")).isSameAs(bucket);
    assertThat(second.getBucket("bucket1")).isSameAs(bucket);
    verify(volume, times(2)).getBucket("bucket1");
  }
}
