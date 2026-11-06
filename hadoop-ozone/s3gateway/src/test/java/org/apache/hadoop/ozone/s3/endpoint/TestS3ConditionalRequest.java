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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import org.apache.hadoop.ozone.client.OzoneKey;
import org.apache.hadoop.ozone.s3.endpoint.S3ConditionalRequest.PreconditionContext;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.apache.hadoop.ozone.web.utils.OzoneUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Tests shared conditional request date handling.
 */
class TestS3ConditionalRequest {

  @ParameterizedTest
  @EnumSource(PreconditionContext.class)
  void testFutureDatesAreIgnored(PreconditionContext context) throws Exception {
    Instant future = Instant.now().plusSeconds(86400);
    HttpHeaders headers = mock(HttpHeaders.class);
    OzoneKey key = mock(OzoneKey.class);
    when(key.getMetadata()).thenReturn(Collections.emptyMap());
    String modifiedHeader = context == PreconditionContext.READ
        ? S3Consts.IF_MODIFIED_SINCE_HEADER : S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
    String unmodifiedHeader = context == PreconditionContext.READ
        ? S3Consts.IF_UNMODIFIED_SINCE_HEADER : S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
    when(headers.getHeaderString(modifiedHeader)).thenReturn(OzoneUtils.formatTime(future.toEpochMilli()));
    when(key.getModificationTime()).thenReturn(future.minusSeconds(172800));
    assertNull(S3ConditionalRequest.evaluatePreconditions(headers, "key", key, context));

    when(headers.getHeaderString(modifiedHeader)).thenReturn(null);
    when(headers.getHeaderString(unmodifiedHeader)).thenReturn(OzoneUtils.formatTime(future.toEpochMilli()));
    when(key.getModificationTime()).thenReturn(future.plusSeconds(86400));
    assertNull(S3ConditionalRequest.evaluatePreconditions(headers, "key", key, context));
  }

  @ParameterizedTest
  @CsvSource({"READ, 0", "READ, 500", "COPY_SOURCE, 0", "COPY_SOURCE, 500"})
  void testSameSecondDateConditions(PreconditionContext context, long millis) throws Exception {
    Instant since = Instant.parse("2020-01-01T00:00:00Z");
    HttpHeaders headers = mock(HttpHeaders.class);
    OzoneKey key = mock(OzoneKey.class);
    when(key.getMetadata()).thenReturn(Collections.emptyMap());
    when(key.getModificationTime()).thenReturn(since.plusMillis(millis));
    String modifiedHeader = context == PreconditionContext.READ
        ? S3Consts.IF_MODIFIED_SINCE_HEADER : S3Consts.COPY_SOURCE_IF_MODIFIED_SINCE;
    String unmodifiedHeader = context == PreconditionContext.READ
        ? S3Consts.IF_UNMODIFIED_SINCE_HEADER : S3Consts.COPY_SOURCE_IF_UNMODIFIED_SINCE;
    when(headers.getHeaderString(modifiedHeader)).thenReturn(OzoneUtils.formatTime(since.toEpochMilli()));
    if (context == PreconditionContext.READ) {
      try (Response response = S3ConditionalRequest.evaluatePreconditions(headers, "key", key, context)) {
        assertEquals(304, response.getStatus());
      }
    } else {
      OS3Exception ex = assertThrows(OS3Exception.class,
          () -> S3ConditionalRequest.evaluatePreconditions(headers, "key", key, context));
      assertEquals(412, ex.getHttpCode());
    }

    when(headers.getHeaderString(modifiedHeader)).thenReturn(null);
    when(headers.getHeaderString(unmodifiedHeader)).thenReturn(OzoneUtils.formatTime(since.toEpochMilli()));
    assertNull(S3ConditionalRequest.evaluatePreconditions(headers, "key", key, context));
  }
}
