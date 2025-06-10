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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.om.exceptions.OMException;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

/**
 * Unit test class for testing logic related to TestS3Owner.
 */
public class TestS3Owner {

  private HttpHeaders headers;

  @BeforeEach
  public void setup() {
    headers = mock(HttpHeaders.class);
  }

  @Test
  public void testHeaderIsNull() {
    assertDoesNotThrow(() -> S3Owner.verify(null, "test"));
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(null, "test", "test"));
  }

  @Test
  public void testServerBucketOwnerIsNull() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("test");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("test");
    assertDoesNotThrow(() -> S3Owner.verify(headers, null));
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(headers, null, "test"));
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(headers, "test", null));
  }

  @ParameterizedTest
  @NullAndEmptySource
  public void testS3OwnerNotEnable(String bucketOwnerHeader) {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(bucketOwnerHeader);
    assertDoesNotThrow(() -> S3Owner.verify(headers, "test"));

    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn(bucketOwnerHeader);
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(null, "test", "test"));
  }

  @Test
  public void testClientBucketOwnerIsNull() {
    assertDoesNotThrow(() -> S3Owner.verify(headers, null));
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("test");
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(headers, null, "test"));
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(headers, "test", null));
  }
  
  @Test
  public void testPassExpectedBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("test");
    assertDoesNotThrow(() -> S3Owner.verify(headers, "test"));
  }

  @Test
  public void testFailExpectedBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("wrong");
    OMException exception = assertThrows(OMException.class, () -> S3Owner.verify(headers, "test"));
    assertThat(exception).hasMessageContaining(S3Owner.ERROR_MESSAGE);
  }

  @Test
  public void testCopyOperationPass() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("source");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("dest");
    assertDoesNotThrow(() -> S3Owner.verifyCopyOperation(headers, "source", "dest"));
  }

  @Test
  public void testCopyOperationFailedOnSourceBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("source");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("dest");
    OMException exception =
        assertThrows(OMException.class, () -> S3Owner.verifyCopyOperation(headers, "wrong", "dest"));
    assertThat(exception).hasMessageContaining(S3Owner.ERROR_MESSAGE);
  }

  @Test
  public void testCopyOperationFailedOnDestBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("source");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("dest");
    OMException exception =
        assertThrows(OMException.class, () -> S3Owner.verifyCopyOperation(headers, "source", "wrong"));
    assertThat(exception).hasMessageContaining(S3Owner.ERROR_MESSAGE);
  }
}
