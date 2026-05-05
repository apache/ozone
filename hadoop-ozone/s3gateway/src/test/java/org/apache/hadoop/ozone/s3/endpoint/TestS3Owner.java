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

import java.util.stream.Stream;
import javax.ws.rs.core.HttpHeaders;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.apache.hadoop.ozone.s3.exception.S3ErrorTable;
import org.apache.hadoop.ozone.s3.util.S3Consts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;

/**
 * Unit test class for testing logic related to TestS3Owner.
 */
public class TestS3Owner {

  private static final String SOURCE_BUCKET_NAME = "source-bucket";
  private static final String DEST_BUCKET_NAME = "dest-bucket";
  private HttpHeaders headers;

  @BeforeEach
  public void setup() {
    headers = mock(HttpHeaders.class);
  }

  private static Stream<Arguments> noBucketOwnerSourceProvider() {
    return Stream.of(
        Arguments.of(null, null),
        Arguments.of(null, ""),
        Arguments.of("", null),
        Arguments.of("", "")
    );
  }

  @ParameterizedTest
  @MethodSource("noBucketOwnerSourceProvider")
  public void testHasBucketOwnershipVerificationConditionsFailed(String bucketOwner, String sourceBucketOwner) {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(bucketOwner);
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn(sourceBucketOwner);
    assertThat(S3Owner.hasBucketOwnershipVerificationConditions(headers)).isFalse();
  }

  private static Stream<Arguments> hasBucketOwnerSourceProvider() {
    return Stream.of(
        Arguments.of("owner1", null),
        Arguments.of(null, "owner2"),
        Arguments.of("owner1", "owner2")
    );
  }

  @ParameterizedTest
  @MethodSource("hasBucketOwnerSourceProvider")
  public void testHasBucketOwnershipVerificationConditionsPass(String bucketOwner, String sourceBucketOwner) {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(bucketOwner);
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn(sourceBucketOwner);
    assertThat(S3Owner.hasBucketOwnershipVerificationConditions(headers)).isTrue();
  }

  @Test
  public void testHeaderIsNull() {
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerCondition(null, SOURCE_BUCKET_NAME, "test"));
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(null, SOURCE_BUCKET_NAME, "test",
        SOURCE_BUCKET_NAME, "test"));
  }

  @Test
  public void testServerBucketOwnerIsNull() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("test");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("test");
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerCondition(headers, SOURCE_BUCKET_NAME, null));
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, null,
        SOURCE_BUCKET_NAME, "test"));
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, "test",
        SOURCE_BUCKET_NAME, null));
  }

  @ParameterizedTest
  @NullAndEmptySource
  public void testS3OwnerNotEnable(String bucketOwnerHeader) {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn(bucketOwnerHeader);
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerCondition(headers, SOURCE_BUCKET_NAME, "test"));

    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn(bucketOwnerHeader);
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(null, SOURCE_BUCKET_NAME, "test",
        SOURCE_BUCKET_NAME, "test"));
  }

  @Test
  public void testClientBucketOwnerIsNull() {
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerCondition(headers, SOURCE_BUCKET_NAME, null));
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("test");
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, null,
        SOURCE_BUCKET_NAME, "test"));
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, "test",
        SOURCE_BUCKET_NAME, null));
  }

  @Test
  public void testPassExpectedBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("test");
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerCondition(headers, SOURCE_BUCKET_NAME, "test"));
  }

  @Test
  public void testFailExpectedBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("wrong");
    OS3Exception exception =
        assertThrows(OS3Exception.class, () -> S3Owner.verifyBucketOwnerCondition(headers, SOURCE_BUCKET_NAME, "test"));
    assertThat(exception.getErrorMessage()).isEqualTo(S3ErrorTable.BUCKET_OWNER_MISMATCH.getErrorMessage());
  }

  @Test
  public void testCopyOperationPass() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("source");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("dest");
    assertDoesNotThrow(() -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, "source",
        SOURCE_BUCKET_NAME, "dest"));
  }

  @Test
  public void testCopyOperationFailedOnSourceBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("source");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("dest");
    OS3Exception exception =
        assertThrows(OS3Exception.class,
            () -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, "wrong",
                DEST_BUCKET_NAME, "dest"));
    assertThat(exception.getErrorMessage()).isEqualTo(S3ErrorTable.BUCKET_OWNER_MISMATCH.getErrorMessage());
    assertThat(exception.getResource()).isEqualTo(SOURCE_BUCKET_NAME);
  }

  @Test
  public void testCopyOperationFailedOnDestBucketOwner() {
    when(headers.getHeaderString(S3Consts.EXPECTED_SOURCE_BUCKET_OWNER_HEADER)).thenReturn("source");
    when(headers.getHeaderString(S3Consts.EXPECTED_BUCKET_OWNER_HEADER)).thenReturn("dest");
    OS3Exception exception =
        assertThrows(OS3Exception.class,
            () -> S3Owner.verifyBucketOwnerConditionOnCopyOperation(headers, SOURCE_BUCKET_NAME, "source",
                DEST_BUCKET_NAME, "wrong"));
    assertThat(exception.getErrorMessage()).isEqualTo(S3ErrorTable.BUCKET_OWNER_MISMATCH.getErrorMessage());
    assertThat(exception.getResource()).isEqualTo(DEST_BUCKET_NAME);
  }
}
