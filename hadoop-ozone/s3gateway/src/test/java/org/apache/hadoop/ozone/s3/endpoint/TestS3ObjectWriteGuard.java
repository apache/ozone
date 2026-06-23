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
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import org.apache.hadoop.ozone.client.OzoneBucketStub.KeyMetadataAwareByteBufferStreamOutput;
import org.apache.hadoop.ozone.client.OzoneBucketStub.KeyMetadataAwareOutputStream;
import org.apache.hadoop.ozone.client.io.OzoneDataStreamOutput;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;

/**
 * Tests for close-time commit guards used by S3 object writes.
 */
class TestS3ObjectWriteGuard {

  private static final String KEY_PATH = "bucket/key";
  private static final byte[] CONTENT = "content".getBytes(UTF_8);

  @Test
  void readIOExceptionBlocksCommitWithCause() {
    IOException failure = new IOException("read failed");
    S3ObjectWriteGuard guard = newGuard(CONTENT.length);

    IOException thrown = assertThrows(IOException.class,
        () -> guard.copyFrom(failingInputStream(failure), 4));

    assertSame(failure, thrown);
    assertCommitBlockedBy(guard, failure);
  }

  @Test
  void readRuntimeExceptionBlocksCommitWithCause() {
    RuntimeException failure = new IllegalStateException("read failed");
    S3ObjectWriteGuard guard = newGuard(CONTENT.length);

    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> guard.copyFrom(failingInputStream(failure), 4));

    assertSame(failure, thrown);
    assertCommitBlockedBy(guard, failure);
  }

  @Test
  void writeIOExceptionDoesNotAdvanceWrittenLengthAndBlocksCommit()
      throws ReflectiveOperationException {
    IOException failure = new IOException("write failed");
    FailingKeyMetadataAwareOutputStream keyOutputStream =
        new FailingKeyMetadataAwareOutputStream(failure);
    S3ObjectWriteGuard guard = new S3ObjectWriteGuard(
        new OzoneOutputStream(keyOutputStream, null),
        CONTENT.length, KEY_PATH);

    IOException thrown = assertThrows(IOException.class,
        () -> guard.copyFrom(new ByteArrayInputStream(CONTENT),
            CONTENT.length));

    assertSame(failure, thrown);
    assertThat(keyOutputStream.getWriteAttempts()).isEqualTo(1);
    assertThat(writtenLength(guard)).isZero();
    assertCommitBlockedBy(guard, failure);
  }

  @Test
  void earlyEofIsBlockedByContentLengthValidation() throws IOException {
    S3ObjectWriteGuard guard = newGuard(CONTENT.length);

    long copied = guard.copyFrom(new ByteArrayInputStream(new byte[] {1, 2}),
        4);

    assertThat(copied).isEqualTo(2);
    OS3Exception failure = assertThrows(OS3Exception.class, guard::close);
    assertThat(failure.getErrorMessage()).contains(
        "Request body length 2 does not match expected length "
            + CONTENT.length);
    assertThat(failure.getCause()).isNull();
  }

  @Test
  void datastreamWriteIOExceptionBlocksCommitWithCause() {
    IOException failure = new IOException("datastream write failed");
    FailingKeyMetadataAwareByteBufferStreamOutput keyOutputStream =
        new FailingKeyMetadataAwareByteBufferStreamOutput(failure);
    S3ObjectStreamingWriteGuard guard = new S3ObjectStreamingWriteGuard(
        new OzoneDataStreamOutput(keyOutputStream, null),
        CONTENT.length, KEY_PATH);

    IOException thrown = assertThrows(IOException.class,
        () -> guard.copyFrom(new ByteArrayInputStream(CONTENT),
            CONTENT.length));

    assertSame(failure, thrown);
    assertThat(keyOutputStream.getWriteAttempts()).isEqualTo(1);
    assertCommitBlockedBy(guard, failure);
  }

  private static S3ObjectWriteGuard newGuard(long expectedLength) {
    KeyMetadataAwareOutputStream keyOutputStream =
        new KeyMetadataAwareOutputStream(Collections.emptyMap());
    return new S3ObjectWriteGuard(new OzoneOutputStream(keyOutputStream, null),
        expectedLength, KEY_PATH);
  }

  private static InputStream failingInputStream(Throwable failure) {
    return new InputStream() {
      @Override
      public int read() throws IOException {
        throwFailure(failure);
        return -1;
      }

      @Override
      public int read(byte[] buffer, int offset, int length)
          throws IOException {
        throwFailure(failure);
        return -1;
      }
    };
  }

  private static void throwFailure(Throwable failure) throws IOException {
    if (failure instanceof IOException) {
      throw (IOException) failure;
    }
    if (failure instanceof RuntimeException) {
      throw (RuntimeException) failure;
    }
    throw new AssertionError(failure);
  }

  private static void assertCommitBlockedBy(
      S3ObjectWriteGuard guard, Throwable failure) {
    IOException commitFailure = assertThrows(IOException.class, guard::close);
    assertThat(commitFailure.getMessage()).isEqualTo(
        "S3 object transfer failed before commit: " + KEY_PATH);
    assertSame(failure, commitFailure.getCause());
  }

  private static long writtenLength(S3ObjectWriteGuard guard)
      throws ReflectiveOperationException {
    Field field = S3ObjectWriteGuard.class.getDeclaredField("writtenLength");
    field.setAccessible(true);
    return field.getLong(guard);
  }

  private static final class FailingKeyMetadataAwareOutputStream
      extends KeyMetadataAwareOutputStream {

    private final IOException failure;
    private int writeAttempts;

    private FailingKeyMetadataAwareOutputStream(IOException failure) {
      super(Collections.emptyMap());
      this.failure = failure;
    }

    @Override
    public void write(byte[] buffer, int offset, int length)
        throws IOException {
      writeAttempts++;
      throw failure;
    }

    private int getWriteAttempts() {
      return writeAttempts;
    }
  }

  private static final class FailingKeyMetadataAwareByteBufferStreamOutput
      extends KeyMetadataAwareByteBufferStreamOutput {

    private final IOException failure;
    private int writeAttempts;

    private FailingKeyMetadataAwareByteBufferStreamOutput(
        IOException failure) {
      super(Collections.emptyMap());
      this.failure = failure;
    }

    @Override
    public void write(ByteBuffer buffer, int offset, int length)
        throws IOException {
      writeAttempts++;
      throw failure;
    }

    private int getWriteAttempts() {
      return writeAttempts;
    }
  }
}
