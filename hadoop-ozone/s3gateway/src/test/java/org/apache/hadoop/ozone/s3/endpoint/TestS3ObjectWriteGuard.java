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
import static org.apache.hadoop.ozone.s3.signature.SignatureTestUtils.signedChunkedBody;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

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
import org.apache.hadoop.ozone.s3.MultiDigestInputStream;
import org.apache.hadoop.ozone.s3.SignedChunksInputStream;
import org.apache.hadoop.ozone.s3.TrailerHeader;
import org.apache.hadoop.ozone.s3.UnsignedChunksInputStream;
import org.apache.hadoop.ozone.s3.exception.OS3Exception;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for close-time commit guards used by S3 object writes.
 */
class TestS3ObjectWriteGuard {

  private static final String KEY_PATH = "bucket/key";
  private static final byte[] CONTENT = "content".getBytes(UTF_8);

  @Test
  void readIOExceptionBlocksCommit() {
    IOException failure = new IOException("read failed");
    S3ObjectWriteGuard guard = newGuard(CONTENT.length);

    IOException thrown = assertThrows(IOException.class,
        () -> guard.copyFrom(failingInputStream(failure), 4));

    assertSame(failure, thrown);
    assertCommitBlocked(guard);
  }

  @Test
  void readRuntimeExceptionBlocksCommit() {
    RuntimeException failure = new IllegalStateException("read failed");
    S3ObjectWriteGuard guard = newGuard(CONTENT.length);

    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> guard.copyFrom(failingInputStream(failure), 4));

    assertSame(failure, thrown);
    assertCommitBlocked(guard);
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
    assertCommitBlocked(guard);
  }

  @Test
  void earlyEofIsBlockedByContentLengthValidation() throws IOException {
    S3ObjectWriteGuard guard = newGuard(CONTENT.length);

    long copied = guard.copyFrom(new ByteArrayInputStream(new byte[] {1, 2}),
        4);

    assertThat(copied).isEqualTo(2);
    guard.markTransferComplete();
    OS3Exception failure = assertThrows(OS3Exception.class, guard::close);
    assertThat(failure.getErrorMessage()).contains(
        "Request body length 2 does not match expected length "
            + CONTENT.length);
    assertThat(failure.getCause()).isNull();
  }

  @Test
  void datastreamWriteIOExceptionBlocksCommit() {
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
    assertCommitBlocked(guard);
  }

  @Test
  void keyOpenedFailureBlocksEmptyCommit() {
    RuntimeException failure = new IllegalStateException("key-open callback failed");
    S3ObjectWriteGuard guard = newGuard(0);

    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> guard.onKeyOpened(key -> {
          throw failure;
        }));

    assertSame(failure, thrown);
    assertCommitBlocked(guard);
  }

  @Test
  void datastreamKeyOpenedFailureBlocksEmptyCommit() {
    RuntimeException failure = new IllegalStateException("key-open callback failed");
    S3ObjectStreamingWriteGuard guard = new S3ObjectStreamingWriteGuard(
        new OzoneDataStreamOutput(
            new KeyMetadataAwareByteBufferStreamOutput(Collections.emptyMap()), null),
        0, KEY_PATH);

    RuntimeException thrown = assertThrows(RuntimeException.class,
        () -> guard.onKeyOpened(key -> {
          throw failure;
        }));

    assertSame(failure, thrown);
    assertCommitBlocked(guard);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "content"})
  void transferNotMarkedCompleteBlocksCommit(String content) throws IOException {
    KeyMetadataAwareOutputStream keyOutputStream = new KeyMetadataAwareOutputStream(Collections.emptyMap());
    byte[] bytes = content.getBytes(UTF_8);
    S3ObjectWriteGuard guard = new S3ObjectWriteGuard(
        new OzoneOutputStream(keyOutputStream, null), bytes.length, KEY_PATH);
    guard.copyFrom(new ByteArrayInputStream(bytes), CONTENT.length);

    IOException failure = assertThrows(IOException.class, guard::close);

    assertThat(failure).hasMessage("S3 object transfer did not complete before commit: " + KEY_PATH);
    assertThat(keyOutputStream.getModificationTime()).isZero();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "content"})
  void completedTransferCommits(String content) throws IOException {
    KeyMetadataAwareOutputStream keyOutputStream = new KeyMetadataAwareOutputStream(Collections.emptyMap());
    byte[] bytes = content.getBytes(UTF_8);
    S3ObjectWriteGuard guard = new S3ObjectWriteGuard(
        new OzoneOutputStream(keyOutputStream, null), bytes.length, KEY_PATH);
    guard.copyFrom(new ByteArrayInputStream(bytes), CONTENT.length);
    guard.markTransferComplete();

    guard.close();

    assertThat(keyOutputStream.toByteArray()).isEqualTo(bytes);
    assertThat(keyOutputStream.getModificationTime()).isPositive();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void finalChunkErrorBlocksCommitAfterExactLengthRead(boolean datastream) throws IOException {
    S3ObjectWriteGuard guard = datastream ? new S3ObjectStreamingWriteGuard(
        new OzoneDataStreamOutput(new KeyMetadataAwareByteBufferStreamOutput(Collections.emptyMap()), null),
        CONTENT.length, KEY_PATH) : newGuard(CONTENT.length);
    SignedChunksInputStream chunks = spy(new SignedChunksInputStream(
        new ByteArrayInputStream(signedChunkedBody("content").getBytes(UTF_8)), KEY_PATH));
    OutOfMemoryError failure = new OutOfMemoryError("final chunk verification");
    doThrow(failure).when(chunks).verifyComplete();

    assertSame(failure, assertThrows(OutOfMemoryError.class, () -> guard.copyFrom(chunks, CONTENT.length)));
    assertThrows(IOException.class, guard::close);
  }

  private static S3ObjectWriteGuard newGuard(long expectedLength) {
    KeyMetadataAwareOutputStream keyOutputStream =
        new KeyMetadataAwareOutputStream(Collections.emptyMap());
    return new S3ObjectWriteGuard(new OzoneOutputStream(keyOutputStream, null),
        expectedLength, KEY_PATH);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void unsignedTrailerFailureBlocksCommitAfterExactLengthRead(boolean datastream) throws ReflectiveOperationException {
    S3ObjectWriteGuard guard = datastream ? new S3ObjectStreamingWriteGuard(
        new OzoneDataStreamOutput(new KeyMetadataAwareByteBufferStreamOutput(Collections.emptyMap()), null),
        CONTENT.length, KEY_PATH) : newGuard(CONTENT.length);
    // All decoded bytes are present; the trailer is missing its final blank line.
    byte[] body = "7\r\ncontent\r\n0\r\nx-amz-checksum-crc32:AAAAAA==\r\n".getBytes(UTF_8);
    UnsignedChunksInputStream chunks = new UnsignedChunksInputStream(
        new ByteArrayInputStream(body), KEY_PATH, TrailerHeader.CRC32);
    MultiDigestInputStream input = new MultiDigestInputStream(chunks, Collections.emptyList());

    assertThrows(OS3Exception.class, () -> guard.copyFrom(input, CONTENT.length));

    assertThat(writtenLength(guard)).isEqualTo(CONTENT.length);
    assertCommitBlocked(guard);
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

  private static void assertCommitBlocked(S3ObjectWriteGuard guard) {
    IOException commitFailure = assertThrows(IOException.class, guard::close);
    assertThat(commitFailure.getMessage()).isEqualTo(
        "S3 object transfer did not complete before commit: " + KEY_PATH);
    assertThat(commitFailure.getCause()).isNull();
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
