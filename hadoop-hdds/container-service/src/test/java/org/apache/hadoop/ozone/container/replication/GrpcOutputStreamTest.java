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

package org.apache.hadoop.ozone.container.replication;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Tests for {@code GrpcOutputStream}.
 */
@ExtendWith(MockitoExtension.class)
abstract class GrpcOutputStreamTest<T> {

  private static final Random RND = new Random();

  private final long containerId = RND.nextLong();
  private final int bufferSize = RND.nextInt(1024) + 128 + 1;
  private final Class<? extends T> clazz;

  @Mock
  private CallStreamObserver<T> observer;

  private OutputStream subject;

  protected GrpcOutputStreamTest(Class<? extends T> clazz) {
    this.clazz = clazz;
  }

  @BeforeEach
  public void setUp() {
    subject = createSubject();
    when(observer.isReady()).thenReturn(true);
  }

  protected abstract OutputStream createSubject();

  @Test
  public void seriesOfBytesInSingleResponse() throws IOException {
    byte[] bytes = getRandomBytes(5);
    for (byte b : bytes) {
      subject.write(b);
    }
    subject.close();

    verifyResponses(bytes);
  }

  @Test
  public void mixedBytesAndArraysInSingleResponse() throws IOException {
    byte[] bytes = getRandomBytes(16);
    subject.write(bytes[0]);
    subject.write(bytes, 1, 14);
    subject.write(bytes[15]);
    subject.close();

    verifyResponses(bytes);
  }

  @Test
  public void mixedArraysAndBytesInSingleResponse() throws IOException {
    byte[] bytes = getRandomBytes(10);

    subject.write(bytes, 0, 5);
    subject.write(bytes[5]);
    subject.write(bytes, 6, 4);
    subject.close();

    verifyResponses(bytes);
  }

  @Test
  public void seriesOfArraysInSingleResponse() throws IOException {
    byte[] bytes = getRandomBytes(8);

    subject.write(bytes, 0, 5);
    subject.write(bytes, 5, 3);
    subject.close();

    verifyResponses(bytes);
  }

  @Test
  public void seriesOfArraysExactlyFillBuffer() throws IOException {
    int half = bufferSize / 2, otherHalf = bufferSize - half;
    byte[] bytes = getRandomBytes(2 * bufferSize);

    // fill buffer
    subject.write(bytes, 0, half);
    subject.write(bytes, half, otherHalf);
    // fill buffer again
    subject.write(bytes, bufferSize, half);
    subject.write(bytes, bufferSize + half, otherHalf);
    subject.close();

    verifyResponses(bytes);
  }

  @Test
  public void bufferFlushedWhenFull() throws IOException {
    byte[] bytes = getRandomBytes(bufferSize);

    subject.write(bytes, 0, bufferSize - 1);
    subject.write(bytes[bufferSize - 1]);
    verify(observer).onNext(any());

    subject.write(bytes[0]);
    subject.write(bytes, 1, bufferSize - 1);
    verify(observer, times(2)).onNext(any());
  }

  @Test
  public void singleArraySpansMultipleResponses() throws IOException {
    byte[] bytes = writeBytes(subject, 2 * bufferSize + bufferSize / 2);
    subject.close();

    verifyResponses(bytes);
  }

  @Test
  public void secondWriteSpillsToNextResponse() throws IOException {
    byte[] bytes1 = writeBytes(subject, bufferSize / 2);
    byte[] bytes2 = writeBytes(subject, 2 * bufferSize);
    subject.close();

    verifyResponses(concat(bytes1, bytes2));
  }

  @Test
  void rejectsWriteAfterClose() throws IOException {
    subject.close();

    assertThrows(IllegalStateException.class, () -> subject.write(42));
    assertThrows(IllegalStateException.class, () -> writeBytes(subject, 42));

    subject.close(); // close is idempotent
  }

  private void verifyResponses(byte[] bytes) {
    int expectedResponseCount = bytes.length / bufferSize;
    if (bytes.length % bufferSize > 0) {
      expectedResponseCount++;
    }

    ArgumentCaptor<T> captor =
        ArgumentCaptor.forClass(clazz);
    verify(observer, times(expectedResponseCount)).onNext(captor.capture());

    List<T> responses =
        new ArrayList<>(captor.getAllValues());
    for (int i = 0; i < expectedResponseCount; i++) {
      T response = responses.get(i);
      int expectedOffset = i * bufferSize;
      int size = Math.min(bufferSize, bytes.length - expectedOffset);
      byte[] part = new byte[size];
      System.arraycopy(bytes, expectedOffset, part, 0, size);
      ByteString data = verifyPart(response, expectedOffset, size);
      assertArrayEquals(part, data.toByteArray());

      // we don't want concatenated ByteStrings
      assertEquals("LiteralByteString", data.getClass().getSimpleName());
    }

    verify(observer, times(1)).onCompleted();
  }

  protected abstract ByteString verifyPart(
      T response, int expectedOffset, int size);

  private static byte[] concat(byte[]... parts) {
    int length = Arrays.stream(parts).mapToInt(each -> each.length).sum();
    byte[] bytes = new byte[length];
    int pos = 0;
    for (byte[] part : parts) {
      System.arraycopy(part, 0, bytes, pos, part.length);
      pos += part.length;
    }
    return bytes;
  }

  private static byte[] writeBytes(OutputStream subject, int size)
      throws IOException {
    byte[] bytes = getRandomBytes(size);
    subject.write(bytes);
    return bytes;
  }

  static byte[] getRandomBytes(int size) {
    byte[] bytes = new byte[size];
    RND.nextBytes(bytes);
    return bytes;
  }

  long getContainerId() {
    return containerId;
  }

  CallStreamObserver<T> getObserver() {
    return observer;
  }

  int getBufferSize() {
    return bufferSize;
  }
}
