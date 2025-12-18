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

package org.apache.hadoop.hdds.utils.db;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hdds.utils.db.CodecTestUtil.gc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import com.google.common.primitives.Shorts;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import org.apache.hadoop.hdds.utils.db.AtomicRDBBatchOperation.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test {@link Codec} implementations.
 */
public final class TestCodec {
  static final Logger LOG = LoggerFactory.getLogger(TestCodec.class);
  static final int NUM_LOOPS = 10;

  static {
    CodecBuffer.enableLeakDetection();
  }

  @Test
  public void testShortCodec() throws Exception {
    runTestShortCodec((short)0);
    runTestShortCodec((short)1);
    runTestShortCodec((short)-1);
    runTestShortCodec(Short.MAX_VALUE);
    runTestShortCodec(Short.MIN_VALUE);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final short original = (short) ThreadLocalRandom.current().nextInt();
      runTestShortCodec(original);
    }
    gc();
  }

  static void runTestShortCodec(short original) throws Exception {
    runTest(ShortCodec.get(), original, Short.BYTES);
    runTestShorts(original);
  }

  /** Make sure that {@link ShortCodec} and {@link Shorts} are compatible. */
  static void runTestShorts(short original) {
    final ShortCodec codec = ShortCodec.get();
    final byte[] bytes = Shorts.toByteArray(original);
    assertArrayEquals(bytes, codec.toPersistedFormat(original));
    assertEquals(original, Shorts.fromByteArray(bytes));
    assertEquals(original, codec.fromPersistedFormat(bytes));
  }

  @Test
  public void testIntegerCodec() throws Exception {
    runTestIntegerCodec(0);
    runTestIntegerCodec(1);
    runTestIntegerCodec(-1);
    runTestIntegerCodec(Integer.MAX_VALUE);
    runTestIntegerCodec(Integer.MIN_VALUE);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final int original = ThreadLocalRandom.current().nextInt();
      runTestIntegerCodec(original);
    }
    gc();
  }

  static void runTestIntegerCodec(int original) throws Exception {
    runTest(IntegerCodec.get(), original, Integer.BYTES);
    runTestInts(original);
  }

  /** Make sure that {@link IntegerCodec} and {@link Ints} are compatible. */
  static void runTestInts(int original) {
    final IntegerCodec codec = IntegerCodec.get();
    final byte[] bytes = Ints.toByteArray(original);
    assertArrayEquals(bytes, codec.toPersistedFormat(original));
    assertEquals(original, Ints.fromByteArray(bytes));
    assertEquals(original, codec.fromPersistedFormat(bytes));
  }

  @Test
  public void testLongCodec() throws Exception {
    runTestLongCodec(0L);
    runTestLongCodec(1L);
    runTestLongCodec(-1L);
    runTestLongCodec(Long.MAX_VALUE);
    runTestLongCodec(Long.MIN_VALUE);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final long original = ThreadLocalRandom.current().nextLong();
      runTestLongCodec(original);
    }
    gc();
  }

  static void runTestLongCodec(long original) throws Exception {
    runTest(LongCodec.get(), original, Long.BYTES);
    runTestLongs(original);
  }

  /** Make sure that {@link LongCodec} and {@link Longs} are compatible. */
  static void runTestLongs(long original) {
    final LongCodec codec = LongCodec.get();
    final byte[] bytes = Longs.toByteArray(original);
    assertArrayEquals(bytes, codec.toPersistedFormat(original));
    assertEquals(original, Longs.fromByteArray(bytes));
    assertEquals(original, codec.fromPersistedFormat(bytes));
  }

  @Test
  public void testStringCodec() throws Exception {
    assertFalse(StringCodec.get().isFixedLength());
    runTestStringCodec("");

    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = "test" + ThreadLocalRandom.current().nextLong();
      final int serializedSize = runTestStringCodec(original);
      assertEquals(original.length(), serializedSize);
    }

    final String alphabets = "AbcdEfghIjklmnOpqrstUvwxyz";
    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = i == 0 ? alphabets : alphabets.substring(0, i);
      final int serializedSize = runTestStringCodec(original);
      assertEquals(original.length(), serializedSize);
    }

    final String[] docs = {
        "Ozone 是 Hadoop 的分布式对象存储系统，具有易扩展和冗余存储的特点。",
        "Ozone 不仅能存储数十亿个不同大小的对象，还支持在容器化环境（比如 Kubernetes）中运行。",
        "Apache Spark、Hive 和 YARN 等应用无需任何修改即可使用 Ozone。"
    };
    for (String original : docs) {
      final int serializedSize = runTestStringCodec(original);
      assertThat(original.length()).isLessThan(serializedSize);
    }

    final String multiByteChars = "官方发行包包括了源代码包和二进制代码包";
    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = i == 0 ? multiByteChars
          : multiByteChars.substring(0, i);
      final int serializedSize = runTestStringCodec(original);
      assertEquals(3 * original.length(), serializedSize);
    }

    gc();
  }

  static int runTestStringCodec(String original) throws Exception {
    final int serializedSize = UTF_8.encode(original).remaining();
    runTest(StringCodec.get(), original, serializedSize);
    return serializedSize;
  }

  @Test
  public void testFixedLengthStringCodec() throws Exception {
    assertTrue(FixedLengthStringCodec.get().isFixedLength());
    runTestFixedLengthStringCodec("");

    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = "test" + ThreadLocalRandom.current().nextLong();
      runTestFixedLengthStringCodec(original);
    }

    final String alphabets = "AbcdEfghIjklmnOpqrstUvwxyz";
    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = i == 0 ? alphabets : alphabets.substring(0, i);
      runTestFixedLengthStringCodec(original);
    }


    final String multiByteChars = "Ozone 是 Hadoop 的分布式对象存储系统，具有易扩展和冗余存储的特点。";
    assertThrows(IOException.class,
        tryCatch(() -> runTestFixedLengthStringCodec(multiByteChars)));
    assertThrows(IllegalStateException.class,
        tryCatch(() -> FixedLengthStringCodec.string2Bytes(multiByteChars)));

    gc();
  }

  @Test
  public void testByteStringCodec() throws Exception {
    for (int i = 0; i < 2; i++) {
      try (CodecBuffer empty = CodecBuffer.getEmptyBuffer()) {
        assertTrue(empty.isDirect());
      }
    }

    runTestByteStringCodec(ByteString.EMPTY);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = "test" + ThreadLocalRandom.current().nextLong();
      runTestByteStringCodec(ByteString.copyFromUtf8(original));
    }

    final String alphabets = "AbcdEfghIjklmnOpqrstUvwxyz";
    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = i == 0 ? alphabets : alphabets.substring(0, i);
      runTestByteStringCodec(ByteString.copyFromUtf8(original));
    }

    final String[] docs = {
        "Ozone 是 Hadoop 的分布式对象存储系统，具有易扩展和冗余存储的特点。",
        "Ozone 不仅能存储数十亿个不同大小的对象，还支持在容器化环境（比如 Kubernetes）中运行。",
        "Apache Spark、Hive 和 YARN 等应用无需任何修改即可使用 Ozone。"
    };
    for (String original : docs) {
      runTestByteStringCodec(ByteString.copyFromUtf8(original));
    }

    final String multiByteChars = "官方发行包包括了源代码包和二进制代码包";
    for (int i = 0; i < NUM_LOOPS; i++) {
      final String original = i == 0 ? multiByteChars
          : multiByteChars.substring(0, i);
      runTestByteStringCodec(ByteString.copyFromUtf8(original));
    }

    gc();
  }

  static void runTestByteStringCodec(ByteString original) throws Exception {
    runTest(ByteStringCodec.get(), original, original.size());
  }

  static Executable tryCatch(Executable executable) {
    return tryCatch(executable, t -> LOG.info("Good!", t));
  }

  static Executable tryCatch(Executable executable, Consumer<Throwable> log) {
    return () -> {
      try {
        executable.execute();
      } catch (Throwable t) {
        log.accept(t);
        throw t;
      }
    };
  }

  static void runTestFixedLengthStringCodec(String original) throws Exception {
    runTest(FixedLengthStringCodec.get(), original, original.length());
  }

  @Test
  public void testUuidCodec() throws Exception {
    final int size = UuidCodec.getSerializedSize();
    final UuidCodec codec = UuidCodec.get();
    runTest(codec, new UUID(0L, 0L), size);
    runTest(codec, new UUID(-1L, -1L), size);

    for (int i = 0; i < NUM_LOOPS; i++) {
      final UUID original = UUID.randomUUID();
      runTest(codec, original, size);
    }
    gc();
  }

  public static <T> void runTest(Codec<T> codec, T original,
      Integer serializedSize) throws Exception {
    CodecTestUtil.runTest(codec, original, serializedSize, null);
    runTestBytes(original, codec);
  }

  static <T> void runTestBytes(T object, Codec<T> codec) throws IOException {
    final byte[] array = codec.toPersistedFormat(object);
    final Bytes fromArray = new Bytes(array);

    try (CodecBuffer buffer = codec.toCodecBuffer(object,
        CodecBuffer.Allocator.HEAP)) {
      final Bytes fromBuffer = new Bytes(buffer);

      assertEquals(fromArray.hashCode(), fromBuffer.hashCode());
      assertEquals(fromArray, fromBuffer);
      assertEquals(fromBuffer, fromArray);
    }
  }
}
