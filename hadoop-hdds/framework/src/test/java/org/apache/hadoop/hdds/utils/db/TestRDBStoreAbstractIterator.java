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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksObjectUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSlice;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.ratis.util.function.CheckedFunction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.rocksdb.RocksIterator;

/**
 * Abstract class for testing RDBStoreAbstractIterator.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class TestRDBStoreAbstractIterator<T extends RDBStoreAbstractIterator<?>> {

  private RocksIterator rocksDBIteratorMock;
  private CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> itrInitializer;
  private RDBTable rocksTableMock;
  private ManagedReadOptions readOptions;

  public RDBTable getRocksTableMock() {
    return rocksTableMock;
  }

  public RocksIterator getRocksDBIteratorMock() {
    return rocksDBIteratorMock;
  }

  public CheckedFunction<ManagedReadOptions, ManagedRocksIterator, RocksDatabaseException> getItrInitializer() {
    return itrInitializer;
  }

  @BeforeAll
  public static void init() {
    ManagedRocksObjectUtils.loadRocksDBLibrary();
  }

  @BeforeEach
  public void setup() {
    rocksDBIteratorMock = mock(RocksIterator.class);
    itrInitializer = readOpts -> {
      this.readOptions = readOpts;
      return new ManagedRocksIterator(rocksDBIteratorMock);
    };
    rocksTableMock = mock(RDBTable.class);
    Logger.getLogger(ManagedRocksObjectUtils.class).setLevel(Level.DEBUG);
  }

  abstract T newIterator() throws RocksDatabaseException;

  abstract T newIterator(byte[] prefix) throws RocksDatabaseException;

  public static Stream<Arguments> prefixTestArgumentsProvider() {
    byte[] randomBytes = RandomUtils.nextBytes(100);
    return Stream.of(
        Arguments.of("Empty prefix", new byte[0], null),
        Arguments.of("null prefix", null, null),
        Arguments.of("Prefix with 0", getArrayFilledWithValue(0, 1), getArrayFilledWithValue(1, 1)),
        Arguments.of("Prefix with 0 with 100 times", getArrayFilledWithValue(0, 100),
            getLastByteIncreased(getArrayFilledWithValue(0, 100))),
        Arguments.of("Prefix with 0xFF", getArrayFilledWithValue(0xFF, 1), null),
        Arguments.of("Prefix with 0xFF with 100 times", getArrayFilledWithValue(0xFF, 100), null),
        Arguments.of("Prefix with random bytes", randomBytes,
            getLastByteIncreased(Arrays.copyOf(randomBytes, randomBytes.length))),
        Arguments.of("Prefix with 0xFF prefixed 100 times and 0 at end",
            getArrayFilledWithValueAndPrefix(0, 1, getArrayFilledWithValue(0xFF, 100)),
            getArrayFilledWithValueAndPrefix(1, 1, getArrayFilledWithValue(0xFF, 100))),
        Arguments.of("Prefix with 0xFF prefixed 100 times and 50 at end",
            getArrayFilledWithValueAndPrefix(50, 1, getArrayFilledWithValue(0xFF, 100)),
            getArrayFilledWithValueAndPrefix(51, 1, getArrayFilledWithValue(0xFF, 100))),
        Arguments.of("Prefix with 0xFF prefixed 100 times and value 0xFE at end",
            getArrayFilledWithValueAndPrefix(0xFE, 1, getArrayFilledWithValue(0xFF, 100)),
            getArrayFilledWithValue(0xFF, 101)),
        Arguments.of("Prefix with 0 prefix 100 times and 100 0xFF at end",
            getArrayFilledWithValueAndPrefix(0xFF, 100, getArrayFilledWithValue(0, 100)),
            getLastByteIncreased(getArrayFilledWithValue(0, 100))),
        Arguments.of("Prefix with 50 prefix 100 times and 100 0xFF at end",
            getArrayFilledWithValueAndPrefix(0xFF, 100, getArrayFilledWithValue(50, 100)),
            getLastByteIncreased(getArrayFilledWithValue(50, 100))),
        Arguments.of("Prefix with 0xFE prefix 100 times and 100 0xFF at end",
            getArrayFilledWithValueAndPrefix(0xFF, 100, getArrayFilledWithValue(0xFE, 100)),
            getLastByteIncreased(getArrayFilledWithValue(0xFE, 100)))
    );
  }

  private static byte[] getArrayFilledWithValue(int value, int length) {
    return getArrayFilledWithValueAndPrefix(value, length, null);
  }

  private static byte[] getArrayFilledWithValueAndPrefix(int value, int length, byte[] prefix) {
    byte[] array = new byte[length + (prefix == null ? 0 : prefix.length)];
    if (prefix != null) {
      System.arraycopy(prefix, 0, array, 0, prefix.length);
    }
    Arrays.fill(array, prefix == null ? 0 : prefix.length, array.length, (byte) value);
    return array;
  }

  private static byte[] getLastByteIncreased(byte[] arr) {
    arr[arr.length - 1]++;
    return arr;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prefixTestArgumentsProvider")
  public void testNormalPrefixedIterator(String name, byte[] prefix, byte[] expectedUpperBound) throws Exception {
    try (T itr = newIterator(prefix);
         ManagedSlice lowerBound = prefix == null ? null : new ManagedSlice(prefix);
         ManagedSlice upperBound = expectedUpperBound == null ? null : new ManagedSlice(expectedUpperBound)) {
      Assertions.assertEquals(this.readOptions.getLowerBound(), lowerBound);
      Assertions.assertEquals(this.readOptions.getUpperBound(), upperBound);
      verify(rocksDBIteratorMock, times(1)).seekToFirst();
      clearInvocations(rocksDBIteratorMock);
      itr.seekToFirst();
      verify(rocksDBIteratorMock, times(1)).seekToFirst();
      clearInvocations(rocksDBIteratorMock);

      when(rocksDBIteratorMock.isValid()).thenReturn(true);
      when(rocksDBIteratorMock.key()).thenReturn(prefix);
      assertTrue(itr.hasNext());
      verify(rocksDBIteratorMock, times(1)).isValid();
      verify(rocksDBIteratorMock, times(0)).key();
      itr.seekToLast();
      verify(rocksDBIteratorMock, times(1)).seekToLast();
    }
    assertFalse(readOptions.isOwningHandle());
    assertFalse(Optional.ofNullable(readOptions.getLowerBound()).map(ManagedSlice::isOwningHandle).orElse(false));
    assertFalse(Optional.ofNullable(readOptions.getUpperBound()).map(ManagedSlice::isOwningHandle).orElse(false));
  }
}
