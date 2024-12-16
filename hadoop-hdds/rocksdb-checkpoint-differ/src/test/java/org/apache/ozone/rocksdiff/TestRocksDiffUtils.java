/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ozone.rocksdiff;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * Class to test RocksDiffUtils.
 */
public class TestRocksDiffUtils {
  @Test
  public void testFilterFunction() {
    assertTrue(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket1/",
        "/vol1/bucket1/key1",
        "/vol1/bucket1/key1"));
    assertTrue(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket3/",
        "/vol1/bucket1/key1",
        "/vol1/bucket5/key1"));
    assertFalse(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket5/",
        "/vol1/bucket1/key1",
        "/vol1/bucket4/key9"));
    assertFalse(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket2/",
        "/vol1/bucket1/key1",
        "/vol1/bucket1/key1"));
    assertFalse(RocksDiffUtils.isKeyWithPrefixPresent(
        "/vol1/bucket/",
        "/vol1/bucket1/key1",
        "/vol1/bucket1/key1"));
    assertTrue(RocksDiffUtils.isKeyWithPrefixPresent(
        "/volume/bucket/",
        "/volume/bucket/key-1",
        "/volume/bucket2/key-97"));
  }

  public static Stream<Arguments> values() {
    return Stream.of(
        arguments("validColumnFamily", "invalidColumnFamily", "a", "d", "b", "f"),
        arguments("validColumnFamily", "invalidColumnFamily", "a", "d", "e", "f"),
        arguments("validColumnFamily", "invalidColumnFamily", "a", "d", "a", "f"),
        arguments("validColumnFamily", "validColumnFamily", "a", "d", "e", "g"),
        arguments("validColumnFamily", "validColumnFamily", "e", "g", "a", "d"),
        arguments("validColumnFamily", "validColumnFamily", "b", "b", "e", "g"),
        arguments("validColumnFamily", "validColumnFamily", "a", "d", "e", "e")
    );
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testFilterRelevantSstFilesWithPreExistingCompactionInfo(String validSSTColumnFamilyName,
                                                                      String invalidColumnFamilyName,
                                                                      String validSSTFileStartRange,
                                                                      String validSSTFileEndRange,
                                                                      String invalidSSTFileStartRange,
                                                                      String invalidSSTFileEndRange) {
    try (MockedStatic<RocksDiffUtils> mockedHandler = Mockito.mockStatic(RocksDiffUtils.class,
        Mockito.CALLS_REAL_METHODS)) {
      mockedHandler.when(() -> RocksDiffUtils.constructBucketKey(anyString())).thenAnswer(i -> i.getArgument(0));
      String validSstFile = "filePath/validSSTFile.sst";
      String invalidSstFile = "filePath/invalidSSTFile.sst";
      String untrackedSstFile = "filePath/untrackedSSTFile.sst";
      String expectedPrefix = String.valueOf((char)(((int)validSSTFileEndRange.charAt(0) +
          validSSTFileStartRange.charAt(0)) / 2));
      Set<String> sstFile = Sets.newTreeSet(validSstFile, invalidSstFile, untrackedSstFile);
      RocksDiffUtils.filterRelevantSstFiles(sstFile, ImmutableMap.of(validSSTColumnFamilyName, expectedPrefix),
          ImmutableMap.of("validSSTFile", new CompactionNode(validSstFile, 0, 0, validSSTFileStartRange,
                  validSSTFileEndRange, validSSTColumnFamilyName), "invalidSSTFile",
              new CompactionNode(invalidSstFile, 0, 0, invalidSSTFileStartRange,
                  invalidSSTFileEndRange, invalidColumnFamilyName)));
      Assertions.assertEquals(Sets.newTreeSet(validSstFile, untrackedSstFile), sstFile);
    }

  }

  private LiveFileMetaData getMockedLiveFileMetadata(String columnFamilyName, String startRange,
                                                     String endRange,
                                                     String name) {
    LiveFileMetaData liveFileMetaData = Mockito.mock(LiveFileMetaData.class);
    Mockito.when(liveFileMetaData.largestKey()).thenReturn(endRange.getBytes(StandardCharsets.UTF_8));
    Mockito.when(liveFileMetaData.columnFamilyName()).thenReturn(columnFamilyName.getBytes(StandardCharsets.UTF_8));
    Mockito.when(liveFileMetaData.smallestKey()).thenReturn(startRange.getBytes(StandardCharsets.UTF_8));
    Mockito.when(liveFileMetaData.fileName()).thenReturn("basePath/" + name + ".sst");
    return liveFileMetaData;
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testFilterRelevantSstFilesFromDB(String validSSTColumnFamilyName,
                                               String invalidColumnFamilyName,
                                               String validSSTFileStartRange,
                                               String validSSTFileEndRange,
                                               String invalidSSTFileStartRange,
                                               String invalidSSTFileEndRange) {
    try (MockedStatic<RocksDiffUtils> mockedHandler = Mockito.mockStatic(RocksDiffUtils.class,
        Mockito.CALLS_REAL_METHODS)) {
      mockedHandler.when(() -> RocksDiffUtils.constructBucketKey(anyString())).thenAnswer(i -> i.getArgument(0));
      for (int numberOfDBs = 1; numberOfDBs < 10; numberOfDBs++) {
        String validSstFile = "filePath/validSSTFile.sst";
        String invalidSstFile = "filePath/invalidSSTFile.sst";
        String untrackedSstFile = "filePath/untrackedSSTFile.sst";
        int expectedDBKeyIndex = numberOfDBs / 2;
        ManagedRocksDB[] rocksDBs =
            IntStream.range(0, numberOfDBs).mapToObj(i -> Mockito.mock(ManagedRocksDB.class))
                .collect(Collectors.toList()).toArray(new ManagedRocksDB[numberOfDBs]);
        for (int i = 0; i < numberOfDBs; i++) {
          ManagedRocksDB managedRocksDB = rocksDBs[i];
          RocksDB mockedRocksDB = Mockito.mock(RocksDB.class);
          Mockito.when(managedRocksDB.get()).thenReturn(mockedRocksDB);
          if (i == expectedDBKeyIndex) {
            LiveFileMetaData validLiveFileMetaData = getMockedLiveFileMetadata(validSSTColumnFamilyName,
                validSSTFileStartRange, validSSTFileEndRange, "validSSTFile");
            LiveFileMetaData invalidLiveFileMetaData = getMockedLiveFileMetadata(invalidColumnFamilyName,
                invalidSSTFileStartRange, invalidSSTFileEndRange, "invalidSSTFile");
            List<LiveFileMetaData> liveFileMetaDatas = Arrays.asList(validLiveFileMetaData, invalidLiveFileMetaData);
            Mockito.when(mockedRocksDB.getLiveFilesMetaData()).thenReturn(liveFileMetaDatas);
          } else {
            Mockito.when(mockedRocksDB.getLiveFilesMetaData()).thenReturn(Collections.emptyList());
          }
          Mockito.when(managedRocksDB.getLiveMetadataForSSTFiles())
              .thenAnswer(invocation -> ManagedRocksDB.getLiveMetadataForSSTFiles(mockedRocksDB));
        }

        String expectedPrefix = String.valueOf((char)(((int)validSSTFileEndRange.charAt(0) +
            validSSTFileStartRange.charAt(0)) / 2));
        Set<String> sstFile = Sets.newTreeSet(validSstFile, invalidSstFile, untrackedSstFile);
        RocksDiffUtils.filterRelevantSstFiles(sstFile, ImmutableMap.of(validSSTColumnFamilyName, expectedPrefix),
            Collections.emptyMap(), rocksDBs);
        Assertions.assertEquals(Sets.newTreeSet(validSstFile, untrackedSstFile), sstFile);
      }

    }

  }
}
