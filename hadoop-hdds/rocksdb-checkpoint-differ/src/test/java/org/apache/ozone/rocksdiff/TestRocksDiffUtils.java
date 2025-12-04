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

package org.apache.ozone.rocksdiff;

import static org.apache.hadoop.hdds.StringUtils.getLexicographicallyHigherString;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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
        arguments("validColumnFamily", "validColumnFamily", "a", "d", "e", "g"),
        arguments("validColumnFamily", "validColumnFamily", "e", "g", "a", "d"),
        arguments("validColumnFamily", "validColumnFamily", "b", "b", "e", "g"),
        arguments("validColumnFamily", "validColumnFamily", "a", "d", "e", "e")
    );
  }

  @ParameterizedTest
  @MethodSource("values")
  public void testFilterRelevantSstFilesMap(String validSSTColumnFamilyName, String invalidColumnFamilyName,
      String validSSTFileStartRange, String validSSTFileEndRange, String invalidSSTFileStartRange,
      String invalidSSTFileEndRange) {
    String validSstFile = "filePath/validSSTFile.sst";
    String invalidSstFile = "filePath/invalidSSTFile.sst";
    String untrackedSstFile = "filePath/untrackedSSTFile.sst";
    String expectedPrefix = String.valueOf((char)(((int)validSSTFileEndRange.charAt(0) +
        validSSTFileStartRange.charAt(0)) / 2));
    Map<String, SstFileInfo> sstFile = ImmutableMap.of(
        validSstFile, new SstFileInfo(validSstFile, validSSTFileStartRange, validSSTFileEndRange,
            validSSTColumnFamilyName), invalidSstFile, new SstFileInfo(invalidSstFile, invalidSSTFileStartRange,
            invalidSSTFileEndRange, invalidColumnFamilyName), untrackedSstFile,
        new SstFileInfo(untrackedSstFile, null, null, null));
    Map<String, SstFileInfo> inputSstFiles = new HashMap<>();
    List<Set<String>> tablesToLookupSet = Arrays.asList(ImmutableSet.of(validSSTColumnFamilyName),
        ImmutableSet.of(invalidColumnFamilyName), ImmutableSet.of(validSSTColumnFamilyName, invalidColumnFamilyName),
        Collections.emptySet());
    for (Set<String> tablesToLookup : tablesToLookupSet) {
      inputSstFiles.clear();
      inputSstFiles.putAll(sstFile);
      RocksDiffUtils.filterRelevantSstFiles(inputSstFiles,
          tablesToLookup,
          new TablePrefixInfo(
              new HashMap<String, String>() {{
                put(invalidColumnFamilyName, getLexicographicallyHigherString(invalidSSTFileEndRange));
                put(validSSTColumnFamilyName, expectedPrefix);
              }}));
      if (tablesToLookup.contains(validSSTColumnFamilyName)) {
        Assertions.assertEquals(Sets.newTreeSet(validSstFile, untrackedSstFile), inputSstFiles.keySet(),
            "Failed for " + tablesToLookup);
      } else {
        Assertions.assertEquals(Sets.newTreeSet(untrackedSstFile), inputSstFiles.keySet(),
            "Failed for " + tablesToLookup);
      }
    }
  }
}
