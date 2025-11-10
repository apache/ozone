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

import static org.apache.hadoop.hdds.StringUtils.getFirstNChars;

import com.google.common.annotations.VisibleForTesting;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for snap-diff operations.
 */
public final class RocksDiffUtils {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDiffUtils.class);

  private RocksDiffUtils() {
  }

  public static boolean isKeyWithPrefixPresent(String prefixForColumnFamily,
                                               String firstDbKey,
                                               String lastDbKey) {
    String firstKeyPrefix = getFirstNChars(firstDbKey, prefixForColumnFamily.length());
    String endKeyPrefix = getFirstNChars(lastDbKey, prefixForColumnFamily.length());
    return firstKeyPrefix.compareTo(prefixForColumnFamily) <= 0
        && prefixForColumnFamily.compareTo(endKeyPrefix) <= 0;
  }

  /**
   * Filter sst files based on prefixes.
   */
  public static <T> Map<T, SstFileInfo> filterRelevantSstFiles(Map<T, SstFileInfo> inputFiles,
      Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) {
    for (Iterator<Map.Entry<T, SstFileInfo>> fileIterator = inputFiles.entrySet().iterator(); fileIterator.hasNext();) {
      SstFileInfo sstFileInfo = fileIterator.next().getValue();
      if (shouldSkipNode(sstFileInfo, tablePrefixInfo, tablesToLookup)) {
        fileIterator.remove();
      }
    }
    return inputFiles;
  }

  /**
   * Filter sst files based on prefixes.
   */
  public static <T> Set<SstFileInfo> filterRelevantSstFiles(Set<SstFileInfo> inputFiles,
      Set<String> tablesToLookup, TablePrefixInfo tablePrefixInfo) {
    for (Iterator<SstFileInfo> fileIterator = inputFiles.iterator(); fileIterator.hasNext();) {
      SstFileInfo sstFileInfo = fileIterator.next();
      if (shouldSkipNode(sstFileInfo, tablePrefixInfo, tablesToLookup)) {
        fileIterator.remove();
      }
    }
    return inputFiles;
  }

  @VisibleForTesting
  static boolean shouldSkipNode(SstFileInfo node, TablePrefixInfo tablePrefixInfo, Set<String> columnFamiliesToLookup) {
    // This is for backward compatibility. Before the compaction log table
    // migration, startKey, endKey and columnFamily information is not persisted
    // in compaction log files.
    // Also for the scenario when there is an exception in reading SST files
    // for the file node.
    if (node.getStartKey() == null || node.getEndKey() == null || node.getColumnFamily() == null) {
      LOG.debug("Compaction node with fileName: {} doesn't have startKey, " +
          "endKey and columnFamily details.", node.getFileName());
      return false;
    }

    if (tablePrefixInfo.size() == 0) {
      LOG.debug("Provided tablePrefixInfo is null or empty.");
      return false;
    }

    if (!columnFamiliesToLookup.contains(node.getColumnFamily())) {
      LOG.debug("SstFile node: {} is for columnFamily: {} while filter map " +
              "contains columnFamilies: {}.", node.getFileName(),
          node.getColumnFamily(), tablePrefixInfo);
      return true;
    }

    String keyPrefix = tablePrefixInfo.getTablePrefix(node.getColumnFamily());
    return !isKeyWithPrefixPresent(keyPrefix, node.getStartKey(), node.getEndKey());
  }
}
