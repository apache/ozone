/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.rocksdiff;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.rocksdb.LiveFileMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;

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
    String firstKeyPrefix = constructBucketKey(firstDbKey);
    String endKeyPrefix = constructBucketKey(lastDbKey);
    return firstKeyPrefix.compareTo(prefixForColumnFamily) <= 0
        && prefixForColumnFamily.compareTo(endKeyPrefix) <= 0;
  }

  public static String constructBucketKey(String keyName) {
    if (!keyName.startsWith(OM_KEY_PREFIX)) {
      keyName = OM_KEY_PREFIX.concat(keyName);
    }
    String[] elements = keyName.split(OM_KEY_PREFIX);
    String volume = elements[1];
    String bucket = elements[2];
    StringBuilder builder =
        new StringBuilder().append(OM_KEY_PREFIX).append(volume);

    if (StringUtils.isNotBlank(bucket)) {
      builder.append(OM_KEY_PREFIX).append(bucket);
    }
    builder.append(OM_KEY_PREFIX);
    return builder.toString();
  }

  public static void filterRelevantSstFiles(Set<String> inputFiles,
                                            Map<String, String> tableToPrefixMap,
                                            ManagedRocksDB... dbs) {
    filterRelevantSstFiles(inputFiles, tableToPrefixMap, Collections.emptyMap(), dbs);
  }

  /**
   * Filter sst files based on prefixes.
   */
  public static void filterRelevantSstFiles(Set<String> inputFiles,
                                            Map<String, String> tableToPrefixMap,
                                            Map<String, CompactionNode> preExistingCompactionNodes,
                                            ManagedRocksDB... dbs) {
    Map<String, LiveFileMetaData> liveFileMetaDataMap = new HashMap<>();
    int dbIdx = 0;
    for (Iterator<String> fileIterator =
         inputFiles.iterator(); fileIterator.hasNext();) {
      String filename = FilenameUtils.getBaseName(fileIterator.next());
      while (!preExistingCompactionNodes.containsKey(filename) && !liveFileMetaDataMap.containsKey(filename)
          && dbIdx < dbs.length) {
        liveFileMetaDataMap.putAll(dbs[dbIdx].getLiveMetadataForSSTFiles());
        dbIdx += 1;
      }
      CompactionNode compactionNode = preExistingCompactionNodes.get(filename);
      if (compactionNode == null) {
        compactionNode = new CompactionNode(new CompactionFileInfo.Builder(filename)
            .setValues(liveFileMetaDataMap.get(filename)).build());
      }
      if (shouldSkipNode(compactionNode, tableToPrefixMap)) {
        fileIterator.remove();
      }
    }
  }

  @VisibleForTesting
  static boolean shouldSkipNode(CompactionNode node,
                                Map<String, String> columnFamilyToPrefixMap) {
    // This is for backward compatibility. Before the compaction log table
    // migration, startKey, endKey and columnFamily information is not persisted
    // in compaction log files.
    // Also for the scenario when there is an exception in reading SST files
    // for the file node.
    if (node.getStartKey() == null || node.getEndKey() == null ||
        node.getColumnFamily() == null) {
      LOG.debug("Compaction node with fileName: {} doesn't have startKey, " +
          "endKey and columnFamily details.", node.getFileName());
      return false;
    }

    if (MapUtils.isEmpty(columnFamilyToPrefixMap)) {
      LOG.debug("Provided columnFamilyToPrefixMap is null or empty.");
      return false;
    }

    if (!columnFamilyToPrefixMap.containsKey(node.getColumnFamily())) {
      LOG.debug("SstFile node: {} is for columnFamily: {} while filter map " +
              "contains columnFamilies: {}.", node.getFileName(),
          node.getColumnFamily(), columnFamilyToPrefixMap.keySet());
      return true;
    }

    String keyPrefix = columnFamilyToPrefixMap.get(node.getColumnFamily());
    return !isKeyWithPrefixPresent(keyPrefix, node.getStartKey(),
        node.getEndKey());
  }
}
