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

import static org.apache.hadoop.ozone.OzoneConsts.OM_KEY_PREFIX;
import static org.apache.ozone.rocksdiff.RocksDBCheckpointDiffer.SST_FILE_EXTENSION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.graph.MutableGraph;
import com.google.protobuf.InvalidProtocolBufferException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;
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

  /**
   * Get number of keys in an SST file.
   * @param filename absolute path of SST file
   * @return number of keys
   */
  public static long getSSTFileNumKeys(String filename) {

    try {
      if (!filename.endsWith(SST_FILE_EXTENSION)) {
        filename += SST_FILE_EXTENSION;
      }

      try (ManagedOptions option = new ManagedOptions();
           ManagedSstFileReader reader = new ManagedSstFileReader(option)) {

        reader.open(filename);

        TableProperties properties = reader.getTableProperties();
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} has {} keys", filename, properties.getNumEntries());
        }
        return properties.getNumEntries();
      }
    } catch (RocksDBException e) {
      LOG.warn("Can't get num of keys in SST '{}': {}", filename, e.getMessage());
    }
    return 0L;
  }

  /**
   * Helper method to add a new file node to the DAG.
   * @return CompactionNode
   */
  public static CompactionNode addNodeToDAG(String file, long seqNum, String startKey,
                                     String endKey, String columnFamily, long numKeys,
                                     MutableGraph<CompactionNode>... graph) {
    CompactionNode fileNode = new CompactionNode(file, numKeys,
        seqNum, startKey, endKey, columnFamily);
    for (MutableGraph<CompactionNode> g : graph) {
      if (g != null) {
        g.addNode(fileNode);
      }
    }
    return fileNode;
  }

  public static void populateCompactionDAG(List<CompactionFileInfo> inputFiles,
                                           List<CompactionFileInfo> outputFiles,
                                           long seqNum, ConcurrentHashMap<String, CompactionNode> compactionNodeMap,
                                           MutableGraph<CompactionNode> forwardCompactionDAG,
                                           MutableGraph<CompactionNode> backwardCompactionDAG) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (CompactionFileInfo outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(
          outfile.getFileName(),

          file -> RocksDiffUtils.addNodeToDAG(file, seqNum, outfile.getStartKey(),
              outfile.getEndKey(), outfile.getColumnFamily(), RocksDiffUtils.getSSTFileNumKeys(file),
              forwardCompactionDAG, backwardCompactionDAG));


      for (CompactionFileInfo infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(
            infile.getFileName(),

            file -> RocksDiffUtils.addNodeToDAG(file, seqNum, infile.getStartKey(),
                infile.getEndKey(), infile.getColumnFamily(), RocksDiffUtils.getSSTFileNumKeys(file),
                forwardCompactionDAG, backwardCompactionDAG));

        // Draw the edges
        if (!outfileNode.getFileName().equals(infileNode.getFileName())) {
          if (forwardCompactionDAG != null) {
            forwardCompactionDAG.putEdge(outfileNode, infileNode);
          }
          if (backwardCompactionDAG != null) {
            backwardCompactionDAG.putEdge(infileNode, outfileNode);
          }
        }
      }
    }
  }

  public static void createCompactionDags(ManagedRocksDB activeRocksDB, ColumnFamilyHandle compactionLogTableCFHandle,
                                          ConcurrentHashMap<String, CompactionNode> compactionNodeMap,
                                          MutableGraph<CompactionNode> forwardCompactionDAG,
                                          MutableGraph<CompactionNode> backwardCompactionDAG) {
    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(compactionLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();
      while (managedRocksIterator.get().isValid()) {
        byte[] value = managedRocksIterator.get().value();
        CompactionLogEntry compactionLogEntry =
            CompactionLogEntry.getFromProtobuf(
                HddsProtos.CompactionLogEntryProto.parseFrom(value));
        RocksDiffUtils.populateCompactionDAG(compactionLogEntry.getInputFileInfoList(),
            compactionLogEntry.getOutputFileInfoList(),
            compactionLogEntry.getDbSequenceNumber(),
            compactionNodeMap, forwardCompactionDAG, backwardCompactionDAG);
        managedRocksIterator.get().next();
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
