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

import com.google.common.graph.MutableGraph;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for creating compaction DAGs.
 */
public class CompactionDagHelper {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionDagHelper.class);
  static final String SST_FILE_EXTENSION = ".sst";

  private final ConcurrentHashMap<String, CompactionNode> compactionNodeMap;

  private final MutableGraph<CompactionNode> forwardCompactionDAG;
  private final MutableGraph<CompactionNode> backwardCompactionDAG;

  private ColumnFamilyHandle compactionLogTableCFHandle;
  private ManagedRocksDB activeRocksDB;

  private String sstBackupDir;
  private String activeDBLocation;
  private boolean numKeys;

  @SuppressWarnings("checkstyle:parameternumber")
  public CompactionDagHelper(ManagedRocksDB activeDB, ColumnFamilyHandle compactionLogTableCFHandle,
                             ConcurrentHashMap<String, CompactionNode> compactionMap,
                             MutableGraph<CompactionNode> forwardDAG,
                             MutableGraph<CompactionNode> backwardDAG,
                             String sstBackup, String activeLocation, boolean numKeysIncluded) {
    this.activeRocksDB = activeDB;
    this.compactionLogTableCFHandle = compactionLogTableCFHandle;
    this.compactionNodeMap = compactionMap;
    this.forwardCompactionDAG = forwardDAG;
    this.backwardCompactionDAG = backwardDAG;
    this.sstBackupDir = sstBackup;
    this.activeDBLocation = activeLocation;
    this.numKeys = numKeysIncluded;
  }

  public MutableGraph<CompactionNode> getBackwardCompactionDAG() {
    return backwardCompactionDAG;
  }

  /**
   * Helper method to add a new file node to the DAGs.
   * @return CompactionNode
   */
  private CompactionNode addNodeToDAG(String file, long seqNum, String startKey,
                                     String endKey, String columnFamily, long keys,
                                     MutableGraph<CompactionNode>... graph) {
    CompactionNode fileNode = new CompactionNode(file, keys,
        seqNum, startKey, endKey, columnFamily);
    for (MutableGraph<CompactionNode> g : graph) {
      if (g != null) {
        g.addNode(fileNode);
      }
    }
    return fileNode;
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   * @param inputFiles List of compaction input files.
   * @param outputFiles List of compaction output files.
   * @param seqNum DB transaction sequence number.
   */
  public void populateCompactionDAG(List<CompactionFileInfo> inputFiles,
                                    List<CompactionFileInfo> outputFiles,
                                    long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (CompactionFileInfo outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(
          outfile.getFileName(),

          file -> addNodeToDAG(file, seqNum, outfile.getStartKey(),
              outfile.getEndKey(), outfile.getColumnFamily(), (numKeys ? getSSTFileNumKeys(file) : 0L),
              forwardCompactionDAG, backwardCompactionDAG));


      for (CompactionFileInfo infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(
            infile.getFileName(),

            file -> addNodeToDAG(file, seqNum, infile.getStartKey(),
                infile.getEndKey(), infile.getColumnFamily(), (numKeys ? getSSTFileNumKeys(file) : 0L),
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

  /**
   * Read a compactionLofTable and create entries in the dags.
   */
  public void createCompactionDags() {
    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(compactionLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();
      while (managedRocksIterator.get().isValid()) {
        byte[] value = managedRocksIterator.get().value();
        CompactionLogEntry compactionLogEntry =
            CompactionLogEntry.getFromProtobuf(
                HddsProtos.CompactionLogEntryProto.parseFrom(value));
        populateCompactionDAG(compactionLogEntry.getInputFileInfoList(),
            compactionLogEntry.getOutputFileInfoList(),
            compactionLogEntry.getDbSequenceNumber());
        managedRocksIterator.get().next();
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }


  private String getAbsoluteSstFilePath(String filename)
      throws FileNotFoundException {
    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      filename += SST_FILE_EXTENSION;
    }
    File sstFile = new File(sstBackupDir + filename);
    File sstFileInActiveDB = new File(activeDBLocation + filename);
    if (sstFile.exists()) {
      return sstBackupDir + filename;
    } else if (sstFileInActiveDB.exists()) {
      return activeDBLocation + filename;
    } else {
      throw new FileNotFoundException("Can't find SST file: " + filename);
    }
  }

  /**
   * Get number of keys in an SST file.
   * @param filename absolute path of SST file
   * @return number of keys
   */
  private long getSSTFileNumKeys(String filename) {

    try {
      if (!filename.endsWith(SST_FILE_EXTENSION)) {
        filename += SST_FILE_EXTENSION;
      }

      try (ManagedOptions option = new ManagedOptions();
           ManagedSstFileReader reader = new ManagedSstFileReader(option)) {

        reader.open(getAbsoluteSstFilePath(filename));

        TableProperties properties = reader.getTableProperties();
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} has {} keys", filename, properties.getNumEntries());
        }
        return properties.getNumEntries();
      }
    } catch (RocksDBException e) {
      LOG.warn("Can't get num of keys in SST '{}': {}", filename, e.getMessage());
    } catch (FileNotFoundException e) {
      LOG.info("Can't find SST '{}'", filename);
    }
    return 0L;
  }
}
