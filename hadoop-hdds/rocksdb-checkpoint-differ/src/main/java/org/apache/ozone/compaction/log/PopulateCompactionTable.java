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

package org.apache.ozone.compaction.log;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Preconditions;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedSstFileReader;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods to populate compactionLogTable from a compaction-log file.
 */
public final class PopulateCompactionTable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PopulateCompactionTable.class);

  /**
   * Used during DAG construction.
   */
  private long reconstructionSnapshotCreationTime;
  private String reconstructionCompactionReason;
  private final String compactionLogDir;
  private ManagedRocksDB activeRocksDB;
  private ColumnFamilyHandle compactionLogTableCFHandle;

  public PopulateCompactionTable(String compactLogDir, ManagedRocksDB db, ColumnFamilyHandle cf) {
    compactionLogDir = compactLogDir;
    activeRocksDB = db;
    compactionLogTableCFHandle = cf;
    reconstructionSnapshotCreationTime = 0L;
    reconstructionCompactionReason = null;
  }

  public void setActiveRocksDB(ManagedRocksDB activeRocksDB) {
    this.activeRocksDB = activeRocksDB;
  }

  public void setCompactionLogTableCFHandle(ColumnFamilyHandle compactionLogTableCFHandle) {
    this.compactionLogTableCFHandle = compactionLogTableCFHandle;
  }

  public void addEntriesFromLogFilesToDagAndCompactionLogTable() {
    preconditionChecksForLoadAllCompactionLogs(true);
    try {
      try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
          .filter(e -> e.toString().toLowerCase()
              .endsWith(RocksDBConsts.COMPACTION_LOG_FILE_NAME_SUFFIX))
          .sorted()) {
        for (Path logPath : pathStream.collect(Collectors.toList())) {
          readCompactionLogFile(logPath.toString());
          // Delete the file once entries are added to compaction table
          // so that on next restart, only compaction log table is used.
          Files.delete(logPath);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error listing compaction log dir " +
          "compaction-log", e);
    }
  }

  /**
   * Helper to read compaction log file to the internal DAG and compaction log
   * table.
   */
  public void readCompactionLogFile(String currCompactionLogPath) {
    LOG.debug("Loading compaction log: {}", currCompactionLogPath);
    try (Stream<String> logLineStream =
             Files.lines(Paths.get(currCompactionLogPath), UTF_8)) {
      logLineStream.forEach(this::processCompactionLogLine);
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  /**
   * Process log line of compaction log text file input and populate the DAG.
   * It also adds the compaction log entry to compaction log table.
   */
  public void processCompactionLogLine(String line) {
    LOG.debug("Processing line: {}", line);
    if (line.startsWith(RocksDBConsts.COMPACTION_LOG_COMMENT_LINE_PREFIX)) {
      reconstructionCompactionReason =
          line.substring(RocksDBConsts.COMPACTION_LOG_COMMENT_LINE_PREFIX.length());
    } else if (line.startsWith(RocksDBConsts.COMPACTION_LOG_SEQ_NUM_LINE_PREFIX)) {
      reconstructionSnapshotCreationTime = getSnapshotCreationTimeFromLogLine(line);
    } else if (line.startsWith(RocksDBConsts.COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
      // Compaction log entry is like following:
      // C sequence_number input_files:output_files
      // where input_files and output_files are joined by ','.
      String[] lineSpilt = line.split(RocksDBConsts.SPACE_DELIMITER);
      if (lineSpilt.length != 3) {
        LOG.error("Invalid line in compaction log: {}", line);
        return;
      }

      String dbSequenceNumber = lineSpilt[1];
      String[] io = lineSpilt[2]
          .split(RocksDBConsts.COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER);
      if (io.length != 2) {
        if (line.endsWith(":")) {
          LOG.debug("Ignoring compaction log line for SST deletion");
        } else {
          LOG.error("Invalid line in compaction log: {}", line);
        }
        return;
      }

      String[] inputFiles = io[0].split(RocksDBConsts.COMPACTION_LOG_ENTRY_FILE_DELIMITER);
      String[] outputFiles = io[1].split(RocksDBConsts.COMPACTION_LOG_ENTRY_FILE_DELIMITER);
      CompactionLogEntry logEntry = createCompactionLogEntry(Long.parseLong(dbSequenceNumber),
          reconstructionSnapshotCreationTime, inputFiles, outputFiles, reconstructionCompactionReason);
      addToCompactionLogTable(logEntry, activeRocksDB, compactionLogTableCFHandle);
    } else {
      LOG.error("Invalid line in compaction log: {}", line);
    }
  }

  public CompactionLogEntry createCompactionLogEntry(
      long dbSequenceNumber, long creationTime,
      String[] inputFiles, String[] outputFiles,
      String compactionReason) {
    List<CompactionFileInfo> inputFileInfoList = Arrays.stream(inputFiles)
        .map(inputFile -> new CompactionFileInfo.Builder(inputFile).build())
        .collect(Collectors.toList());
    List<CompactionFileInfo> outputFileInfoList = Arrays.stream(outputFiles)
        .map(outputFile -> new CompactionFileInfo.Builder(outputFile).build())
        .collect(Collectors.toList());

    CompactionLogEntry.Builder builder =
        new CompactionLogEntry.Builder(dbSequenceNumber, creationTime,
            inputFileInfoList, outputFileInfoList);
    if (compactionReason != null) {
      builder.setCompactionReason(compactionReason);
    }
    return builder.build();
  }

  public void addToCompactionLogTable(CompactionLogEntry compactionLogEntry, ManagedRocksDB rocksDB,
                                      ColumnFamilyHandle cfHandle) {
    String dbSequenceIdStr =
        String.valueOf(compactionLogEntry.getDbSequenceNumber());

    if (dbSequenceIdStr.length() < RocksDBConsts.LONG_MAX_STR_LEN) {
      // Pad zeroes to the left to make sure it is lexicographic ordering.
      dbSequenceIdStr = org.apache.commons.lang3.StringUtils.leftPad(
          dbSequenceIdStr, RocksDBConsts.LONG_MAX_STR_LEN, "0");
    }

    // Key in the transactionId-currentTime
    // Just trxId can't be used because multiple compaction might be
    // running, and it is possible no new entry was added to DB.
    // Adding current time to transactionId eliminates key collision.
    String keyString = dbSequenceIdStr + "-" +
        compactionLogEntry.getCompactionTime();

    byte[] key = keyString.getBytes(UTF_8);
    byte[] value = compactionLogEntry.getProtobuf().toByteArray();
    try {
      rocksDB.get().put(cfHandle, key, value);
    } catch (RocksDBException exception) {
      // TODO: Revisit exception handling before merging the PR.
      throw new RuntimeException(exception);
    }
  }

  public long getSnapshotCreationTimeFromLogLine(String logLine) {
    // Remove `S ` from the line.
    String line = logLine.substring(RocksDBConsts.COMPACTION_LOG_SEQ_NUM_LINE_PREFIX.length());

    String[] splits = line.split(RocksDBConsts.SPACE_DELIMITER);
    Preconditions.checkArgument(splits.length == 3,
        "Snapshot info log statement has more than expected parameters.");

    return Long.parseLong(splits[2]);
  }

  public void preconditionChecksForLoadAllCompactionLogs(boolean checkCompactionLog) {
    if (checkCompactionLog) {
      Preconditions.checkNotNull(compactionLogDir,
          "Compaction log directory must be set.");
    }
    Preconditions.checkNotNull(compactionLogTableCFHandle,
        "compactionLogTableCFHandle must be set before calling " +
            "loadAllCompactionLogs.");
    Preconditions.checkNotNull(activeRocksDB,
        "activeRocksDB must be set before calling loadAllCompactionLogs.");
  }

  public long getSSTFileSummary(String filename, String dbPath)
      throws RocksDBException, FileNotFoundException {

    if (!filename.endsWith(".sst")) {
      filename += ".sst";
    }

    try (ManagedOptions option = new ManagedOptions();
         ManagedSstFileReader reader = new ManagedSstFileReader(option)) {

      reader.open(dbPath + "/" + filename);

      TableProperties properties = reader.getTableProperties();
      //if (LOG.isDebugEnabled()) {
      LOG.info("{} has {} keys", filename, properties.getNumEntries());
      //}
      return properties.getNumEntries();
    }
  }
}
