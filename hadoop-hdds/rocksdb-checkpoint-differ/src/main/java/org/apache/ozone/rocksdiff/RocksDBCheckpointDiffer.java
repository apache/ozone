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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.CompactionLogEntryProto;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedReadOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.rocksdb.util.RdbUtil;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ozone.graph.PrintableGraph.GraphType;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
import org.rocksdb.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT;

/**
 * RocksDB checkpoint differ.
 * <p>
 * Implements Ozone Manager RocksDB compaction listener (compaction log
 * persistence and SST file hard-linking), compaction DAG construction,
 * and compaction DAG reconstruction upon OM restarts.
 * <p>
 * It is important to note that compaction log is per-DB instance. Since
 * each OM DB instance might trigger compactions at different timings.
 */
public class RocksDBCheckpointDiffer implements AutoCloseable,
    BootstrapStateHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  private final String metadataDir;
  private final String sstBackupDir;
  private final String activeDBLocationStr;

  private final String compactionLogDir;

  public static final String COMPACTION_LOG_FILE_NAME_SUFFIX = ".log";

  /**
   * Marks the beginning of a comment line in the compaction log.
   */
  private static final String COMPACTION_LOG_COMMENT_LINE_PREFIX = "# ";

  /**
   * Marks the beginning of a compaction log entry.
   */
  private static final String COMPACTION_LOG_ENTRY_LINE_PREFIX = "C ";

  /**
   * Prefix for the sequence number line when writing to compaction log
   * right after taking an Ozone snapshot.
   */
  private static final String COMPACTION_LOG_SEQ_NUM_LINE_PREFIX = "S ";

  /**
   * Delimiter use to join compaction's input and output files.
   * e.g. input1,input2,input3 or output1,output2,output3
   */
  private static final String COMPACTION_LOG_ENTRY_FILE_DELIMITER = ",";

  private static final String SPACE_DELIMITER = " ";

  /**
   * Delimiter use to join compaction's input and output file set strings.
   * e.g. input1,input2,input3:output1,output2,output3
   */
  private static final String COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER
      = ":";

  /**
   * SST file extension. Must be lower case.
   * Used to trim the file extension when writing compaction entries to the log
   * to save space.
   */
  static final String SST_FILE_EXTENSION = ".sst";
  public static final int SST_FILE_EXTENSION_LENGTH =
      SST_FILE_EXTENSION.length();

  private static final int LONG_MAX_STR_LEN =
      String.valueOf(Long.MAX_VALUE).length();

  /**
   * Used during DAG reconstruction.
   */
  private long reconstructionSnapshotCreationTime;
  private String reconstructionCompactionReason;

  private final Scheduler scheduler;
  private volatile boolean closed;
  private final long maxAllowedTimeInDag;
  private final BootstrapStateHandler.Lock lock
      = new BootstrapStateHandler.Lock();

  private ColumnFamilyHandle snapshotInfoTableCFHandle;
  private final AtomicInteger tarballRequestCount;
  private final String dagPruningServiceName = "CompactionDagPruningService";
  private AtomicBoolean suspended;

  private ColumnFamilyHandle compactionLogTableCFHandle;
  private RocksDB activeRocksDB;

  /**
   * For snapshot diff calculation we only need to track following column
   * families. Other column families are irrelevant for snapshot diff.
   */
  public static final Set<String> COLUMN_FAMILIES_TO_TRACK_IN_DAG =
      ImmutableSet.of("keyTable", "directoryTable", "fileTable");

  /**
   * This is a package private constructor and should not be used other than
   * testing. Caller should use RocksDBCheckpointDifferHolder#getInstance() to
   * get RocksDBCheckpointDiffer instance.
   * Note that previous compaction logs are loaded by RDBStore after this
   * object's initialization by calling loadAllCompactionLogs().
   *
   * @param metadataDirName Ozone metadata directory.
   * @param sstBackupDirName Name of the SST backup dir under metadata dir.
   * @param compactionLogDirName Name of the compaction log dir.
   * @param activeDBLocationName Active RocksDB directory's location.
   * @param configuration ConfigurationSource.
   */
  @VisibleForTesting
  RocksDBCheckpointDiffer(String metadataDirName,
                          String sstBackupDirName,
                          String compactionLogDirName,
                          String activeDBLocationName,
                          ConfigurationSource configuration) {
    Preconditions.checkNotNull(metadataDirName);
    Preconditions.checkNotNull(sstBackupDirName);
    Preconditions.checkNotNull(compactionLogDirName);
    Preconditions.checkNotNull(activeDBLocationName);

    this.metadataDir = metadataDirName;
    this.compactionLogDir =
        createCompactionLogDir(metadataDirName, compactionLogDirName);
    this.sstBackupDir = Paths.get(metadataDirName, sstBackupDirName) + "/";
    createSstBackUpDir();

    // Active DB location is used in getSSTFileSummary
    this.activeDBLocationStr = activeDBLocationName + "/";
    this.maxAllowedTimeInDag = configuration.getTimeDuration(
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED,
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.suspended = new AtomicBoolean(false);

    long pruneCompactionDagDaemonRunIntervalInMs =
        configuration.getTimeDuration(
            OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL,
            OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    if (pruneCompactionDagDaemonRunIntervalInMs > 0) {
      this.scheduler = new Scheduler(dagPruningServiceName,
          true, 1);

      this.scheduler.scheduleWithFixedDelay(
          this::pruneOlderSnapshotsWithCompactionHistory,
          pruneCompactionDagDaemonRunIntervalInMs,
          pruneCompactionDagDaemonRunIntervalInMs,
          TimeUnit.MILLISECONDS);

      this.scheduler.scheduleWithFixedDelay(
          this::pruneSstFiles,
          pruneCompactionDagDaemonRunIntervalInMs,
          pruneCompactionDagDaemonRunIntervalInMs,
          TimeUnit.MILLISECONDS
      );
    } else {
      this.scheduler = null;
    }
    this.tarballRequestCount = new AtomicInteger(0);
  }

  private String createCompactionLogDir(String metadataDirName,
                                        String compactionLogDirName) {

    final File parentDir = new File(metadataDirName);
    if (!parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        LOG.error("Error creating compaction log parent dir.");
        return null;
      }
    }

    final String compactionLogDirectory =
        Paths.get(metadataDirName, compactionLogDirName).toString();
    File clDir = new File(compactionLogDirectory);
    if (!clDir.exists() && !clDir.mkdir()) {
      LOG.error("Error creating compaction log dir.");
      return null;
    }

    // Create a readme file explaining what the compaction log dir is for
    final Path readmePath = Paths.get(compactionLogDirectory, "_README.txt");
    final File readmeFile = new File(readmePath.toString());
    if (!readmeFile.exists()) {
      try (BufferedWriter bw = Files.newBufferedWriter(
          readmePath, StandardOpenOption.CREATE)) {
        bw.write("This directory holds Ozone Manager RocksDB compaction" +
            " logs.\nDO NOT add, change or delete any files in this directory" +
            " unless you know what you are doing.\n");
      } catch (IOException ignored) {
      }
    }

    // Append '/' to make it dir.
    return compactionLogDirectory + "/";
  }

  /**
   * Create the directory if SST backup path does not already exist.
   */
  private void createSstBackUpDir() {
    File dir = new File(this.sstBackupDir);
    if (!dir.exists() && !dir.mkdir()) {
      String errorMsg = "Failed to create SST file backup directory. "
          + "Check if OM has write permission.";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
  }

  @Override
  public void close() throws Exception {
    if (!closed) {
      synchronized (this) {
        if (!closed) {
          closed = true;
          if (scheduler != null) {
            LOG.info("Shutting down {}.", dagPruningServiceName);
            scheduler.close();
          }
        }
      }
    }
  }

  // Hash table to track CompactionNode for a given SST File.
  private final ConcurrentHashMap<String, CompactionNode> compactionNodeMap =
      new ConcurrentHashMap<>();

  // We are maintaining a two way DAG. This allows easy traversal from
  // source snapshot to destination snapshot as well as the other direction.

  private final MutableGraph<CompactionNode> forwardCompactionDAG =
      GraphBuilder.directed().build();

  private final MutableGraph<CompactionNode> backwardCompactionDAG =
      GraphBuilder.directed().build();

  public static final Integer DEBUG_DAG_BUILD_UP = 2;
  public static final Integer DEBUG_DAG_TRAVERSAL = 3;
  public static final Integer DEBUG_DAG_LIVE_NODES = 4;
  public static final Integer DEBUG_READ_ALL_DB_KEYS = 5;
  private static final HashSet<Integer> DEBUG_LEVEL = new HashSet<>();

  static {
    addDebugLevel(DEBUG_DAG_BUILD_UP);
    addDebugLevel(DEBUG_DAG_TRAVERSAL);
    addDebugLevel(DEBUG_DAG_LIVE_NODES);
  }

  static {
    RocksDB.loadLibrary();
  }

  public static void addDebugLevel(Integer level) {
    DEBUG_LEVEL.add(level);
  }

  /**
   * Takes {@link org.rocksdb.Options}.
   */
  public void setRocksDBForCompactionTracking(Options rocksOptions,
      List<AbstractEventListener> list) {
    list.add(newCompactionBeginListener());
    list.add(newCompactionCompletedListener());
    rocksOptions.setListeners(list);
  }

  public void setRocksDBForCompactionTracking(Options rocksOptions) {
    setRocksDBForCompactionTracking(rocksOptions, new ArrayList<>());
  }

  /**
   * Takes {@link org.rocksdb.DBOptions}.
   */
  public void setRocksDBForCompactionTracking(DBOptions rocksOptions,
      List<AbstractEventListener> list) {
    list.add(newCompactionBeginListener());
    list.add(newCompactionCompletedListener());
    rocksOptions.setListeners(list);
  }

  public void setRocksDBForCompactionTracking(DBOptions rocksOptions) {
    setRocksDBForCompactionTracking(rocksOptions, new ArrayList<>());
  }

  /**
   * Set SnapshotInfoTable DB column family handle to be used in DB listener.
   * @param snapshotInfoTableCFHandle ColumnFamilyHandle
   */
  public void setSnapshotInfoTableCFHandle(
      ColumnFamilyHandle snapshotInfoTableCFHandle) {
    Preconditions.checkNotNull(snapshotInfoTableCFHandle,
        "Column family handle should not be null");
    this.snapshotInfoTableCFHandle = snapshotInfoTableCFHandle;
  }

  /**
   * Set CompactionLogTable DB column family handle to access the table.
   * @param compactionLogTableCFHandle ColumnFamilyHandle
   */
  public synchronized void setCompactionLogTableCFHandle(
      ColumnFamilyHandle compactionLogTableCFHandle) {
    Preconditions.checkNotNull(compactionLogTableCFHandle,
        "Column family handle should not be null");
    this.compactionLogTableCFHandle = compactionLogTableCFHandle;
  }

  /**
   * Set activeRocksDB to access CompactionLogTable.
   * @param activeRocksDB RocksDB
   */
  public synchronized void setActiveRocksDB(RocksDB activeRocksDB) {
    Preconditions.checkNotNull(activeRocksDB, "RocksDB should not be null.");
    this.activeRocksDB = activeRocksDB;
  }

  /**
   * Helper method to check whether the SnapshotInfoTable column family is empty
   * in a given DB instance.
   * @param db RocksDB instance
   * @return true when column family is empty, false otherwise
   */
  private boolean isSnapshotInfoTableEmpty(RocksDB db) {
    // Can't use metadataManager.getSnapshotInfoTable().isEmpty() or use
    // any wrapper classes here. Any of those introduces circular dependency.
    // The solution here is to use raw RocksDB API.

    // There is this small gap when the db is open but the handle is not yet set
    // in RDBStore. Compaction could theoretically happen during that small
    // window. This condition here aims to handle that (falls back to not
    // skipping compaction tracking).
    if (snapshotInfoTableCFHandle == null) {
      LOG.warn("Snapshot info table column family handle is not set!");
      // Proceed to log compaction in this case
      return false;
    }

    // SnapshotInfoTable has table cache. But that wouldn't matter in this case
    // because the first SnapshotInfo entry would have been written to the DB
    // right before checkpoint happens in OMSnapshotCreateResponse.
    //
    // Note the goal of compaction DAG is to track all compactions that happened
    // _after_ a DB checkpoint is taken.

    try (ManagedRocksIterator it = ManagedRocksIterator.managed(
        db.newIterator(snapshotInfoTableCFHandle))) {
      it.get().seekToFirst();
      return !it.get().isValid();
    }
  }

  @VisibleForTesting
  boolean shouldSkipCompaction(byte[] columnFamilyBytes,
                               List<String> inputFiles,
                               List<String> outputFiles) {
    String columnFamily = StringUtils.bytes2String(columnFamilyBytes);

    if (!COLUMN_FAMILIES_TO_TRACK_IN_DAG.contains(columnFamily)) {
      LOG.debug("Skipping compaction for columnFamily: {}", columnFamily);
      return true;
    }

    if (inputFiles.isEmpty()) {
      LOG.debug("Compaction input files list is empty");
      return true;
    }

    if (new HashSet<>(inputFiles).equals(new HashSet<>(outputFiles))) {
      LOG.info("Skipped the compaction entry. Compaction input files: " +
          "{} and output files: {} are same.", inputFiles, outputFiles);
      return true;
    }

    return false;
  }

  private AbstractEventListener newCompactionBeginListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionBegin(RocksDB db,
                                    CompactionJobInfo compactionJobInfo) {
        if (shouldSkipCompaction(compactionJobInfo.columnFamilyName(),
            compactionJobInfo.inputFiles(),
            compactionJobInfo.outputFiles())) {
          return;
        }

        synchronized (this) {
          if (closed) {
            return;
          }

          // Skip compaction DAG tracking if the snapshotInfoTable is empty.
          // i.e. No snapshot exists in OM.
          if (isSnapshotInfoTableEmpty(db)) {
            return;
          }
        }

        for (String file : compactionJobInfo.inputFiles()) {
          createLink(Paths.get(sstBackupDir, new File(file).getName()),
              Paths.get(file));
        }
      }
    };
  }


  private AbstractEventListener newCompactionCompletedListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(RocksDB db,
                                        CompactionJobInfo compactionJobInfo) {
        if (shouldSkipCompaction(compactionJobInfo.columnFamilyName(),
            compactionJobInfo.inputFiles(),
            compactionJobInfo.outputFiles())) {
          return;
        }

        long trxId = db.getLatestSequenceNumber();

        CompactionLogEntry.Builder builder;
        try (ManagedOptions options = new ManagedOptions();
             ManagedReadOptions readOptions = new ManagedReadOptions()) {
          builder = new CompactionLogEntry.Builder(trxId,
              System.currentTimeMillis(),
              toFileInfoList(compactionJobInfo.inputFiles(), options,
                  readOptions),
              toFileInfoList(compactionJobInfo.outputFiles(), options,
                  readOptions));
        }

        if (LOG.isDebugEnabled()) {
          builder = builder.setCompactionReason(
              compactionJobInfo.compactionReason().toString());
        }

        CompactionLogEntry compactionLogEntry = builder.build();

        synchronized (this) {
          if (closed) {
            return;
          }

          // Skip compaction DAG tracking if the snapshotInfoTable is empty.
          // i.e. No snapshot exists in OM.
          if (isSnapshotInfoTableEmpty(db)) {
            return;
          }

          waitForTarballCreation();

          // Add the compaction log entry to Compaction log table.
          addToCompactionLogTable(compactionLogEntry);

          // Populate the DAG
          populateCompactionDAG(compactionLogEntry.getInputFileInfoList(),
              compactionLogEntry.getOutputFileInfoList(),
              compactionLogEntry.getDbSequenceNumber());
        }
      }
    };
  }

  @VisibleForTesting
  void addToCompactionLogTable(CompactionLogEntry compactionLogEntry) {
    String dbSequenceIdStr =
        String.valueOf(compactionLogEntry.getDbSequenceNumber());

    if (dbSequenceIdStr.length() < LONG_MAX_STR_LEN) {
      // Pad zeroes to the left to make sure it is lexicographic ordering.
      dbSequenceIdStr = org.apache.commons.lang3.StringUtils.leftPad(
          dbSequenceIdStr, LONG_MAX_STR_LEN, "0");
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
      activeRocksDB.put(compactionLogTableCFHandle, key, value);
    } catch (RocksDBException exception) {
      // TODO: Revisit exception handling before merging the PR.
      throw new RuntimeException(exception);
    }
  }

  /**
   * Check if there is any in_progress tarball creation request and wait till
   * all tarball creation finish, and it gets notified.
   */
  private void waitForTarballCreation() {
    while (tarballRequestCount.get() != 0) {
      try {
        wait(Integer.MAX_VALUE);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("Compaction log thread {} is interrupted.",
            Thread.currentThread().getName());
      }
    }
  }

  /**
   * Creates a hard link between provided link and source.
   * It doesn't throw any exception if {@link Files#createLink} throws
   * {@link FileAlreadyExistsException} while creating hard link.
   */
  private void createLink(Path link, Path source) {
    try {
      Files.createLink(link, source);
    } catch (FileAlreadyExistsException ignored) {
      // This could happen if another thread tried to create the same hard link
      // and succeeded.
      LOG.debug("SST file already exists: {}", source);
    } catch (IOException e) {
      LOG.error("Exception in creating hard link for {}", source);
      throw new RuntimeException("Failed to create hard link", e);
    }
  }

  /**
   * Get number of keys in an SST file.
   * @param filename SST filename
   * @return number of keys
   */
  private long getSSTFileSummary(String filename)
      throws RocksDBException, FileNotFoundException {

    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      filename += SST_FILE_EXTENSION;
    }

    try (
        ManagedOptions option = new ManagedOptions();
        SstFileReader reader = new SstFileReader(option)) {

      reader.open(getAbsoluteSstFilePath(filename));

      TableProperties properties = reader.getTableProperties();
      if (LOG.isDebugEnabled()) {
        LOG.debug("{} has {} keys", filename, properties.getNumEntries());
      }
      return properties.getNumEntries();
    }
  }

  private String getAbsoluteSstFilePath(String filename)
      throws FileNotFoundException {
    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      filename += SST_FILE_EXTENSION;
    }
    File sstFile = new File(sstBackupDir + filename);
    File sstFileInActiveDB = new File(activeDBLocationStr + filename);
    if (sstFile.exists()) {
      return sstBackupDir + filename;
    } else if (sstFileInActiveDB.exists()) {
      return activeDBLocationStr + filename;
    } else {
      throw new FileNotFoundException("Can't find SST file: " + filename);
    }
  }

  /**
   * Helper method to trim the filename retrieved from LiveFileMetaData.
   */
  private String trimSSTFilename(String filename) {
    if (!filename.startsWith("/")) {
      final String errorMsg = String.format(
          "Invalid start of filename: '%s'. Expected '/'", filename);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      final String errorMsg = String.format(
          "Invalid extension of file: '%s'. Expected '%s'",
          filename, SST_FILE_EXTENSION_LENGTH);
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }
    return filename.substring("/".length(),
        filename.length() - SST_FILE_EXTENSION_LENGTH);
  }

  /**
   * Read the current Live manifest for a given RocksDB instance (Active or
   * Checkpoint).
   * @param rocksDB open rocksDB instance.
   * @return a list of SST files (without extension) in the DB.
   */
  public HashSet<String> readRocksDBLiveFiles(ManagedRocksDB rocksDB) {
    HashSet<String> liveFiles = new HashSet<>();

    final List<String> cfs = Arrays.asList(
        org.apache.hadoop.hdds.StringUtils.bytes2String(
            RocksDB.DEFAULT_COLUMN_FAMILY), "keyTable", "directoryTable",
        "fileTable");
    // Note it retrieves only the selected column families by the descriptor
    // i.e. keyTable, directoryTable, fileTable
    List<LiveFileMetaData> liveFileMetaDataList =
        RdbUtil.getLiveSSTFilesForCFs(rocksDB, cfs);
    LOG.debug("SST File Metadata for DB: " + rocksDB.get().getName());
    for (LiveFileMetaData m : liveFileMetaDataList) {
      LOG.debug("File: {}, Level: {}", m.fileName(), m.level());
      final String trimmedFilename = trimSSTFilename(m.fileName());
      liveFiles.add(trimmedFilename);
    }
    return liveFiles;
  }

  /**
   * Process log line of compaction log text file input and populate the DAG.
   * It also adds the compaction log entry to compaction log table.
   */
  void processCompactionLogLine(String line) {

    LOG.debug("Processing line: {}", line);

    synchronized (this) {
      if (line.startsWith(COMPACTION_LOG_COMMENT_LINE_PREFIX)) {
        reconstructionCompactionReason =
            line.substring(COMPACTION_LOG_COMMENT_LINE_PREFIX.length());
      } else if (line.startsWith(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX)) {
        reconstructionSnapshotCreationTime =
            getSnapshotCreationTimeFromLogLine(line);
      } else if (line.startsWith(COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
        // Compaction log entry is like following:
        // C sequence_number input_files:output_files
        // where input_files and output_files are joined by ','.
        String[] lineSpilt = line.split(SPACE_DELIMITER);
        if (lineSpilt.length != 3) {
          LOG.error("Invalid line in compaction log: {}", line);
          return;
        }

        String dbSequenceNumber = lineSpilt[1];
        String[] io = lineSpilt[2]
            .split(COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER);

        if (io.length != 2) {
          if (line.endsWith(":")) {
            LOG.debug("Ignoring compaction log line for SST deletion");
          } else {
            LOG.error("Invalid line in compaction log: {}", line);
          }
          return;
        }

        String[] inputFiles = io[0].split(COMPACTION_LOG_ENTRY_FILE_DELIMITER);
        String[] outputFiles = io[1].split(COMPACTION_LOG_ENTRY_FILE_DELIMITER);
        addFileInfoToCompactionLogTable(Long.parseLong(dbSequenceNumber),
            reconstructionSnapshotCreationTime, inputFiles, outputFiles,
            reconstructionCompactionReason);
      } else {
        LOG.error("Invalid line in compaction log: {}", line);
      }
    }
  }

  /**
   * Helper to read compaction log file to the internal DAG and compaction log
   * table.
   */
  private void readCompactionLogFile(String currCompactionLogPath) {
    LOG.debug("Loading compaction log: {}", currCompactionLogPath);
    try (Stream<String> logLineStream =
        Files.lines(Paths.get(currCompactionLogPath), UTF_8)) {
      logLineStream.forEach(this::processCompactionLogLine);
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  public void addEntriesFromLogFilesToDagAndCompactionLogTable() {
    synchronized (this) {
      reconstructionSnapshotCreationTime = 0L;
      reconstructionCompactionReason = null;
      try {
        try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
            .filter(e -> e.toString().toLowerCase()
                .endsWith(COMPACTION_LOG_FILE_NAME_SUFFIX))
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
            compactionLogDir, e);
      }
    }
  }

  /**
   * Load existing compaction log from table to the in-memory DAG.
   * This only needs to be done once during OM startup.
   */
  public void loadAllCompactionLogs() {
    synchronized (this) {
      preconditionChecksForLoadAllCompactionLogs();
      addEntriesFromLogFilesToDagAndCompactionLogTable();
      try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
          activeRocksDB.newIterator(compactionLogTableCFHandle))) {
        managedRocksIterator.get().seekToFirst();
        while (managedRocksIterator.get().isValid()) {
          byte[] value = managedRocksIterator.get().value();
          CompactionLogEntry compactionLogEntry =
              CompactionLogEntry.getFromProtobuf(
                  CompactionLogEntryProto.parseFrom(value));
          populateCompactionDAG(compactionLogEntry.getInputFileInfoList(),
              compactionLogEntry.getOutputFileInfoList(),
              compactionLogEntry.getDbSequenceNumber());
          managedRocksIterator.get().next();
        }
      } catch (InvalidProtocolBufferException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private void preconditionChecksForLoadAllCompactionLogs() {
    Preconditions.checkNotNull(compactionLogDir,
        "Compaction log directory must be set.");
    Preconditions.checkNotNull(compactionLogTableCFHandle,
        "compactionLogTableCFHandle must be set before calling " +
            "loadAllCompactionLogs.");
    Preconditions.checkNotNull(activeRocksDB,
        "activeRocksDB must be set before calling loadAllCompactionLogs.");
  }
  /**
   * Helper function that prepends SST file name with SST backup directory path
   * (or DB checkpoint path if compaction hasn't happened yet as SST files won't
   * exist in backup directory before being involved in compactions),
   * and appends the extension '.sst'.
   */
  private String getSSTFullPath(String sstFilenameWithoutExtension,
      String dbPath) {

    // Try to locate the SST in the backup dir first
    final Path sstPathInBackupDir = Paths.get(sstBackupDir,
        sstFilenameWithoutExtension + SST_FILE_EXTENSION);
    if (Files.exists(sstPathInBackupDir)) {
      return sstPathInBackupDir.toString();
    }

    // SST file does not exist in the SST backup dir, this means the SST file
    // has not gone through any compactions yet and is only available in the
    // src DB directory
    final Path sstPathInDBDir = Paths.get(dbPath,
        sstFilenameWithoutExtension + SST_FILE_EXTENSION);
    if (Files.exists(sstPathInDBDir)) {
      return sstPathInDBDir.toString();
    }

    // TODO: More graceful error handling?
    throw new RuntimeException("Unable to locate SST file: " +
        sstFilenameWithoutExtension);
  }

  /**
   * A wrapper of getSSTDiffList() that copies the SST files to the
   * `sstFilesDirForSnapDiffJob` dir for the snap diff job and returns the
   * absolute path of SST files with extension in the provided dir.
   *
   * @param src source snapshot
   * @param dest destination snapshot
   * @param sstFilesDirForSnapDiffJob dir to create hardlinks for SST files
   *                                 for snapDiff job.
   * @return A list of SST files without extension.
   *         e.g. ["/path/to/sstBackupDir/000050.sst",
   *               "/path/to/sstBackupDir/000060.sst"]
   */
  public synchronized List<String> getSSTDiffListWithFullPath(
      DifferSnapshotInfo src,
      DifferSnapshotInfo dest,
      String sstFilesDirForSnapDiffJob
  ) throws IOException {

    List<String> sstDiffList = getSSTDiffList(src, dest);

    return sstDiffList.stream()
        .map(
            sst -> {
              String sstFullPath = getSSTFullPath(sst, src.getDbPath());
              Path link = Paths.get(sstFilesDirForSnapDiffJob,
                  sst + SST_FILE_EXTENSION);
              Path srcFile = Paths.get(sstFullPath);
              createLink(link, srcFile);
              return link.toString();
            })
        .collect(Collectors.toList());
  }

  /**
   * Get a list of SST files that differs between src and destination snapshots.
   * <p>
   * Expected input: src is a snapshot taken AFTER the dest.
   * <p>
   * Use getSSTDiffListWithFullPath() instead if you need the full path to SSTs.
   *
   * @param src source snapshot
   * @param dest destination snapshot
   * @return A list of SST files without extension. e.g. ["000050", "000060"]
   */
  public synchronized List<String> getSSTDiffList(
      DifferSnapshotInfo src,
      DifferSnapshotInfo dest
  ) throws IOException {

    // TODO: Reject or swap if dest is taken after src, once snapshot chain
    //  integration is done.
    Set<String> srcSnapFiles = readRocksDBLiveFiles(src.getRocksDB());
    Set<String> destSnapFiles = readRocksDBLiveFiles(dest.getRocksDB());

    Set<String> fwdDAGSameFiles = new HashSet<>();
    Set<String> fwdDAGDifferentFiles = new HashSet<>();

    LOG.debug("Doing forward diff from src '{}' to dest '{}'",
        src.getDbPath(), dest.getDbPath());
    internalGetSSTDiffList(src, dest, srcSnapFiles, destSnapFiles,
        fwdDAGSameFiles, fwdDAGDifferentFiles);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Result of diff from src '" + src.getDbPath() + "' to dest '" +
          dest.getDbPath() + "':");
      StringBuilder logSB = new StringBuilder();

      logSB.append("Fwd DAG same SST files:      ");
      for (String file : fwdDAGSameFiles) {
        logSB.append(file).append(SPACE_DELIMITER);
      }
      LOG.debug(logSB.toString());

      logSB.setLength(0);
      logSB.append("Fwd DAG different SST files: ");
      for (String file : fwdDAGDifferentFiles) {
        logSB.append(file).append(SPACE_DELIMITER);
      }
      LOG.debug("{}", logSB);
    }

    if (src.getTablePrefixes() != null && !src.getTablePrefixes().isEmpty()) {
      filterRelevantSstFilesFullPath(fwdDAGDifferentFiles,
          src.getTablePrefixes());
    }

    return new ArrayList<>(fwdDAGDifferentFiles);
  }

  /**
   * construct absolute sst file path first and
   * filter the files.
   */
  public void filterRelevantSstFilesFullPath(Set<String> inputFiles,
      Map<String, String> tableToPrefixMap) throws IOException {
    for (Iterator<String> fileIterator =
         inputFiles.iterator(); fileIterator.hasNext();) {
      String filename = fileIterator.next();
      String filepath = getAbsoluteSstFilePath(filename);
      if (!RocksDiffUtils.doesSstFileContainKeyRange(filepath,
          tableToPrefixMap)) {
        fileIterator.remove();
      }
    }
  }

  /**
   * Core getSSTDiffList logic.
   * <p>
   * For each SST in the src snapshot, traverse the DAG to find its final
   * successors.  If any of those successors match an SST in the dest
   * snapshot, add it to the sameFiles map (as it doesn't need further
   * diffing).  Otherwise, add it to the differentFiles map, as it will
   * need further diffing.
   */
  synchronized void internalGetSSTDiffList(
      DifferSnapshotInfo src,
      DifferSnapshotInfo dest,
      Set<String> srcSnapFiles,
      Set<String> destSnapFiles,
      Set<String> sameFiles,
      Set<String> differentFiles) {

    Preconditions.checkArgument(sameFiles.isEmpty(), "Set must be empty");
    Preconditions.checkArgument(differentFiles.isEmpty(), "Set must be empty");

    // Use source snapshot's table prefix. At this point Source and target's
    // table prefix should be same.
    Map<String, String> columnFamilyToPrefixMap = src.getTablePrefixes();

    for (String fileName : srcSnapFiles) {
      if (destSnapFiles.contains(fileName)) {
        LOG.debug("Source '{}' and destination '{}' share the same SST '{}'",
            src.getDbPath(), dest.getDbPath(), fileName);
        sameFiles.add(fileName);
        continue;
      }

      CompactionNode infileNode = compactionNodeMap.get(fileName);
      if (infileNode == null) {
        LOG.debug("Source '{}' SST file '{}' is never compacted",
            src.getDbPath(), fileName);
        differentFiles.add(fileName);
        continue;
      }

      LOG.debug("Expanding SST file: {}", fileName);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.add(infileNode);
      // Traversal level/depth indicator for debug print
      int level = 1;
      while (!currentLevel.isEmpty()) {
        LOG.debug("Traversal level: {}. Current level has {} nodes.",
            level++, currentLevel.size());

        if (level >= 1000000) {
          final String errorMsg = String.format(
                  "Graph traversal level exceeded allowed maximum (%d). "
                  + "This could be due to invalid input generating a "
                  + "loop in the traversal path. Same SSTs found so far: %s, "
                  + "different SSTs: %s", level, sameFiles, differentFiles);
          LOG.error(errorMsg);
          // Clear output in case of error. Expect fall back to full diff
          sameFiles.clear();
          differentFiles.clear();
          // TODO: Revisit error handling here. Use custom exception?
          throw new RuntimeException(errorMsg);
        }

        final Set<CompactionNode> nextLevel = new HashSet<>();
        for (CompactionNode current : currentLevel) {
          LOG.debug("Processing node: '{}'", current.getFileName());
          if (current.getSnapshotGeneration() < dest.getSnapshotGeneration()) {
            LOG.debug("Current node's snapshot generation '{}' "
                    + "reached destination snapshot's '{}'. "
                    + "Src '{}' and dest '{}' have different SST file: '{}'",
                current.getSnapshotGeneration(), dest.getSnapshotGeneration(),
                src.getDbPath(), dest.getDbPath(), current.getFileName());
            differentFiles.add(current.getFileName());
            continue;
          }

          Set<CompactionNode> successors =
              forwardCompactionDAG.successors(current);
          if (successors.isEmpty()) {
            LOG.debug("No further compaction happened to the current file. " +
                "Src '{}' and dest '{}' have different file: {}",
                src.getDbPath(), dest.getDbPath(), current.getFileName());
            differentFiles.add(current.getFileName());
            continue;
          }

          for (CompactionNode nextNode : successors) {
            if (shouldSkipNode(nextNode, columnFamilyToPrefixMap)) {
              LOG.debug("Skipping next node: '{}' with startKey: '{}' and " +
                      "endKey: '{}' because it doesn't have keys related to " +
                      "columnFamilyToPrefixMap: '{}'.",
                  nextNode.getFileName(), nextNode.getStartKey(),
                  nextNode.getEndKey(), columnFamilyToPrefixMap);
              continue;
            }

            if (sameFiles.contains(nextNode.getFileName()) ||
                differentFiles.contains(nextNode.getFileName())) {
              LOG.debug("Skipping known processed SST: {}",
                  nextNode.getFileName());
              continue;
            }

            if (destSnapFiles.contains(nextNode.getFileName())) {
              LOG.debug("Src '{}' and dest '{}' have the same SST: {}",
                  src.getDbPath(), dest.getDbPath(), nextNode.getFileName());
              sameFiles.add(nextNode.getFileName());
              continue;
            }

            // Queue different SST to the next level
            LOG.debug("Src '{}' and dest '{}' have a different SST: {}",
                src.getDbPath(), dest.getDbPath(), nextNode.getFileName());
            nextLevel.add(nextNode);
          }
        }
        currentLevel = nextLevel;
      }
    }
  }

  public String getMetadataDir() {
    return metadataDir;
  }

  static class NodeComparator
      implements Comparator<CompactionNode>, Serializable {
    public int compare(CompactionNode a, CompactionNode b) {
      return a.getFileName().compareToIgnoreCase(b.getFileName());
    }

    @Override
    public Comparator<CompactionNode> reversed() {
      return null;
    }
  }

  @VisibleForTesting
  void dumpCompactionNodeTable() {
    List<CompactionNode> nodeList = compactionNodeMap.values().stream()
        .sorted(new NodeComparator()).collect(Collectors.toList());
    for (CompactionNode n : nodeList) {
      LOG.debug("File '{}' total keys: {}",
          n.getFileName(), n.getTotalNumberOfKeys());
      LOG.debug("File '{}' cumulative keys: {}",
          n.getFileName(), n.getCumulativeKeysReverseTraversal());
    }
  }

  @VisibleForTesting
  public MutableGraph<CompactionNode> getForwardCompactionDAG() {
    return forwardCompactionDAG;
  }

  @VisibleForTesting
  public MutableGraph<CompactionNode> getBackwardCompactionDAG() {
    return backwardCompactionDAG;
  }

  /**
   * Helper method to add a new file node to the DAG.
   * @return CompactionNode
   */
  private CompactionNode addNodeToDAG(String file, long seqNum, String startKey,
                                      String endKey, String columnFamily) {
    long numKeys = 0L;
    try {
      numKeys = getSSTFileSummary(file);
    } catch (RocksDBException e) {
      LOG.warn("Can't get num of keys in SST '{}': {}", file, e.getMessage());
    } catch (FileNotFoundException e) {
      LOG.info("Can't find SST '{}'", file);
    }

    CompactionNode fileNode = new CompactionNode(file, numKeys,
        seqNum, startKey, endKey, columnFamily);

    forwardCompactionDAG.addNode(fileNode);
    backwardCompactionDAG.addNode(fileNode);

    return fileNode;
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   * @param inputFiles List of compaction input files.
   * @param outputFiles List of compaction output files.
   * @param seqNum DB transaction sequence number.
   */
  private void populateCompactionDAG(List<CompactionFileInfo> inputFiles,
                                     List<CompactionFileInfo> outputFiles,
                                     long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (CompactionFileInfo outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(
          outfile.getFileName(),

          file -> addNodeToDAG(file, seqNum, outfile.getStartKey(),
              outfile.getEndKey(), outfile.getColumnFamily()));


      for (CompactionFileInfo infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(
            infile.getFileName(),

            file -> addNodeToDAG(file, seqNum, infile.getStartKey(),
                infile.getEndKey(), infile.getColumnFamily()));

        // Draw the edges
        if (!outfileNode.getFileName().equals(infileNode.getFileName())) {
          forwardCompactionDAG.putEdge(outfileNode, infileNode);
          backwardCompactionDAG.putEdge(infileNode, outfileNode);
        }
      }
    }
  }

  private void addFileInfoToCompactionLogTable(
      long dbSequenceNumber,
      long creationTime,
      String[] inputFiles,
      String[] outputFiles,
      String compactionReason
  ) {
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

    addToCompactionLogTable(builder.build());
  }

  /**
   * This is the task definition which is run periodically by the service
   * executor at fixed delay.
   * It looks for snapshots in compaction DAG which are older than the allowed
   * time to be in compaction DAG and removes them from the DAG.
   */
  public void pruneOlderSnapshotsWithCompactionHistory() {
    if (!shouldRun()) {
      return;
    }
    Pair<Set<String>, List<byte[]>> fileNodeToKeyPair =
        getOlderFileNodes();
    Set<String> lastCompactionSstFiles = fileNodeToKeyPair.getLeft();
    List<byte[]> keysToRemove = fileNodeToKeyPair.getRight();

    Set<String> sstFileNodesRemoved =
        pruneSstFileNodesFromDag(lastCompactionSstFiles);

    if (CollectionUtils.isNotEmpty(sstFileNodesRemoved)) {
      LOG.info("Removing SST files: {} as part of compaction DAG pruning.",
          sstFileNodesRemoved);
    }

    try (BootstrapStateHandler.Lock lock = getBootstrapStateLock().lock()) {
      removeSstFiles(sstFileNodesRemoved);
      removeKeyFromCompactionLogTable(keysToRemove);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Returns the list of input files from the compaction entries which are
   * older than the maximum allowed in the compaction DAG.
   */
  private synchronized Pair<Set<String>, List<byte[]>> getOlderFileNodes() {
    long compactionLogPruneStartTime = System.currentTimeMillis();
    Set<String> compactionNodes = new HashSet<>();
    List<byte[]> keysToRemove = new ArrayList<>();

    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.newIterator(compactionLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();
      while (managedRocksIterator.get().isValid()) {
        CompactionLogEntry compactionLogEntry = CompactionLogEntry
            .getFromProtobuf(CompactionLogEntryProto
                .parseFrom(managedRocksIterator.get().value()));

        if (compactionLogPruneStartTime -
            compactionLogEntry.getCompactionTime() < maxAllowedTimeInDag) {
          break;
        }

        compactionLogEntry.getInputFileInfoList()
            .forEach(inputFileInfo ->
                compactionNodes.add(inputFileInfo.getFileName()));
        keysToRemove.add(managedRocksIterator.get().key());
        managedRocksIterator.get().next();

      }
    } catch (InvalidProtocolBufferException exception) {
      // TODO: Handle this properly before merging the PR.
      throw new RuntimeException(exception);
    }
    return Pair.of(compactionNodes, keysToRemove);
  }

  private synchronized void removeKeyFromCompactionLogTable(
      List<byte[]> keysToRemove) {
    try {
      for (byte[] key: keysToRemove) {
        activeRocksDB.delete(compactionLogTableCFHandle, key);
      }
    } catch (RocksDBException exception) {
      // TODO Handle exception properly before merging the PR.
      throw new RuntimeException(exception);
    }
  }

  /**
   * Deletes the SST files from the backup directory if exists.
   */
  private void removeSstFiles(Set<String> sstFileNodes) {
    for (String sstFileNode: sstFileNodes) {
      File file =
          new File(sstBackupDir + "/" + sstFileNode + SST_FILE_EXTENSION);
      try {
        Files.deleteIfExists(file.toPath());
      } catch (IOException exception) {
        LOG.warn("Failed to delete SST file: " + sstFileNode, exception);
      }
    }
  }

  /**
   * Prunes forward and backward DAGs when oldest snapshot with compaction
   * history gets deleted.
   */
  public Set<String> pruneSstFileNodesFromDag(Set<String> sstFileNodes) {
    Set<CompactionNode> startNodes = new HashSet<>();
    for (String sstFileNode : sstFileNodes) {
      CompactionNode infileNode = compactionNodeMap.get(sstFileNode);
      if (infileNode == null) {
        LOG.warn("Compaction node doesn't exist for sstFile: {}.", sstFileNode);
        continue;
      }

      startNodes.add(infileNode);
    }

    synchronized (this) {
      pruneBackwardDag(backwardCompactionDAG, startNodes);
      Set<String> sstFilesPruned = pruneForwardDag(forwardCompactionDAG,
          startNodes);

      // Remove SST file nodes from compactionNodeMap too,
      // since those nodes won't be needed after clean up.
      sstFilesPruned.forEach(compactionNodeMap::remove);
      return sstFilesPruned;
    }
  }

  /**
   * Prunes backward DAG's upstream from the level, that needs to be removed.
   */
  @VisibleForTesting
  Set<String> pruneBackwardDag(MutableGraph<CompactionNode> backwardDag,
                               Set<CompactionNode> startNodes) {
    Set<String> removedFiles = new HashSet<>();
    Set<CompactionNode> currentLevel = startNodes;

    synchronized (this) {
      while (!currentLevel.isEmpty()) {
        Set<CompactionNode> nextLevel = new HashSet<>();
        for (CompactionNode current : currentLevel) {
          if (!backwardDag.nodes().contains(current)) {
            continue;
          }

          nextLevel.addAll(backwardDag.predecessors(current));
          backwardDag.removeNode(current);
          removedFiles.add(current.getFileName());
        }
        currentLevel = nextLevel;
      }
    }

    return removedFiles;
  }

  /**
   * Prunes forward DAG's downstream from the level that needs to be removed.
   */
  @VisibleForTesting
  Set<String> pruneForwardDag(MutableGraph<CompactionNode> forwardDag,
                              Set<CompactionNode> startNodes) {
    Set<String> removedFiles = new HashSet<>();
    Set<CompactionNode> currentLevel = new HashSet<>(startNodes);

    synchronized (this) {
      while (!currentLevel.isEmpty()) {
        Set<CompactionNode> nextLevel = new HashSet<>();
        for (CompactionNode current : currentLevel) {
          if (!forwardDag.nodes().contains(current)) {
            continue;
          }

          nextLevel.addAll(forwardDag.successors(current));
          forwardDag.removeNode(current);
          removedFiles.add(current.getFileName());
        }

        currentLevel = nextLevel;
      }
    }

    return removedFiles;
  }

  private long getSnapshotCreationTimeFromLogLine(String logLine) {
    // Remove `S ` from the line.
    String line =
        logLine.substring(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX.length());

    String[] splits = line.split(SPACE_DELIMITER);
    Preconditions.checkArgument(splits.length == 3,
        "Snapshot info log statement has more than expected parameters.");

    return Long.parseLong(splits[2]);
  }

  public String getSSTBackupDir() {
    return sstBackupDir;
  }

  public String getCompactionLogDir() {
    return compactionLogDir;
  }

  /**
   * Defines the task that removes SST files from backup directory which are
   * not needed to generate snapshot diff using compaction DAG to clean
   * the disk space.
   * We can’t simply delete input files in the compaction completed listener
   * because it is not known which of input files are from previous compaction
   * and which were created after the compaction.
   * We can remove SST files which were created from the compaction because
   * those are not needed to generate snapshot diff. These files are basically
   * non-leaf nodes of the DAG.
   */
  public synchronized void pruneSstFiles() {
    if (!shouldRun()) {
      return;
    }

    Set<String> nonLeafSstFiles;
    nonLeafSstFiles = forwardCompactionDAG.nodes().stream()
        .filter(node -> !forwardCompactionDAG.successors(node).isEmpty())
        .map(node -> node.getFileName())
        .collect(Collectors.toSet());

    if (CollectionUtils.isNotEmpty(nonLeafSstFiles)) {
      LOG.info("Removing SST files: {} as part of SST file pruning.",
          nonLeafSstFiles);
    }

    try (BootstrapStateHandler.Lock lock = getBootstrapStateLock().lock()) {
      removeSstFiles(nonLeafSstFiles);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void incrementTarballRequestCount() {
    tarballRequestCount.incrementAndGet();
  }

  public void decrementTarballRequestCount() {
    tarballRequestCount.decrementAndGet();
  }

  public boolean shouldRun() {
    return !suspended.get();
  }

  @VisibleForTesting
  public int getTarballRequestCount() {
    return tarballRequestCount.get();
  }

  @VisibleForTesting
  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }

  @VisibleForTesting
  public static Logger getLog() {
    return LOG;
  }

  @VisibleForTesting
  public ConcurrentHashMap<String, CompactionNode> getCompactionNodeMap() {
    return compactionNodeMap;
  }

  @VisibleForTesting
  public void resume() {
    suspended.set(false);
  }

  @VisibleForTesting
  public void suspend() {
    suspended.set(true);
  }

  /**
   * Holder for RocksDBCheckpointDiffer instance.
   * This is to protect from creating more than one instance of
   * RocksDBCheckpointDiffer per RocksDB dir and use the single instance per dir
   * throughout the whole OM process.
   */
  public static class RocksDBCheckpointDifferHolder {
    private static final ConcurrentMap<String, RocksDBCheckpointDiffer>
        INSTANCE_MAP = new ConcurrentHashMap<>();

    public static RocksDBCheckpointDiffer getInstance(
        String metadataDirName,
        String sstBackupDirName,
        String compactionLogDirName,
        String activeDBLocationName,
        ConfigurationSource configuration
    ) {
      return INSTANCE_MAP.computeIfAbsent(metadataDirName, (key) ->
          new RocksDBCheckpointDiffer(metadataDirName,
              sstBackupDirName,
              compactionLogDirName,
              activeDBLocationName,
              configuration));
    }

    /**
     * Close RocksDBCheckpointDiffer object if value is present for the key.
     * @param cacheKey cacheKey is metadataDirName path which is used as key
     *                for cache.
     */
    public static void invalidateCacheEntry(String cacheKey) {
      IOUtils.closeQuietly(INSTANCE_MAP.get(cacheKey));
      INSTANCE_MAP.remove(cacheKey);
    }
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  public void pngPrintMutableGraph(String filePath, GraphType graphType)
      throws IOException {
    Objects.requireNonNull(filePath, "Image file path is required.");
    Objects.requireNonNull(graphType, "Graph type is required.");

    PrintableGraph graph;
    synchronized (this) {
      graph = new PrintableGraph(backwardCompactionDAG, graphType);
    }

    graph.generateImage(filePath);
  }

  private List<CompactionFileInfo> toFileInfoList(List<String> sstFiles,
                                                  ManagedOptions options,
                                                  ManagedReadOptions readOptions
  ) {
    if (CollectionUtils.isEmpty(sstFiles)) {
      return Collections.emptyList();
    }

    List<CompactionFileInfo> response = new ArrayList<>();

    for (String sstFile : sstFiles) {
      CompactionFileInfo fileInfo = toFileInfo(sstFile, options, readOptions);
      response.add(fileInfo);
    }
    return response;
  }

  private CompactionFileInfo toFileInfo(String sstFile,
                                        ManagedOptions options,
                                        ManagedReadOptions readOptions) {
    final int fileNameOffset = sstFile.lastIndexOf("/") + 1;
    String fileName = sstFile.substring(fileNameOffset,
        sstFile.length() - SST_FILE_EXTENSION_LENGTH);
    CompactionFileInfo.Builder fileInfoBuilder =
        new CompactionFileInfo.Builder(fileName);

    try (SstFileReader fileReader = new SstFileReader(options)) {
      fileReader.open(sstFile);
      String columnFamily = StringUtils.bytes2String(
          fileReader.getTableProperties().getColumnFamilyName());
      SstFileReaderIterator iterator = fileReader.newIterator(readOptions);
      iterator.seekToFirst();
      String startKey = StringUtils.bytes2String(iterator.key());
      iterator.seekToLast();
      String endKey = StringUtils.bytes2String(iterator.key());
      fileInfoBuilder.setStartRange(startKey)
          .setEndRange(endKey)
          .setColumnFamily(columnFamily);
    } catch (RocksDBException rocksDBException) {
      // Ideally it should not happen. If it does just log the exception.
      // And let the compaction complete without the exception.
      // Throwing exception in compaction listener could fail the RocksDB.
      // In case of exception, compaction node will be missing start key,
      // end key and column family. And during diff calculation it will
      // continue the traversal as it was before HDDS-8940.
      LOG.warn("Failed to read SST file: {}.", sstFile, rocksDBException);
    }
    return fileInfoBuilder.build();
  }

  @VisibleForTesting
  boolean shouldSkipNode(CompactionNode node,
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
    return !RocksDiffUtils.isKeyWithPrefixPresent(keyPrefix, node.getStartKey(),
        node.getEndKey());
  }
}
