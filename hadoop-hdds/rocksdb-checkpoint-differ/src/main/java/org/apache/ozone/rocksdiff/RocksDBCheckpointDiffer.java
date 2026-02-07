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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static org.apache.hadoop.hdds.utils.db.IteratorType.KEY_ONLY;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT;
import static org.apache.hadoop.ozone.OzoneConsts.ROCKSDB_SST_SUFFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.StringUtils;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.FlushLogEntryProto;
import org.apache.hadoop.hdds.utils.IOUtils;
import org.apache.hadoop.hdds.utils.NativeLibraryNotLoadedException;
import org.apache.hadoop.hdds.utils.Scheduler;
import org.apache.hadoop.hdds.utils.db.CodecBuffer;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileIterator;
import org.apache.hadoop.hdds.utils.db.ManagedRawSSTFileReader;
import org.apache.hadoop.hdds.utils.db.RDBSstFileWriter;
import org.apache.hadoop.hdds.utils.db.RocksDatabaseException;
import org.apache.hadoop.hdds.utils.db.TablePrefixInfo;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedEnvOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.lock.BootstrapStateHandler;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.compaction.log.FlushFileInfo;
import org.apache.ozone.compaction.log.FlushLogEntry;
import org.apache.ozone.rocksdb.util.SstFileInfo;
import org.apache.ratis.util.UncheckedAutoCloseable;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushJobInfo;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB checkpoint differ.
 * <p>
 * Implements Ozone Manager RocksDB flush listener (flush log persistence
 * and SST file hard-linking), FlushLinkedList construction, and flush log
 * reconstruction upon OM restarts.
 * <p>
 * It is important to note that flush log is per-DB instance. Since
 * each OM DB instance might trigger flushes at different timings.
 */
public class RocksDBCheckpointDiffer implements AutoCloseable,
    BootstrapStateHandler {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  private final String metadataDir;
  private final String sstBackupDir;
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
  public static final String SST_FILE_EXTENSION = ".sst";
  public static final int SST_FILE_EXTENSION_LENGTH =
      SST_FILE_EXTENSION.length();
  static final String PRUNED_SST_FILE_TEMP = "pruned.sst.tmp";

  private static final int LONG_MAX_STR_LEN =
      String.valueOf(Long.MAX_VALUE).length();

  /**
   * Used during DAG reconstruction.
   */
  private long reconstructionSnapshotCreationTime;
  private String reconstructionCompactionReason;

  private final Scheduler scheduler;
  private volatile boolean closed;
  private final long maxSnapshotHistoryRetentionMs;
  private final BootstrapStateHandler.Lock lock;
  private static final int SST_READ_AHEAD_SIZE = 2 * 1024 * 1024;
  private int pruneSSTFileBatchSize;
  private SSTFilePruningMetrics sstFilePruningMetrics;
  private ColumnFamilyHandle snapshotInfoTableCFHandle;
  private static final String DAG_PRUNING_SERVICE_NAME = "CompactionDagPruningService";
  private AtomicBoolean suspended;

  private ColumnFamilyHandle compactionLogTableCFHandle;
  private ColumnFamilyHandle flushLogTableCFHandle;
  private ManagedRocksDB activeRocksDB;
  private final FlushLinkedList flushLinkedList;
  private Queue<byte[]> pruneQueue = null;

  /**
   * For snapshot diff calculation we only need to track following column
   * families. Other column families are irrelevant for snapshot diff.
   */
  public static final Set<String> COLUMN_FAMILIES_TO_TRACK =
      ImmutableSet.of("keyTable", "directoryTable", "fileTable");

  static {
    RocksDB.loadLibrary();
  }

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
                          ConfigurationSource configuration,
                          Function<Boolean, UncheckedAutoCloseable> lockSupplier) {
    this.metadataDir = Objects.requireNonNull(metadataDirName, "metadataDirName == null");
    Objects.requireNonNull(sstBackupDirName, "sstBackupDirName == null");
    Objects.requireNonNull(compactionLogDirName, "compactionLogDirName == null");
    Objects.requireNonNull(activeDBLocationName, "activeDBLocationName == null");
    Objects.requireNonNull(lockSupplier, "lockSupplier == null");
    this.lock = new BootstrapStateHandler.Lock(lockSupplier);
    this.compactionLogDir =
        createCompactionLogDir(metadataDirName, compactionLogDirName);
    this.sstBackupDir = Paths.get(metadataDirName, sstBackupDirName) + "/";
    createSstBackUpDir();

    this.maxSnapshotHistoryRetentionMs = configuration.getTimeDuration(
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED,
        OZONE_OM_SNAPSHOT_COMPACTION_DAG_MAX_TIME_ALLOWED_DEFAULT,
        TimeUnit.MILLISECONDS);
    this.suspended = new AtomicBoolean(false);

    long pruneCompactionDagDaemonRunIntervalInMs =
        configuration.getTimeDuration(
            OZONE_OM_SNAPSHOT_COMPACTION_DAG_PRUNE_DAEMON_RUN_INTERVAL,
            OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_DAG_DAEMON_RUN_INTERVAL_DEFAULT,
            TimeUnit.MILLISECONDS);

    this.pruneSSTFileBatchSize = configuration.getInt(
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE,
        OZONE_OM_SNAPSHOT_PRUNE_COMPACTION_BACKUP_BATCH_SIZE_DEFAULT);
    this.sstFilePruningMetrics = SSTFilePruningMetrics.create(activeDBLocationName);
    try {
      if (configuration.getBoolean(OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB, OZONE_OM_SNAPSHOT_LOAD_NATIVE_LIB_DEFAULT)
          && ManagedRawSSTFileReader.loadLibrary()) {
        this.pruneQueue = new ConcurrentLinkedQueue<>();
      }
    } catch (NativeLibraryNotLoadedException e) {
      LOG.warn("Native Library for raw sst file reading loading failed." +
          " Cannot prune OMKeyInfo from SST files. {}", e.getMessage());
    }

    if (pruneCompactionDagDaemonRunIntervalInMs > 0) {
      this.scheduler = new Scheduler(DAG_PRUNING_SERVICE_NAME,
          true, 1);

      // Use flush-based pruning to clean up old flush entries and backup SST files
      // to replace CompactionDag pruning.
      this.scheduler.scheduleWithFixedDelay(
          this::pruneOlderSnapshotsWithFlushHistory,
          pruneCompactionDagDaemonRunIntervalInMs,
          pruneCompactionDagDaemonRunIntervalInMs,
          TimeUnit.MILLISECONDS);

      // Backup SST files are now cleaned up in pruneOlderSnapshotsWithFlushHistory()
      // along with flush log entries.

      if (pruneQueue != null) {
        this.scheduler.scheduleWithFixedDelay(
            this::pruneSstFileValues,
            pruneCompactionDagDaemonRunIntervalInMs,
            pruneCompactionDagDaemonRunIntervalInMs,
            TimeUnit.MILLISECONDS);
      }
    } else {
      this.scheduler = null;
    }
    this.flushLinkedList = new FlushLinkedList();
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
  public void close() {
    if (!closed) {
      synchronized (this) {
        if (!closed) {
          closed = true;
          if (scheduler != null) {
            LOG.info("Shutting down {}.", DAG_PRUNING_SERVICE_NAME);
            scheduler.close();
          }
          if (sstFilePruningMetrics != null) {
            sstFilePruningMetrics.unRegister();
          }
        }
      }
    }
  }

  public void setRocksDBForCompactionTracking(ManagedDBOptions rocksOptions) {
    List<AbstractEventListener> events = new ArrayList<>();
    // Use flush-based tracking instead of compaction DAG
    events.add(newFlushCompletedListener());
    rocksOptions.setListeners(events);
  }

  /**
   * Set SnapshotInfoTable DB column family handle to be used in DB listener.
   * @param handle ColumnFamilyHandle
   */
  public void setSnapshotInfoTableCFHandle(
      ColumnFamilyHandle handle) {
    this.snapshotInfoTableCFHandle = Objects.requireNonNull(handle, "handle == null");
  }

  /**
   * Set CompactionLogTable DB column family handle to access the table.
   * @param handle ColumnFamilyHandle
   */
  public synchronized void setCompactionLogTableCFHandle(
      ColumnFamilyHandle handle) {
    this.compactionLogTableCFHandle = Objects.requireNonNull(handle, "handle == null");
  }

  /**
   * Set FlushLogTable DB column family handle to access the table.
   * @param handle ColumnFamilyHandle
   */
  public synchronized void setFlushLogTableCFHandle(
      ColumnFamilyHandle handle) {
    this.flushLogTableCFHandle = Objects.requireNonNull(handle, "handle == null");
  }

  /**
   * Set activeRocksDB to access CompactionLogTable and FlushLogTable.
   * @param activeRocksDB RocksDB
   */
  public synchronized void setActiveRocksDB(ManagedRocksDB activeRocksDB) {
    Objects.requireNonNull(activeRocksDB, "RocksDB should not be null.");
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

    try (ManagedRocksIterator it = ManagedRocksIterator.managed(db.newIterator(snapshotInfoTableCFHandle))) {
      it.get().seekToFirst();
      return !it.get().isValid();
    }
  }

  @VisibleForTesting
  boolean shouldSkipCompaction(byte[] columnFamilyBytes,
                               List<String> inputFiles,
                               List<String> outputFiles) {
    String columnFamily = StringUtils.bytes2String(columnFamilyBytes);

    if (!COLUMN_FAMILIES_TO_TRACK.contains(columnFamily)) {
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

  /**
   * Creates a new flush completed event listener for RocksDB.
   * This listener tracks L0 SST files created by flush operations,
   * which will be used for snapshot diff calculation instead of
   * tracking compaction events.
   *
   * @return AbstractEventListener that handles onFlushCompleted events
   */
  private AbstractEventListener newFlushCompletedListener() {
    return new AbstractEventListener() {
      @Override
      public void onFlushCompleted(RocksDB db, FlushJobInfo flushJobInfo) {
        String columnFamily = flushJobInfo.getColumnFamilyName();

        // Only track column families relevant for snapshot diff
        if (!COLUMN_FAMILIES_TO_TRACK.contains(columnFamily)) {
          LOG.debug("Skipping flush for columnFamily: {}", columnFamily);
          return;
        }

        synchronized (RocksDBCheckpointDiffer.this) {
          if (closed) {
            return;
          }

          // Skip flush tracking if no snapshots exist
          if (isSnapshotInfoTableEmpty(db)) {
            return;
          }
        }

        // Get SST file info from FlushJobInfo
        String filePath = flushJobInfo.getFilePath();
        String fileName = FilenameUtils.getBaseName(filePath);
        long seqNum = flushJobInfo.getLargestSeqno();
        long flushTime = System.currentTimeMillis();

        // Get key range from LiveFileMetaData
        Map<String, LiveFileMetaData> metadataMap =
            ManagedRocksDB.getLiveMetadataForSSTFiles(db);
        LiveFileMetaData metadata = metadataMap.get(fileName);

        // Create hard link to preserve the SST file
        createLink(Paths.get(sstBackupDir, new File(filePath).getName()),
            Paths.get(filePath));

        // Build FlushFileInfo using LiveFileMetaData
        FlushFileInfo fileInfo = new FlushFileInfo.Builder(fileName)
            .setValues(metadata)
            .build();

        FlushLogEntry flushLogEntry = new FlushLogEntry.Builder(
            seqNum, flushTime, fileInfo)
            .setFlushReason(flushJobInfo.getFlushReason().toString())
            .build();

        synchronized (RocksDBCheckpointDiffer.this) {
          if (closed) {
            return;
          }

          // Recheck snapshot existence after acquiring lock since it may have changed
          // between the first check and creating the hard link.
          if (isSnapshotInfoTableEmpty(db)) {
            return;
          }

          // Persist to FlushLogTable
          addToFlushLogTable(flushLogEntry);

          // Update in-memory FlushLinkedList
          flushLinkedList.addFlush(fileName, seqNum, flushTime,
              fileInfo.getStartKey(), fileInfo.getEndKey(), columnFamily);
        }

        LOG.debug("Tracked flush: file={}, seq={}, CF={}, reason={}",
            fileName, seqNum, columnFamily, flushJobInfo.getFlushReason());
      }
    };
  }

  @VisibleForTesting
  synchronized byte[] addToCompactionLogTable(CompactionLogEntry compactionLogEntry) {
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
      activeRocksDB.get().put(compactionLogTableCFHandle, key, value);
    } catch (RocksDBException exception) {
      // TODO: Revisit exception handling before merging the PR.
      throw new RuntimeException(exception);
    }
    return key;
  }

  /**
   * Add a FlushLogEntry to the FlushLogTable for persistence.
   * Similar to addToCompactionLogTable but for flush events.
   *
   * @param flushLogEntry the flush log entry to persist
   * @return the key used to store the entry
   */
  @VisibleForTesting
  synchronized byte[] addToFlushLogTable(FlushLogEntry flushLogEntry) {
    String dbSequenceIdStr =
        String.valueOf(flushLogEntry.getDbSequenceNumber());

    if (dbSequenceIdStr.length() < LONG_MAX_STR_LEN) {
      // Pad zeroes to the left to make sure it is lexicographic ordering.
      dbSequenceIdStr = org.apache.commons.lang3.StringUtils.leftPad(
          dbSequenceIdStr, LONG_MAX_STR_LEN, "0");
    }

    // Key: sequenceNumber-flushTime
    // Adding flush time to sequence number eliminates key collision.
    String keyString = dbSequenceIdStr + "-" + flushLogEntry.getFlushTime();

    byte[] key = keyString.getBytes(UTF_8);
    byte[] value = flushLogEntry.getProtobuf().toByteArray();
    try {
      activeRocksDB.get().put(flushLogTableCFHandle, key, value);
    } catch (RocksDBException exception) {
      throw new RuntimeException("Failed to add flush log entry", exception);
    }
    return key;
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
   * It is only for backward compatibility.
   */
  public void loadAllCompactionLogs() {
    synchronized (this) {
      preconditionChecksForLoadAllCompactionLogs();
      addEntriesFromLogFilesToDagAndCompactionLogTable();
      // Load flush log entries for flush-based tracking
      loadFlushLogFromDB();
    }
  }

  /**
   * Load flush log entries from FlushLogTable into the in-memory FlushLinkedList.
   * This is called during OM startup to restore flush tracking state.
   */
  private void loadFlushLogFromDB() {
    if (flushLogTableCFHandle == null) {
      LOG.debug("FlushLogTable CF handle not set, skipping flush log loading.");
      return;
    }

    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(flushLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();
      int count = 0;
      while (managedRocksIterator.get().isValid()) {
        byte[] value = managedRocksIterator.get().value();
        FlushLogEntry flushLogEntry =
            FlushLogEntry.getFromProtobuf(FlushLogEntryProto.parseFrom(value));
        FlushFileInfo fileInfo = flushLogEntry.getFileInfo();

        // Populate the in-memory FlushLinkedList
        flushLinkedList.addFlush(
            fileInfo.getFileName(),
            flushLogEntry.getDbSequenceNumber(),
            flushLogEntry.getFlushTime(),
            fileInfo.getStartKey(),
            fileInfo.getEndKey(),
            fileInfo.getColumnFamily());
        count++;
        managedRocksIterator.get().next();
      }
      LOG.info("Loaded {} flush log entries from FlushLogTable.", count);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to load flush log from DB", e);
    }
  }

  private void preconditionChecksForLoadAllCompactionLogs() {
    Objects.requireNonNull(compactionLogDir,
        "Compaction log directory must be set.");
    Objects.requireNonNull(compactionLogTableCFHandle,
        "compactionLogTableCFHandle must be set before calling " +
            "loadAllCompactionLogs.");
    Objects.requireNonNull(activeRocksDB,
        "activeRocksDB must be set before calling loadAllCompactionLogs.");
  }

  /**
   * Helper function that prepends SST file name with SST backup directory path
   * (or DB checkpoint path if compaction hasn't happened yet as SST files won't
   * exist in backup directory before being involved in compactions),
   * and appends the extension '.sst'.
   */
  private Path getSSTFullPath(SstFileInfo sstFileInfo, Path... dbPaths) throws IOException {

    // Try to locate the SST in the backup dir first
    final Path sstPathInBackupDir = sstFileInfo.getFilePath(Paths.get(sstBackupDir).toAbsolutePath());
    if (Files.exists(sstPathInBackupDir)) {
      return sstPathInBackupDir.toAbsolutePath();
    }

    // SST file does not exist in the SST backup dir, this means the SST file
    // has not gone through any compactions yet and is only available in the
    // src DB directory or destDB directory
    for (Path dbPath : dbPaths) {
      final Path sstPathInDBDir = sstFileInfo.getFilePath(dbPath);
      if (Files.exists(sstPathInDBDir)) {
        return sstPathInDBDir.toAbsolutePath();
      }
    }

    throw new IOException("Unable to locate SST file: " + sstFileInfo);
  }

  /**
   * A wrapper of getSSTDiffList() that copies the SST files to the
   * `sstFilesDirForSnapDiffJob` dir for the snap diff job and returns the
   * absolute path of SST files with extension in the provided dir.
   *
   * @param src source snapshot
   * @param dest destination snapshot
   * @param versionMap version map containing the connection between source snapshot version and dest snapshot version.
   * @param tablesToLookup tablesToLookup set of table (column family) names used to restrict which SST files to return.
   * @return map of SST file absolute paths with extension to SstFileInfo.
   */
  public synchronized Optional<Map<Path, SstFileInfo>> getSSTDiffListWithFullPath(DifferSnapshotInfo src,
      DifferSnapshotInfo dest, Map<Integer, Integer> versionMap, TablePrefixInfo prefixInfo,
      Set<String> tablesToLookup) throws IOException {
    int srcVersion = src.getMaxVersion();
    if (!versionMap.containsKey(srcVersion)) {
      throw new IOException("No corresponding dest version corresponding srcVersion : " + srcVersion + " in " +
          "versionMap : " + versionMap);
    }
    int destVersion = versionMap.get(srcVersion);
    DifferSnapshotVersion srcSnapshotVersion = new DifferSnapshotVersion(src, src.getMaxVersion(), tablesToLookup);
    DifferSnapshotVersion destSnapshotVersion = new DifferSnapshotVersion(dest, destVersion, tablesToLookup);

    // If the source snapshot version is 0, use the compaction DAG path otherwise performs a full diff on the basis
    // of the sst file names.
    Optional<List<SstFileInfo>> sstDiffList = getSSTDiffList(srcSnapshotVersion, destSnapshotVersion, prefixInfo,
        tablesToLookup, srcVersion == 0);
    if (sstDiffList.isPresent()) {
      Map<Path, SstFileInfo> sstFileInfoMap = new HashMap<>();
      for (SstFileInfo sstFileInfo : sstDiffList.get()) {
        Path sstPath = getSSTFullPath(sstFileInfo, srcSnapshotVersion.getDbPath());
        sstFileInfoMap.put(sstPath, sstFileInfo);
      }
      return Optional.of(sstFileInfoMap);
    }
    return Optional.empty();
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
   * @param prefixInfo TablePrefixInfo to filter irrelevant SST files; can be null.
   * @param tablesToLookup tablesToLookup Set of column-family (table) names to include when reading SST files;
   *                       must be non-null.
   * @param useCompactionDag If true, the method uses the compaction history to produce the incremental diff,
   *                         otherwise a full diff would be performed on the basis of the sst file names.
   * @return A list of SST files without extension. e.g. ["000050", "000060"]
   */
  public synchronized Optional<List<SstFileInfo>> getSSTDiffList(DifferSnapshotVersion src,
      DifferSnapshotVersion dest, TablePrefixInfo prefixInfo, Set<String> tablesToLookup, boolean useCompactionDag) {

    // TODO: Reject or swap if dest is taken after src, once snapshot chain
    //  integration is done.
    Map<String, SstFileInfo>  fwdDAGSameFiles = new HashMap<>();
    Map<String, SstFileInfo>  fwdDAGDifferentFiles = new HashMap<>();

    if (useCompactionDag) {
      // Always use flush-based tracking (CompactionDag has been removed)
      LOG.debug("Using flush-based tracking for diff from src '{}' to dest '{}'",
          src.getDbPath(), dest.getDbPath());
      internalGetSSTDiffListUsingFlush(src, dest, fwdDAGSameFiles, fwdDAGDifferentFiles);
    } else {
      Set<SstFileInfo> srcSstFileInfos = new HashSet<>(src.getSstFileMap().values());
      Set<SstFileInfo> destSstFileInfos = new HashSet<>(dest.getSstFileMap().values());
      for (SstFileInfo srcSstFileInfo : srcSstFileInfos) {
        if (destSstFileInfos.contains(srcSstFileInfo)) {
          fwdDAGSameFiles.put(srcSstFileInfo.getFileName(), srcSstFileInfo);
        } else {
          fwdDAGDifferentFiles.put(srcSstFileInfo.getFileName(), srcSstFileInfo);
        }
      }
      for (SstFileInfo destSstFileInfo : destSstFileInfos) {
        if (srcSstFileInfos.contains(destSstFileInfo)) {
          fwdDAGSameFiles.put(destSstFileInfo.getFileName(), destSstFileInfo);
        } else {
          fwdDAGDifferentFiles.put(destSstFileInfo.getFileName(), destSstFileInfo);
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Result of diff from src '" + src.getDbPath() + "' to dest '" +
          dest.getDbPath() + "':");
      StringBuilder logSB = new StringBuilder();

      logSB.append("Fwd DAG same SST files:      ");
      for (String file : fwdDAGSameFiles.keySet()) {
        logSB.append(file).append(SPACE_DELIMITER);
      }
      LOG.debug(logSB.toString());

      logSB.setLength(0);
      logSB.append("Fwd DAG different SST files: ");
      for (String file : fwdDAGDifferentFiles.keySet()) {
        logSB.append(file).append(SPACE_DELIMITER);
      }
      LOG.debug("{}", logSB);
    }

    // Check if the DAG traversal was able to reach all the destination SST files.
    for (String destSnapFile : dest.getSstFiles()) {
      if (!fwdDAGSameFiles.containsKey(destSnapFile) && !fwdDAGDifferentFiles.containsKey(destSnapFile)) {
        return Optional.empty();
      }
    }

    if (prefixInfo != null && prefixInfo.size() != 0) {
      RocksDiffUtils.filterRelevantSstFiles(fwdDAGDifferentFiles, tablesToLookup, prefixInfo);
    }
    return Optional.of(new ArrayList<>(fwdDAGDifferentFiles.values()));
  }

  /**
   * This class represents a version of a snapshot in a database differ operation.
   * It contains metadata associated with a specific snapshot version, including
   * SST file information, generation id, and the database path for the given version.
   *
   * Designed to work with `DifferSnapshotInfo`, this class allows the retrieval of
   * snapshot-related metadata and facilitates mapping of SST files for version comparison
   * and other operations.
   *
   * The core functionality is to store and provide read-only access to:
   * - SST file information for a specified snapshot version.
   * - Snapshot generation identifier.
   * - Path to the database directory corresponding to the snapshot version.
   */
  public static class DifferSnapshotVersion {
    private Map<String, SstFileInfo> sstFiles;
    private long generation;
    private Path dbPath;

    public DifferSnapshotVersion(DifferSnapshotInfo differSnapshotInfo, int version,
        Set<String> tablesToLookup) {
      this.sstFiles = differSnapshotInfo.getSstFiles(version, tablesToLookup)
          .stream().collect(Collectors.toMap(SstFileInfo::getFileName, identity()));
      this.generation = differSnapshotInfo.getGeneration();
      this.dbPath = differSnapshotInfo.getDbPath(version);
    }

    private Path getDbPath() {
      return dbPath;
    }

    private long getGeneration() {
      return generation;
    }

    private Set<String> getSstFiles() {
      return sstFiles.keySet();
    }

    private Map<String, SstFileInfo> getSstFileMap() {
      return Collections.unmodifiableMap(sstFiles);
    }
  }

  /**
   * Get SST diff list using flush-based tracking instead of compaction DAG.
   * This is a simpler O(N) algorithm that uses FlushLinkedList range queries.
   * The logic is:
   * 1. Find all L0 SST files flushed between src and dest snapshot generations
   * 2. These flushed files represent the "different" files that need diffing
   * 3. Files in both snapshots that weren't flushed between them are "same"
   *
   * @param src source snapshot version
   * @param dest destination snapshot version
   * @param sameFiles output map for files that exist in both snapshots
   * @param differentFiles output map for files that need diffing
   */
  synchronized void internalGetSSTDiffListUsingFlush(
      DifferSnapshotVersion src,
      DifferSnapshotVersion dest,
      Map<String, SstFileInfo> sameFiles,
      Map<String, SstFileInfo> differentFiles) {

    Preconditions.checkArgument(sameFiles.isEmpty(), "sameFiles must be empty");
    Preconditions.checkArgument(differentFiles.isEmpty(), "differentFiles must be empty");

    Map<String, SstFileInfo> srcSnapFiles = src.getSstFileMap();
    Map<String, SstFileInfo> destSnapFiles = dest.getSstFileMap();

    // Get all L0 files flushed between the two snapshot generations
    // src is newer (higher generation), dest is older (lower generation)
    List<FlushNode> flushedFilesBetween = flushLinkedList.getFlushNodesBetween(
        dest.getGeneration(), src.getGeneration());

    Set<String> flushedFileNames = flushedFilesBetween.stream()
        .map(FlushNode::getFileName)
        .collect(Collectors.toSet());

    LOG.debug("Found {} flushed files between generations {} and {}",
        flushedFileNames.size(), dest.getGeneration(), src.getGeneration());

    // Process source snapshot files
    for (Map.Entry<String, SstFileInfo> entry : srcSnapFiles.entrySet()) {
      String fileName = entry.getKey();
      SstFileInfo fileInfo = entry.getValue();

      if (destSnapFiles.containsKey(fileName)) {
        // File exists in both snapshots - it's the same
        sameFiles.put(fileName, fileInfo);
      } else {
        // File only in src (either flushed between snapshots or from compaction after dest snapshot) - it's different
        differentFiles.put(fileName, fileInfo);
      }
    }

    // Process destination snapshot files not in source
    for (Map.Entry<String, SstFileInfo> entry : destSnapFiles.entrySet()) {
      String fileName = entry.getKey();
      SstFileInfo fileInfo = entry.getValue();

      if (!srcSnapFiles.containsKey(fileName) &&
          !sameFiles.containsKey(fileName) &&
          !differentFiles.containsKey(fileName)) {
        // File only in dest - it's different
        differentFiles.put(fileName, fileInfo);
      }
    }

    LOG.debug("Flush-based diff: src={}, dest={}, same={}, different={}",
        src.getDbPath(), dest.getDbPath(), sameFiles.size(), differentFiles.size());
  }

  public String getMetadataDir() {
    return metadataDir;
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
   * Periodically prunes old flush log entries and backup SST files using
   * flush-based tracking. This is the replacement for pruneOlderSnapshotsWithCompactionHistory().
   *
   * This task removes flush entries that are older than maxSnapshotHistoryRetentionMs
   * and cleans up the corresponding backup SST files.
   */
  public void pruneOlderSnapshotsWithFlushHistory() {
    if (!shouldRun()) {
      return;
    }

    long pruneStartTime = System.currentTimeMillis();
    long cutoffTime = pruneStartTime - maxSnapshotHistoryRetentionMs;

    // Prune old entries from FlushLinkedList based on flush time and get list of pruned file names
    List<String> prunedFiles = flushLinkedList.pruneOlderThanTime(cutoffTime);

    if (CollectionUtils.isNotEmpty(prunedFiles)) {
      LOG.info("Pruned {} flush entries older than time {}",
          prunedFiles.size(), cutoffTime);
    }

    // Remove backup SST files and FlushLogTable entries for pruned flush entries
    try (UncheckedAutoCloseable lock = getBootstrapStateLock().acquireReadLock()) {
      removeSstFiles(new HashSet<>(prunedFiles));
      removeOlderFlushLogEntries(cutoffTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during flush history pruning", e);
    }
  }

  /**
   * Remove flush log entries older than the specified timestamp from FlushLogTable.
   *
   * @param cutoffTime Timestamp in milliseconds; entries with flushTime older than this are removed
   */
  private synchronized void removeOlderFlushLogEntries(long cutoffTime) {
    List<byte[]> keysToRemove = new ArrayList<>();

    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(flushLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();

      while (managedRocksIterator.get().isValid()) {
        byte[] value = managedRocksIterator.get().value();
        FlushLogEntry flushLogEntry =
            FlushLogEntry.getFromProtobuf(FlushLogEntryProto.parseFrom(value));

        if (flushLogEntry.getFlushTime() < cutoffTime) {
          keysToRemove.add(managedRocksIterator.get().key());
        } else {
          // Entries are ordered by time, so we can stop here
          break;
        }

        managedRocksIterator.get().next();
      }

      // Delete old entries
      for (byte[] key : keysToRemove) {
        activeRocksDB.get().delete(flushLogTableCFHandle, key);
      }

      if (!keysToRemove.isEmpty()) {
        LOG.info("Removed {} old flush log entries from FlushLogTable", keysToRemove.size());
      }
    } catch (InvalidProtocolBufferException | RocksDBException e) {
      throw new RuntimeException("Failed to remove old flush log entries", e);
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
   * Defines the task that removes OMKeyInfo from SST files from backup directory to
   * save disk space.
   */
  public void pruneSstFileValues() {
    if (!shouldRun()) {
      return;
    }
    long batchStartTime = System.nanoTime();
    int filesPrunedInBatch = 0;
    int filesSkippedInBatch = 0;
    int batchCounter = 0;

    Path sstBackupDirPath = Paths.get(sstBackupDir);
    Path prunedSSTFilePath = sstBackupDirPath.resolve(PRUNED_SST_FILE_TEMP);
    try (ManagedOptions managedOptions = new ManagedOptions();
         ManagedEnvOptions envOptions = new ManagedEnvOptions()) {
      byte[] compactionLogEntryKey;
      while ((compactionLogEntryKey = pruneQueue.peek()) != null && ++batchCounter <= pruneSSTFileBatchSize) {
        CompactionLogEntry compactionLogEntry;
        // Get the compaction log entry.
        synchronized (this) {
          try {
            compactionLogEntry = CompactionLogEntry.getCodec().fromPersistedFormat(
                activeRocksDB.get().get(compactionLogTableCFHandle, compactionLogEntryKey));
          } catch (RocksDBException ex) {
            throw new RocksDatabaseException("Failed to get compaction log entry.", ex);
          }

          boolean shouldUpdateTable = false;
          List<CompactionFileInfo> fileInfoList = compactionLogEntry.getInputFileInfoList();
          List<CompactionFileInfo> updatedFileInfoList = new ArrayList<>();
          for (CompactionFileInfo fileInfo : fileInfoList) {
            // Skip pruning file if it is already pruned or is removed.
            if (fileInfo.isPruned()) {
              updatedFileInfoList.add(fileInfo);
              continue;
            }
            Path sstFilePath = sstBackupDirPath.resolve(fileInfo.getFileName() + ROCKSDB_SST_SUFFIX);
            if (Files.notExists(sstFilePath)) {
              LOG.debug("Skipping pruning SST file {} as it does not exist in backup directory.", sstFilePath);
              updatedFileInfoList.add(fileInfo);
              filesSkippedInBatch++;
              continue;
            }

            // Prune file.sst => pruned.sst.tmp
            Files.deleteIfExists(prunedSSTFilePath);
            removeValueFromSSTFile(managedOptions, sstFilePath.toFile().getAbsolutePath(), prunedSSTFilePath.toFile());

            // Move pruned.sst.tmp => file.sst and replace existing file atomically.
            try (UncheckedAutoCloseable lock = getBootstrapStateLock().acquireReadLock()) {
              Files.move(prunedSSTFilePath, sstFilePath,
                  StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            }
            shouldUpdateTable = true;
            fileInfo.setPruned();
            updatedFileInfoList.add(fileInfo);
            LOG.debug("Completed pruning OMKeyInfo from {}", sstFilePath);
            filesPrunedInBatch++;
          }

          // Update compaction log entry in table.
          if (shouldUpdateTable) {
            CompactionLogEntry.Builder builder = compactionLogEntry.toBuilder();
            builder.updateInputFileInfoList(updatedFileInfoList);
            try {
              activeRocksDB.get().put(compactionLogTableCFHandle, compactionLogEntryKey,
                  builder.build().getProtobuf().toByteArray());
            } catch (RocksDBException ex) {
              throw new RocksDatabaseException("Failed to update the compaction log table for entry: "
                  + compactionLogEntry, ex);
            }
          }
        }
        pruneQueue.poll();
      }
    } catch (IOException | InterruptedException e) {
      LOG.error("Could not prune source OMKeyInfo from backup SST files.", e);
      sstFilePruningMetrics.incrPruningFailures();
    } finally {
      LOG.info("Completed pruning OMKeyInfo from backup SST files in {}ms.",
          (System.nanoTime() - batchStartTime) / 1_000_000);
      sstFilePruningMetrics.updateBatchLevelMetrics(filesPrunedInBatch, filesSkippedInBatch,
          batchCounter, pruneQueue.size());
    }
  }

  private void removeValueFromSSTFile(ManagedOptions options, String sstFilePath, File prunedFile) throws IOException {
    try (ManagedRawSSTFileReader sstFileReader = new ManagedRawSSTFileReader(options, sstFilePath, SST_READ_AHEAD_SIZE);
         ManagedRawSSTFileIterator<Pair<CodecBuffer, Integer>> itr = sstFileReader.newIterator(
             keyValue -> Pair.of(keyValue.getKey(), keyValue.getType()), null, null, KEY_ONLY);
         RDBSstFileWriter sstFileWriter = new RDBSstFileWriter(prunedFile);
         CodecBuffer emptyCodecBuffer = CodecBuffer.getEmptyBuffer()) {
      while (itr.hasNext()) {
        Pair<CodecBuffer, Integer> keyValue = itr.next();
        if (keyValue.getValue() == 0) {
          sstFileWriter.delete(keyValue.getKey());
        } else {
          sstFileWriter.put(keyValue.getKey(), emptyCodecBuffer);
        }
      }
    }
  }

  public boolean shouldRun() {
    return !suspended.get();
  }

  @VisibleForTesting
  public FlushLinkedList getFlushLinkedList() {
    return flushLinkedList;
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
        ConfigurationSource configuration,
        Function<Boolean, UncheckedAutoCloseable> lockSupplier
    ) {
      return INSTANCE_MAP.computeIfAbsent(metadataDirName, (key) ->
          new RocksDBCheckpointDiffer(metadataDirName,
              sstBackupDirName,
              compactionLogDirName,
              activeDBLocationName,
              configuration,
              lockSupplier));
    }

    /**
     * Close RocksDBCheckpointDiffer object if value is present for the key.
     * @param cacheKey cacheKey is metadataDirName path which is used as key
     *                for cache.
     */
    public static void invalidateCacheEntry(String cacheKey) {
      IOUtils.close(LOG, INSTANCE_MAP.remove(cacheKey));
    }
  }

  @Override
  public BootstrapStateHandler.Lock getBootstrapStateLock() {
    return lock;
  }

  @VisibleForTesting
  public SSTFilePruningMetrics getPruningMetrics() {
    return sstFilePruningMetrics;
  }
}
