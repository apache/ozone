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
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.hdds.utils.db.managed.ManagedDBOptions;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.DBOptions;
import org.rocksdb.LiveFileMetaData;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileReader;
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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;

// TODO
//  8. Handle bootstrapping rocksDB for a new OM follower node
//      - new node will receive Active object store as well as all existing
//      rocksDB checkpoints.
//      - This bootstrapping should also receive the compaction-DAG information
//  9. Handle rebuilding the DAG for a lagging follower. There are two cases
//      - receive RATIS transactions to replay. Nothing needs to be done in
//      these cases.
//      - Getting the DB sync. This case needs to handle getting the
//      compaction-DAG information as well.

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
public class RocksDBCheckpointDiffer implements AutoCloseable {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  private final String sstBackupDir;
  private final String activeDBLocationStr;

  private final String compactionLogDir;

  /**
   * Compaction log path for DB compaction history persistence.
   * This is the source of truth for in-memory SST DAG reconstruction upon
   * OM restarts.
   * <p>
   * Initialized to the latest sequence number on OM startup. The log also rolls
   * over (gets appended to a new file) whenever an Ozone snapshot is taken.
   */
  private volatile String currentCompactionLogPath = null;

  static final String COMPACTION_LOG_FILE_NAME_SUFFIX = ".log";

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
  private static final int SST_FILE_EXTENSION_LENGTH =
      SST_FILE_EXTENSION.length();

  private static final int LONG_MAX_STR_LEN =
      String.valueOf(Long.MAX_VALUE).length();

  /**
   * Used during DAG reconstruction.
   */
  private long reconstructionSnapshotGeneration;
  private String reconstructionLastSnapshotID;

  /**
   * Dummy object that acts as a write lock in compaction listener.
   */
  private final Object compactionListenerWriteLock = new Object();

  private final ScheduledExecutorService executor;
  private boolean closed;
  private final long maxAllowedTimeInDag;

  private ColumnFamilyHandle snapshotInfoTableCFHandle;

  /**
   * Constructor.
   * Note that previous compaction logs are loaded by RDBStore after this
   * object's initialization by calling loadAllCompactionLogs().
   *
   * @param metadataDirName Ozone metadata directory.
   * @param sstBackupDirName Name of the SST backup dir under metadata dir.
   * @param compactionLogDirName Name of the compaction log dir.
   * @param activeDBLocationName Active RocksDB directory's location.
   * @param maxTimeAllowedForSnapshotInDagInMs Time after which snapshot will be
   *                                           pruned from the DAG by daemon.
   * @param pruneCompactionDagDaemonRunIntervalInMs Internal at which DAG
   *                                               pruning daemon will run.
   */
  public RocksDBCheckpointDiffer(String metadataDirName,
                                 String sstBackupDirName,
                                 String compactionLogDirName,
                                 String activeDBLocationName,
                                 long maxTimeAllowedForSnapshotInDagInMs,
                                 long pruneCompactionDagDaemonRunIntervalInMs) {
    Preconditions.checkNotNull(metadataDirName);
    Preconditions.checkNotNull(sstBackupDirName);
    Preconditions.checkNotNull(compactionLogDirName);
    Preconditions.checkNotNull(activeDBLocationName);

    this.compactionLogDir =
        createCompactionLogDir(metadataDirName, compactionLogDirName);
    this.sstBackupDir = Paths.get(metadataDirName, sstBackupDirName) + "/";
    createSstBackUpDir();

    // Active DB location is used in getSSTFileSummary
    this.activeDBLocationStr = activeDBLocationName + "/";
    this.maxAllowedTimeInDag = maxTimeAllowedForSnapshotInDagInMs;

    if (pruneCompactionDagDaemonRunIntervalInMs > 0) {
      this.executor = Executors.newSingleThreadScheduledExecutor();
      this.executor.scheduleWithFixedDelay(
          this::pruneOlderSnapshotsWithCompactionHistory,
          pruneCompactionDagDaemonRunIntervalInMs,
          pruneCompactionDagDaemonRunIntervalInMs,
          TimeUnit.MILLISECONDS);

      this.executor.scheduleWithFixedDelay(
          this::pruneSstFiles,
          pruneCompactionDagDaemonRunIntervalInMs,
          pruneCompactionDagDaemonRunIntervalInMs,
          TimeUnit.MILLISECONDS
      );
    } else {
      this.executor = null;
    }
  }

  private String createCompactionLogDir(String metadataDir,
                                        String compactionLogDirName) {

    final File parentDir = new File(metadataDir);
    if (!parentDir.exists()) {
      if (!parentDir.mkdirs()) {
        LOG.error("Error creating compaction log parent dir.");
        return null;
      }
    }

    final String compactionLogDirectory =
        Paths.get(metadataDir, compactionLogDirName).toString();
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

  /**
   * Set the current compaction log filename with a given RDB sequence number.
   * @param latestSequenceNum latest sequence number of RDB.
   */
  public void setCurrentCompactionLog(long latestSequenceNum) {
    String latestSequenceIdStr = String.valueOf(latestSequenceNum);

    if (latestSequenceIdStr.length() < LONG_MAX_STR_LEN) {
      // Pad zeroes to the left for ordered file listing when sorted
      // alphabetically.
      latestSequenceIdStr =
          StringUtils.leftPad(latestSequenceIdStr, LONG_MAX_STR_LEN, "0");
    }

    // Local temp variable for storing the new compaction log file path
    final String newCompactionLog = compactionLogDir + latestSequenceIdStr +
        COMPACTION_LOG_FILE_NAME_SUFFIX;

    File clFile = new File(newCompactionLog);
    if (clFile.exists()) {
      LOG.warn("Compaction log exists: {}. Will append", newCompactionLog);
    }

    this.currentCompactionLogPath = newCompactionLog;

    // Create empty file if it doesn't exist
    appendToCurrentCompactionLog("");
  }

  @Override
  public void close() {
    synchronized (compactionListenerWriteLock) {
      if (!closed) {
        closed = true;
        if (executor != null) {
          executor.shutdown();
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
   * Append (then flush) to the current compaction log file path.
   * Note: This does NOT automatically append newline to the log.
   */
  private synchronized void appendToCurrentCompactionLog(String content) {
    if (currentCompactionLogPath == null) {
      LOG.error("Unable to append compaction log. "
          + "Compaction log path is not set. "
          + "Please check initialization.");
      throw new RuntimeException("Compaction log path not set");
    }
    try (BufferedWriter bw = Files.newBufferedWriter(
        Paths.get(currentCompactionLogPath),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      bw.write(content);
      bw.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to append compaction log to " +
          currentCompactionLogPath, e);
    }
  }

  /**
   * Append a sequence number to the compaction log (roughly) when an Ozone
   * snapshot (RDB checkpoint) is taken.
   */
  public void appendSnapshotInfoToCompactionLog(long sequenceNum,
                                                String snapshotID,
                                                long creationTime) {
    final String line = COMPACTION_LOG_SEQ_NUM_LINE_PREFIX + sequenceNum +
        SPACE_DELIMITER + snapshotID + SPACE_DELIMITER + creationTime + "\n";
    appendToCurrentCompactionLog(line);
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

  public void setRocksDBForCompactionTracking(DBOptions rocksOptions)
      throws RocksDBException {
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

  private AbstractEventListener newCompactionBeginListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionBegin(RocksDB db,
          CompactionJobInfo compactionJobInfo) {

        if (compactionJobInfo.inputFiles().size() == 0) {
          LOG.error("Compaction input files list is empty");
          return;
        }

        // Note the current compaction listener implementation does not
        // differentiate which column family each SST store. It is tracking
        // all SST files.

        synchronized (compactionListenerWriteLock) {
          if (closed) {
            return;
          }

          // Skip compaction DAG tracking if the snapshotInfoTable is empty.
          // i.e. No snapshot exists in OM.
          if (isSnapshotInfoTableEmpty(db)) {
            return;
          }

          // Create hardlink backups for the SST files that are going
          // to be deleted after this RDB compaction.
          for (String file : compactionJobInfo.inputFiles()) {
            LOG.debug("Creating hard link for '{}'", file);
            String saveLinkFileName =
                sstBackupDir + new File(file).getName();
            Path link = Paths.get(saveLinkFileName);
            Path srcFile = Paths.get(file);
            try {
              Files.createLink(link, srcFile);
            } catch (FileAlreadyExistsException ignored) {
              // This could happen if a previous compaction is a "trivial move",
              // where output SSTs files are exactly the same as input files.
              // Those SSTs are simply moved to the next level without rewrites
              // or renames.
              LOG.debug("SST file already exists: {}", file);
            } catch (IOException e) {
              LOG.error("Exception in creating hard link for {}", file);
              throw new RuntimeException("Failed to create hard link", e);
            }
          }

        }
      }
    };
  }

  private AbstractEventListener newCompactionCompletedListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionCompleted(RocksDB db,
          CompactionJobInfo compactionJobInfo) {

        if (compactionJobInfo.inputFiles().isEmpty()) {
          LOG.error("Compaction input files list is empty");
          return;
        }

        if (new HashSet<>(compactionJobInfo.inputFiles())
            .equals(new HashSet<>(compactionJobInfo.outputFiles()))) {
          LOG.info("Skipped the compaction entry. Compaction input files: " +
                  "{} and output files: {} are same.",
              compactionJobInfo.inputFiles(),
              compactionJobInfo.outputFiles());
          return;
        }

        final StringBuilder sb = new StringBuilder();

        if (LOG.isDebugEnabled()) {
          // Print compaction reason for this entry in the log file
          // e.g. kLevelL0FilesNum / kLevelMaxLevelSize.
          sb.append(COMPACTION_LOG_COMMENT_LINE_PREFIX)
              .append(compactionJobInfo.compactionReason())
              .append('\n');
        }

        // Mark the beginning of a compaction log
        sb.append(COMPACTION_LOG_ENTRY_LINE_PREFIX);

        // Trim DB path, only keep the SST file name
        final int filenameOffset =
            compactionJobInfo.inputFiles().get(0).lastIndexOf("/") + 1;

        // Append the list of input files
        final List<String> inputFiles = compactionJobInfo.inputFiles();
        // Trim the file path, leave only the SST file name without extension
        inputFiles.replaceAll(s -> s.substring(
            filenameOffset, s.length() - SST_FILE_EXTENSION_LENGTH));
        final String inputFilesJoined =
            String.join(COMPACTION_LOG_ENTRY_FILE_DELIMITER, inputFiles);
        sb.append(inputFilesJoined);

        // Insert delimiter between input files and output files
        sb.append(COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER);

        // Append the list of output files
        final List<String> outputFiles = compactionJobInfo.outputFiles();
        outputFiles.replaceAll(s -> s.substring(
            filenameOffset, s.length() - SST_FILE_EXTENSION_LENGTH));
        final String outputFilesJoined =
            String.join(COMPACTION_LOG_ENTRY_FILE_DELIMITER, outputFiles);
        sb.append(outputFilesJoined);

        // End of line
        sb.append('\n');

        String content = sb.toString();

        synchronized (compactionListenerWriteLock) {
          if (closed) {
            return;
          }

          // Skip compaction DAG tracking if the snapshotInfoTable is empty.
          // i.e. No snapshot exists in OM.
          if (isSnapshotInfoTableEmpty(db)) {
            return;
          }

          // Write input and output file names to compaction log
          appendToCurrentCompactionLog(content);

          // Populate the DAG
          // TODO: Once SnapshotChainManager is put into use, set snapshotID to
          //  snapshotChainManager.getLatestGlobalSnapshot()
          populateCompactionDAG(inputFiles, outputFiles, null,
              db.getLatestSequenceNumber());
        }
      }
    };
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

    Options option = new Options();
    SstFileReader reader = new SstFileReader(option);

    reader.open(getAbsoluteSstFilePath(filename));

    TableProperties properties = reader.getTableProperties();
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} has {} keys", filename, properties.getNumEntries());
    }
    return properties.getNumEntries();
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
   * Get a list of relevant column family descriptors.
   * @param cfOpts ColumnFamilyOptions
   * @return List of ColumnFamilyDescriptor
   */
  @VisibleForTesting
  static List<ColumnFamilyDescriptor> getCFDescriptorList(
      ColumnFamilyOptions cfOpts) {
    return asList(
        new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
        new ColumnFamilyDescriptor("keyTable".getBytes(UTF_8), cfOpts),
        new ColumnFamilyDescriptor("directoryTable".getBytes(UTF_8), cfOpts),
        new ColumnFamilyDescriptor("fileTable".getBytes(UTF_8), cfOpts)
    );
  }

  /**
   * Read the current Live manifest for a given RocksDB instance (Active or
   * Checkpoint).
   * @param dbPathArg path to a RocksDB directory
   * @return a list of SST files (without extension) in the DB.
   */
  public HashSet<String> readRocksDBLiveFiles(String dbPathArg) {
    ManagedRocksDB rocksDB = null;
    HashSet<String> liveFiles = new HashSet<>();

    final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        getCFDescriptorList(cfOpts);
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    try (ManagedDBOptions managedDBOptions = new ManagedDBOptions()) {
      managedDBOptions.setParanoidChecks(true);
      rocksDB = ManagedRocksDB.openReadOnly(managedDBOptions, dbPathArg,
          cfDescriptors, columnFamilyHandles);
      // Note it retrieves only the selected column families by the descriptor
      // i.e. keyTable, directoryTable, fileTable
      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.get().getLiveFilesMetaData();
      LOG.debug("SST File Metadata for DB: " + dbPathArg);
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.debug("File: {}, Level: {}", m.fileName(), m.level());
        final String trimmedFilename = trimSSTFilename(m.fileName());
        liveFiles.add(trimmedFilename);
      }
    } catch (RocksDBException e) {
      LOG.error("Error during RocksDB operation: {}", e.getMessage());
      e.printStackTrace();
    } finally {
      if (rocksDB != null) {
        rocksDB.close();
      }
      cfOpts.close();
    }
    return liveFiles;
  }

  /**
   * Process each line of compaction log text file input and populate the DAG.
   */
  synchronized void processCompactionLogLine(String line) {

    LOG.debug("Processing line: {}", line);

    if (line.startsWith("#")) {
      // Skip comments
      LOG.debug("Comment line, skipped");
    } else if (line.startsWith(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX)) {
      SnapshotLogInfo snapshotLogInfo = getSnapshotLogInfo(line);
      reconstructionSnapshotGeneration = snapshotLogInfo.snapshotGenerationId;
      reconstructionLastSnapshotID = snapshotLogInfo.snapshotId;
    } else if (line.startsWith(COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
      // Read compaction log entry

      // Trim the beginning
      line = line.substring(COMPACTION_LOG_ENTRY_LINE_PREFIX.length());
      String[] io =
          line.split(COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER);

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
      populateCompactionDAG(asList(inputFiles), asList(outputFiles),
          reconstructionLastSnapshotID, reconstructionSnapshotGeneration);
    } else {
      LOG.error("Invalid line in compaction log: {}", line);
    }
  }

  /**
   * Helper to read compaction log to the internal DAG.
   */
  private void readCompactionLogToDAG(String currCompactionLogPath) {
    LOG.debug("Loading compaction log: {}", currCompactionLogPath);
    try (Stream<String> logLineStream =
        Files.lines(Paths.get(currCompactionLogPath), UTF_8)) {
      logLineStream.forEach(this::processCompactionLogLine);
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  /**
   * Load existing compaction log files to the in-memory DAG.
   * This only needs to be done once during OM startup.
   */
  public synchronized void loadAllCompactionLogs() {
    if (compactionLogDir == null) {
      throw new RuntimeException("Compaction log directory must be set first");
    }
    reconstructionSnapshotGeneration = 0L;
    try {
      try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
          .filter(e -> e.toString().toLowerCase()
              .endsWith(COMPACTION_LOG_FILE_NAME_SUFFIX))
          .sorted()) {
        for (Path logPath : pathStream.collect(Collectors.toList())) {
          readCompactionLogToDAG(logPath.toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error listing compaction log dir " +
          compactionLogDir, e);
    }
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
   * A wrapper of getSSTDiffList() that returns the absolute path of SST files,
   * rather than SST file names without extension.
   *
   * @param src source snapshot
   * @param dest destination snapshot
   * @return A list of SST files without extension.
   *         e.g. ["/path/to/sstBackupDir/000050.sst",
   *               "/path/to/sstBackupDir/000060.sst"]
   */
  public List<String> getSSTDiffListWithFullPath(
      DifferSnapshotInfo src, DifferSnapshotInfo dest) throws IOException {

    List<String> sstDiffList = getSSTDiffList(src, dest);

    return sstDiffList.stream()
        .map(sst -> getSSTFullPath(sst, src.getDbPath()))
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
      DifferSnapshotInfo src, DifferSnapshotInfo dest) throws IOException {

    // TODO: Reject or swap if dest is taken after src, once snapshot chain
    //  integration is done.

    HashSet<String> srcSnapFiles = readRocksDBLiveFiles(src.getDbPath());
    HashSet<String> destSnapFiles = readRocksDBLiveFiles(dest.getDbPath());

    HashSet<String> fwdDAGSameFiles = new HashSet<>();
    HashSet<String> fwdDAGDifferentFiles = new HashSet<>();

    LOG.debug("Doing forward diff from src '{}' to dest '{}'",
        src.getDbPath(), dest.getDbPath());
    internalGetSSTDiffList(src, dest, srcSnapFiles, destSnapFiles,
        forwardCompactionDAG, fwdDAGSameFiles, fwdDAGDifferentFiles);

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
  void internalGetSSTDiffList(
      DifferSnapshotInfo src, DifferSnapshotInfo dest,
      Set<String> srcSnapFiles, Set<String> destSnapFiles,
      MutableGraph<CompactionNode> mutableGraph,
      Set<String> sameFiles, Set<String> differentFiles) {

    // Sanity check
    Preconditions.checkArgument(sameFiles.isEmpty(), "Set must be empty");
    Preconditions.checkArgument(differentFiles.isEmpty(), "Set must be empty");

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
          LOG.debug("Processing node: {}", current.getFileName());
          if (current.getSnapshotGeneration() < dest.getSnapshotGeneration()) {
            LOG.debug("Current node's snapshot generation '{}' "
                    + "reached destination snapshot's '{}'. "
                    + "Src '{}' and dest '{}' have different SST file: '{}'",
                current.getSnapshotGeneration(), dest.getSnapshotGeneration(),
                src.getDbPath(), dest.getDbPath(), current.getFileName());
            differentFiles.add(current.getFileName());
            continue;
          }

          Set<CompactionNode> successors = mutableGraph.successors(current);
          if (successors.isEmpty()) {
            LOG.debug("No further compaction happened to the current file. " +
                "Src '{}' and dest '{}' have different file: {}",
                src.getDbPath(), dest.getDbPath(), current.getFileName());
            differentFiles.add(current.getFileName());
            continue;
          }

          for (CompactionNode node : successors) {
            if (sameFiles.contains(node.getFileName()) ||
                differentFiles.contains(node.getFileName())) {
              LOG.debug("Skipping known processed SST: {}", node.getFileName());
              continue;
            }

            if (destSnapFiles.contains(node.getFileName())) {
              LOG.debug("Src '{}' and dest '{}' have the same SST: {}",
                  src.getDbPath(), dest.getDbPath(), node.getFileName());
              sameFiles.add(node.getFileName());
              continue;
            }

            // Queue different SST to the next level
            LOG.debug("Src '{}' and dest '{}' have a different SST: {}",
                src.getDbPath(), dest.getDbPath(), node.getFileName());
            nextLevel.add(node);
          }
        }
        currentLevel = nextLevel;
      }
    }
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

  public MutableGraph<CompactionNode> getForwardCompactionDAG() {
    return forwardCompactionDAG;
  }

  public MutableGraph<CompactionNode> getBackwardCompactionDAG() {
    return backwardCompactionDAG;
  }

  /**
   * Helper method to add a new file node to the DAG.
   * @return CompactionNode
   */
  private CompactionNode addNodeToDAG(String file, String snapshotID,
      long seqNum) {
    long numKeys = 0L;
    try {
      numKeys = getSSTFileSummary(file);
    } catch (RocksDBException e) {
      LOG.warn("Can't get num of keys in SST '{}': {}", file, e.getMessage());
    } catch (FileNotFoundException e) {
      LOG.info("Can't find SST '{}'", file);
    }
    CompactionNode fileNode = new CompactionNode(
        file, snapshotID, numKeys, seqNum);
    forwardCompactionDAG.addNode(fileNode);
    backwardCompactionDAG.addNode(fileNode);

    return fileNode;
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   * @param inputFiles List of compaction input files.
   * @param outputFiles List of compaction output files.
   * @param snapshotId Snapshot ID for debugging purpose. In fact, this can be
   *                   arbitrary String as long as it helps debugging.
   * @param seqNum DB transaction sequence number.
   */
  private void populateCompactionDAG(List<String> inputFiles,
      List<String> outputFiles, String snapshotId, long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (String outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(
          outfile, file -> addNodeToDAG(file, snapshotId, seqNum));

      for (String infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(
            infile, file -> addNodeToDAG(file, snapshotId, seqNum));
        // Draw the edges
        if (!outfileNode.getFileName().equals(infileNode.getFileName())) {
          forwardCompactionDAG.putEdge(outfileNode, infileNode);
          backwardCompactionDAG.putEdge(infileNode, outfileNode);
        }
      }
    }

  }

  /**
   * This is the task definition which is run periodically by the service
   * executor at fixed delay.
   * It looks for snapshots in compaction DAG which are older than the allowed
   * time to be in compaction DAG and removes them from the DAG.
   */
  public void pruneOlderSnapshotsWithCompactionHistory() {
    List<Path> olderSnapshotsLogFilePaths =
        getOlderSnapshotsCompactionLogFilePaths();
    List<String> lastCompactionSstFiles =
        getLastCompactionSstFiles(olderSnapshotsLogFilePaths);

    Set<String> sstFileNodesRemoved =
        pruneSnapshotFileNodesFromDag(lastCompactionSstFiles);
    removeSstFile(sstFileNodesRemoved);
    deleteOlderSnapshotsCompactionFiles(olderSnapshotsLogFilePaths);
  }

  /**
   * Deletes the SST file from the backup directory if exists.
   */
  private void removeSstFile(Set<String> sstFileNodes) {
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
   * Returns the list of compaction log files which are older than allowed
   * max time in the compaction DAG.
   */
  private List<Path> getOlderSnapshotsCompactionLogFilePaths() {
    long compactionLogPruneStartTime = System.currentTimeMillis();

    List<Path> compactionFiles =
        listCompactionLogFileFromCompactionLogDirectory();

    int index = compactionFiles.size() - 1;
    for (; index >= 0; index--) {
      Path compactionLogPath = compactionFiles.get(index);
      SnapshotLogInfo snapshotLogInfo =
          getSnapshotInfoFromLog(compactionLogPath);

      if (snapshotLogInfo == null) {
        continue;
      }

      if (compactionLogPruneStartTime - snapshotLogInfo.snapshotCreatedAt >
          maxAllowedTimeInDag) {
        break;
      }
    }

    if (index >= 0) {
      return compactionFiles.subList(0, index + 1);
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Returns the list of compaction log file path from compaction log directory.
   */
  private List<Path> listCompactionLogFileFromCompactionLogDirectory() {
    try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
        .filter(e -> e.toString().toLowerCase()
            .endsWith(COMPACTION_LOG_FILE_NAME_SUFFIX))
        .sorted()) {
      return pathStream.collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException("Error listing compaction log dir " +
          compactionLogDir, e);
    }
  }

  public void deleteOlderSnapshotsCompactionFiles(
      List<Path> olderSnapshotsLogFilePaths) {

    for (int i = 0; i < olderSnapshotsLogFilePaths.size(); i++) {
      Path olderSnapshotsLogFilePath = olderSnapshotsLogFilePaths.get(i);
      try {
        Files.deleteIfExists(olderSnapshotsLogFilePath);
      } catch (IOException exception) {
        LOG.error("Failed to deleted SST file: {}", olderSnapshotsLogFilePath,
            exception);
      }
    }
  }

  /**
   * Prunes forward and backward DAGs when oldest snapshot with compaction
   * history gets deleted.
   */
  public Set<String > pruneSnapshotFileNodesFromDag(List<String> sstFileNodes) {
    Set<CompactionNode> startNodes = new HashSet<>();
    for (String sstFileNode : sstFileNodes) {
      CompactionNode infileNode = compactionNodeMap.get(sstFileNode);
      if (infileNode == null) {
        LOG.warn("Compaction node doesn't exist for sstFile: {}.", sstFileNode);
        continue;
      }

      startNodes.add(infileNode);
    }

    pruneBackwardDag(backwardCompactionDAG, startNodes);
    Set<String> sstFilesPruned = pruneForwardDag(forwardCompactionDAG,
        startNodes);

    // Remove SST file nodes from compactionNodeMap too,
    // since those nodes won't be needed after clean up.
    sstFilesPruned.forEach(compactionNodeMap::remove);

    return sstFilesPruned;
  }

  /**
   * Prunes backward DAG's upstream from the level, that needs to be removed.
   */
  @VisibleForTesting
  Set<String> pruneBackwardDag(MutableGraph<CompactionNode> backwardDag,
                               Set<CompactionNode> startNodes) {
    Set<String> removedFiles = new HashSet<>();
    Set<CompactionNode> currentLevel = startNodes;

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

    return removedFiles;
  }

  private SnapshotLogInfo getSnapshotInfoFromLog(Path compactionLogFile) {
    AtomicReference<SnapshotLogInfo> snapshotLogInfo =
        new AtomicReference<>();
    try (Stream<String> logStream = Files.lines(compactionLogFile, UTF_8)) {
      logStream.forEach(logLine -> {
        if (!logLine.startsWith(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX)) {
          return;
        }

        snapshotLogInfo.set(getSnapshotLogInfo(logLine));
      });
    } catch (IOException exception) {
      throw new RuntimeException("Failed to read compaction log file: " +
          compactionLogFile, exception);
    }

    return snapshotLogInfo.get();
  }

  /**
   * Converts a snapshot compaction log line to SnapshotLogInfo.
   */
  private SnapshotLogInfo getSnapshotLogInfo(String logLine) {
    // Remove `S ` from the line.
    String line =
        logLine.substring(COMPACTION_LOG_SEQ_NUM_LINE_PREFIX.length());

    String[] splits = line.split(SPACE_DELIMITER);
    Preconditions.checkArgument(splits.length == 3,
        "Snapshot info log statement has more than expected parameters.");

    return new SnapshotLogInfo(Long.parseLong(splits[0]),
        splits[1],
        Long.parseLong(splits[2]));
  }

  /**
   * Returns the list of SST files got compacted in the last compaction from
   * the provided list of compaction log files.
   * We can't simply use last file from the list because it is possible that
   * no compaction happened between the last snapshot and previous to that.
   * Hence, we go over the list in reverse order and return the SST files from
   * first the compaction happened in the reverse list.
   * If no compaction happen at all, it returns empty list.
   */
  private List<String> getLastCompactionSstFiles(
      List<Path> compactionLogFiles
  ) {

    if (compactionLogFiles.isEmpty()) {
      return Collections.emptyList();
    }
    compactionLogFiles = new ArrayList<>(compactionLogFiles);
    Collections.reverse(compactionLogFiles);

    for (Path compactionLogFile: compactionLogFiles) {
      List<String> sstFiles = getLastCompactionSstFiles(compactionLogFile);
      if (!sstFiles.isEmpty()) {
        return  sstFiles;
      }
    }

    return Collections.emptyList();
  }

  private List<String>  getLastCompactionSstFiles(Path compactionLogFile) {

    AtomicReference<String> sstFiles = new AtomicReference<>();

    try (Stream<String> logStream = Files.lines(compactionLogFile, UTF_8)) {
      logStream.forEach(logLine -> {
        if (!logLine.startsWith(COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
          return;
        }
        sstFiles.set(logLine);
      });
    } catch (IOException exception) {
      throw new RuntimeException("Failed to read file: " + compactionLogFile,
          exception);
    }

    String lastCompactionLogEntry = sstFiles.get();

    if (StringUtils.isEmpty(lastCompactionLogEntry)) {
      return Collections.emptyList();
    }

    // Trim the beginning
    lastCompactionLogEntry = lastCompactionLogEntry
        .substring(COMPACTION_LOG_ENTRY_LINE_PREFIX.length());

    String[] io = lastCompactionLogEntry
        .split(COMPACTION_LOG_ENTRY_INPUT_OUTPUT_FILES_DELIMITER);

    assert (io.length == 2);

    String[] outputFiles = io[1].split(COMPACTION_LOG_ENTRY_FILE_DELIMITER);

    return asList(outputFiles);
  }

  public String getSSTBackupDir() {
    return sstBackupDir;
  }

  private static final class SnapshotLogInfo {
    private final long snapshotGenerationId;
    private final String snapshotId;
    private final long snapshotCreatedAt;

    private SnapshotLogInfo(long snapshotGenerationId,
                            String snapshotId,
                            long snapshotCreatedAt) {
      this.snapshotGenerationId = snapshotGenerationId;
      this.snapshotId = snapshotId;
      this.snapshotCreatedAt = snapshotCreatedAt;
    }
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
  public void pruneSstFiles() {
    Set<String> nonLeafSstFiles;

    synchronized (compactionListenerWriteLock) {
      nonLeafSstFiles = forwardCompactionDAG.nodes().stream()
          .filter(node -> !forwardCompactionDAG.successors(node).isEmpty())
          .map(node -> node.getFileName())
          .collect(Collectors.toSet());
    }

    removeSstFile(nonLeafSstFiles);
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
}
