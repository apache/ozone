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
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import org.apache.commons.lang3.StringUtils;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
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
//      - recieve RATIS transactions to replay. Nothing needs to be done in
//      thise case.
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
public class RocksDBCheckpointDiffer {

  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  private final String sstBackupDir;
  private final String activeDBLocationStr;

  private String compactionLogDir = null;

  /**
   * Compaction log path for DB compaction history persistence.
   * This is the source of truth for in-memory SST DAG reconstruction upon
   * OM restarts.
   * <p>
   * Initialized to the latest sequence number on OM startup. The log also rolls
   * over (gets appended to a new file) whenever an Ozone snapshot is taken.
   */
  private volatile String currentCompactionLogPath = null;

  private static final String COMPACTION_LOG_FILENAME_SUFFIX = ".log";

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
  private static final String COMPACTION_LOG_SEQNUM_LINE_PREFIX = "S ";

  /**
   * SST file extension. Must be lower case.
   * Used to trim the file extension when writing compaction entries to the log
   * to save space.
   */
  private static final String SST_FILE_EXTENSION = ".sst";
  private static final int SST_FILE_EXTENSION_LENGTH =
      SST_FILE_EXTENSION.length();

  private static final int LONG_MAX_STRLEN =
      String.valueOf(Long.MAX_VALUE).length();

  private long reconstructionSnapshotGeneration;

  /**
   * Dummy object that acts as a write lock in compaction listener.
   */
  private final Object compactionListenerWriteLock = new Object();

  /**
   * Constructor.
   * Note that previous compaction logs are loaded by RDBStore after this
   * object's initialization by calling loadAllCompactionLogs().
   * @param metadataDir Ozone metadata directory.
   * @param sstBackupDir Name of the SST backup dir under metadata dir.
   * @param compactionLogDirName Name of the compaction log dir.
   */
  public RocksDBCheckpointDiffer(String metadataDir, String sstBackupDir,
      String compactionLogDirName, File activeDBLocation) {

    setCompactionLogDir(metadataDir, compactionLogDirName);

    this.sstBackupDir = Paths.get(metadataDir, sstBackupDir) + "/";

    // Create the directory if SST backup path does not already exist
    File dir = new File(this.sstBackupDir);
    if (!dir.exists() && !dir.mkdir()) {
      final String errorMsg = "Failed to create SST file backup directory. "
          + "Check if OM has write permission.";
      LOG.error(errorMsg);
      throw new RuntimeException(errorMsg);
    }

    // Active DB location is used in getSSTFileSummary
    this.activeDBLocationStr = activeDBLocation.toString() + "/";
  }

  private void setCompactionLogDir(String metadataDir,
      String compactionLogDirName) {

    final File parentDir = new File(metadataDir);
    if (!parentDir.exists()) {
      if (!parentDir.mkdir()) {
        LOG.error("Error creating compaction log parent dir.");
        return;
      }
    }

    this.compactionLogDir =
        Paths.get(metadataDir, compactionLogDirName).toString();
    File clDir = new File(compactionLogDir);
    if (!clDir.exists() && !clDir.mkdir()) {
      LOG.error("Error creating compaction log dir.");
      return;
    }

    // Create a readme file explaining what the compaction log dir is for
    final Path readmePath = Paths.get(compactionLogDir, "_README.txt");
    final File readmeFile = new File(readmePath.toString());
    if (!readmeFile.exists()) {
      try (BufferedWriter bw = Files.newBufferedWriter(
          readmePath, StandardOpenOption.CREATE)) {
        bw.write("This directory holds Ozone Manager RocksDB compaction logs.\n"
            + "DO NOT add, change or delete any files in this directory unless "
            + "you know what you are doing.\n");
      } catch (IOException ignored) {
      }
    }

    // Append /
    this.compactionLogDir += "/";
  }

  /**
   * Set the current compaction log filename with a given RDB sequence number.
   * @param latestSequenceNum latest sequence number of RDB.
   */
  public void setCurrentCompactionLog(long latestSequenceNum) {
    String latestSequenceIdStr = String.valueOf(latestSequenceNum);

    if (latestSequenceIdStr.length() < LONG_MAX_STRLEN) {
      // Pad zeroes to the left for ordered file listing when sorted
      // alphabetically.
      latestSequenceIdStr =
          StringUtils.leftPad(latestSequenceIdStr, LONG_MAX_STRLEN, "0");
    }

    // Local temp variable for storing the new compaction log file path
    final String newCompactionLog =
        compactionLogDir + latestSequenceIdStr + COMPACTION_LOG_FILENAME_SUFFIX;

    File clFile = new File(newCompactionLog);
    if (clFile.exists()) {
      LOG.warn("Compaction log exists: {}. Will append", newCompactionLog);
    }

    this.currentCompactionLogPath = newCompactionLog;

    // Create empty file if it doesn't exist
    appendToCurrentCompactionLog("");
  }

  // Node in the DAG to represent an SST file
  private static class CompactionNode {
    // Name of the SST file
    private final String fileName;
    // The last snapshot created before this node came into existence
    private final String snapshotId;
    private final long snapshotGeneration;
    private final long totalNumberOfKeys;
    private long cumulativeKeysReverseTraversal;

    CompactionNode(String file, String ssId, long numKeys, long seqNum) {
      fileName = file;
      // Retained for debuggability. Unused for now.
      snapshotId = ssId;
      totalNumberOfKeys = numKeys;
      snapshotGeneration = seqNum;
      cumulativeKeysReverseTraversal = 0L;
    }

    @Override
    public String toString() {
      return String.format("Node{%s}", fileName);
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
   * @param sequenceNum RDB sequence number
   */
  public void appendSequenceNumberToCompactionLog(long sequenceNum) {
    final String line = COMPACTION_LOG_SEQNUM_LINE_PREFIX + sequenceNum + "\n";
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

  private AbstractEventListener newCompactionBeginListener() {
    return new AbstractEventListener() {
      @Override
      public void onCompactionBegin(RocksDB db,
          CompactionJobInfo compactionJobInfo) {

        synchronized (compactionListenerWriteLock) {

          if (compactionJobInfo.inputFiles().size() == 0) {
            LOG.error("Compaction input files list is empty");
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

        synchronized (compactionListenerWriteLock) {

          if (compactionJobInfo.inputFiles().isEmpty()) {
            LOG.error("Compaction input files list is empty");
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
          final String inputFilesJoined = String.join(",", inputFiles);
          sb.append(inputFilesJoined);

          // Insert delimiter between input files an output files
          sb.append(':');

          // Append the list of output files
          final List<String> outputFiles = compactionJobInfo.outputFiles();
          outputFiles.replaceAll(s -> s.substring(
              filenameOffset, s.length() - SST_FILE_EXTENSION_LENGTH));
          final String outputFilesJoined = String.join(",", outputFiles);
          sb.append(outputFilesJoined);

          // End of line
          sb.append('\n');

          // Write input and output file names to compaction log
          appendToCurrentCompactionLog(sb.toString());

          // Populate the DAG
          populateCompactionDAG(inputFiles, outputFiles,
              db.getLatestSequenceNumber());
/*
          if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
            printMutableGraph(null, null, compactionDAGFwd);
          }
 */
        }
      }
    };
  }

  /**
   * Get number of keys in an SST file.
   * @param filename SST filename
   * @return number of keys
   */
  private long getSSTFileSummary(String filename) throws RocksDBException {

    if (!filename.endsWith(SST_FILE_EXTENSION)) {
      filename += SST_FILE_EXTENSION;
    }

    Options option = new Options();
    SstFileReader reader = new SstFileReader(option);

    File sstFile = new File(sstBackupDir + filename);
    File sstFileInActiveDB = new File(activeDBLocationStr + filename);
    if (sstFile.exists()) {
      reader.open(sstBackupDir + filename);
    } else if (sstFileInActiveDB.exists()) {
      reader.open(activeDBLocationStr + filename);
    } else {
      throw new RuntimeException("Can't find SST file: " + filename);
    }

    TableProperties properties = reader.getTableProperties();
    if (LOG.isDebugEnabled()) {
      LOG.debug("{} has {} keys", filename, properties.getNumEntries());
    }
    return properties.getNumEntries();
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
    RocksDB rocksDB = null;
    HashSet<String> liveFiles = new HashSet<>();

    final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions();
    final List<ColumnFamilyDescriptor> cfDescriptors =
        getCFDescriptorList(cfOpts);
    final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();

    try (DBOptions dbOptions = new DBOptions()
        .setParanoidChecks(true)) {

      rocksDB = RocksDB.openReadOnly(dbOptions, dbPathArg,
          cfDescriptors, columnFamilyHandles);
      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
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
  private synchronized void processCompactionLogLine(String line) {

    LOG.debug("Processing line: {}", line);

    if (line.startsWith("#")) {
      // Skip comments
      LOG.debug("Comment line, skipped");
    } else if (line.startsWith(COMPACTION_LOG_SEQNUM_LINE_PREFIX)) {
      // Read sequence number
      LOG.debug("Reading sequence number as snapshot generation");
      final String seqNumStr =
          line.substring(COMPACTION_LOG_SEQNUM_LINE_PREFIX.length()).trim();
      // This would the snapshot generation for the nodes to come
      reconstructionSnapshotGeneration = Long.parseLong(seqNumStr);
    } else if (line.startsWith(COMPACTION_LOG_ENTRY_LINE_PREFIX)) {
      // Read compaction log entry

      // Trim the beginning
      line = line.substring(COMPACTION_LOG_SEQNUM_LINE_PREFIX.length());
      final String[] io = line.split(":");
      if (io.length != 2) {
        LOG.error("Invalid line in compaction log: {}", line);
        return;
      }
      final String[] inputFiles = io[0].split(",");
      final String[] outputFiles = io[1].split(",");
      populateCompactionDAG(asList(inputFiles),
          asList(outputFiles), reconstructionSnapshotGeneration);
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
              .endsWith(COMPACTION_LOG_FILENAME_SUFFIX))
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
   * Snapshot information node class for the differ.
   */
  public static class DifferSnapshotInfo {
    private final String dbPath;
    private final String snapshotID;
    private final long snapshotGeneration;

    public DifferSnapshotInfo(String db, String id, long gen) {
      dbPath = db;
      snapshotID = id;
      snapshotGeneration = gen;
    }

    public String getDbPath() {
      return dbPath;
    }

    public String getSnapshotID() {
      return snapshotID;
    }

    public long getSnapshotGeneration() {
      return snapshotGeneration;
    }

    @Override
    public String toString() {
      return "DifferSnapshotInfo{" + "dbPath='" + dbPath + '\''
          + ", snapshotID='" + snapshotID + '\'' + ", snapshotGeneration="
          + snapshotGeneration + '}';
    }
  }

  /**
   * Get a list of SST files that differs between src and destination snapshots.
   * <p>
   * Expected input: src is a snapshot taken AFTER the dest.
   *
   * @param src source snapshot
   * @param dest destination snapshot
   */
  public synchronized List<String> getSSTDiffList(
      DifferSnapshotInfo src, DifferSnapshotInfo dest) {

    HashSet<String> srcSnapFiles = readRocksDBLiveFiles(src.dbPath);
    HashSet<String> destSnapFiles = readRocksDBLiveFiles(dest.dbPath);

    HashSet<String> fwdDAGSameFiles = new HashSet<>();
    HashSet<String> fwdDAGDifferentFiles = new HashSet<>();

    LOG.debug("Doing forward diff from src '{}' to dest '{}'",
        src.dbPath, dest.dbPath);
    internalGetSSTDiffList(src, dest, srcSnapFiles, destSnapFiles,
        forwardCompactionDAG, fwdDAGSameFiles, fwdDAGDifferentFiles);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Result of diff from src '" + src.dbPath + "' to dest '" +
          dest.dbPath + "':");
      StringBuilder logSB = new StringBuilder();

      logSB.append("Fwd DAG same SST files:      ");
      for (String file : fwdDAGSameFiles) {
        logSB.append(file).append(" ");
      }
      LOG.debug(logSB.toString());

      logSB.setLength(0);
      logSB.append("Fwd DAG different SST files: ");
      for (String file : fwdDAGDifferentFiles) {
        logSB.append(file).append(" ");
      }
      LOG.debug("{}", logSB);
    }

    return new ArrayList<>(fwdDAGDifferentFiles);
  }

  /**
   * Core getSSTDiffList logic.
   */
  private void internalGetSSTDiffList(
      DifferSnapshotInfo src, DifferSnapshotInfo dest,
      HashSet<String> srcSnapFiles, HashSet<String> destSnapFiles,
      MutableGraph<CompactionNode> mutableGraph,
      HashSet<String> sameFiles, HashSet<String> differentFiles) {

    for (String fileName : srcSnapFiles) {
      if (destSnapFiles.contains(fileName)) {
        LOG.debug("Source '{}' and destination '{}' share the same SST '{}'",
            src.dbPath, dest.dbPath, fileName);
        sameFiles.add(fileName);
        continue;
      }

      CompactionNode infileNode = compactionNodeMap.get(fileName);
      if (infileNode == null) {
        LOG.debug("Source '{}' SST file '{}' is never compacted",
            src.dbPath, fileName);
        differentFiles.add(fileName);
        continue;
      }

      LOG.debug("Expanding SST file: {}", fileName);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.add(infileNode);
      // Traversal level/depth indicator for debug print
      int level = 1;
      while (!currentLevel.isEmpty()) {
        LOG.debug("BFS level: {}. Current level has {} nodes.",
            level++, currentLevel.size());

        final Set<CompactionNode> nextLevel = new HashSet<>();
        for (CompactionNode current : currentLevel) {
          LOG.debug("Processing node: {}", current.fileName);
          if (current.snapshotGeneration <= dest.snapshotGeneration) {
            LOG.debug("Current node's snapshot generation '{}' "
                    + "reached destination snapshot's '{}'. "
                    + "Src '{}' and dest '{}' have different SST file: '{}'",
                current.snapshotGeneration, dest.snapshotGeneration,
                src.dbPath, dest.dbPath, current.fileName);
            differentFiles.add(current.fileName);
            continue;
          }

          Set<CompactionNode> successors = mutableGraph.successors(current);
          if (successors.isEmpty()) {
            LOG.debug("No further compaction happened to the current file. " +
                "Src '{}' and dest '{}' have different file: {}",
                src.dbPath, dest.dbPath, current.fileName);
            differentFiles.add(current.fileName);
            continue;
          }

          for (CompactionNode node : successors) {
            if (sameFiles.contains(node.fileName) ||
                differentFiles.contains(node.fileName)) {
              LOG.debug("Skipping known processed SST: {}", node.fileName);
              continue;
            }

            if (destSnapFiles.contains(node.fileName)) {
              LOG.debug("Src '{}' and dest '{}' have the same SST: {}",
                  src.dbPath, dest.dbPath, node.fileName);
              sameFiles.add(node.fileName);
              continue;
            }

            // Queue different SST to the next level
            LOG.debug("Src '{}' and dest '{}' have a different SST: {}",
                src.dbPath, dest.dbPath, node.fileName);
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
      return a.fileName.compareToIgnoreCase(b.fileName);
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
      LOG.debug("File '{}' total keys: {}", n.fileName, n.totalNumberOfKeys);
      LOG.debug("File '{}' cumulative keys: {}", n.fileName,
          n.cumulativeKeysReverseTraversal);
    }
  }

  @VisibleForTesting
  public synchronized void printMutableGraphFromAGivenNode(String fileName,
      int sstLevel, MutableGraph<CompactionNode> mutableGraph) {

    CompactionNode infileNode = compactionNodeMap.get(fileName);
    if (infileNode == null) {
      return;
    }
    LOG.debug("Expanding file: {}. SST compaction level: {}",
        fileName, sstLevel);
    Set<CompactionNode> currentLevel = new HashSet<>();
    currentLevel.add(infileNode);
    int levelCounter = 1;
    while (!currentLevel.isEmpty()) {
      LOG.debug("DAG Level: {}", levelCounter++);
      final Set<CompactionNode> nextLevel = new HashSet<>();
      StringBuilder sb = new StringBuilder();
      for (CompactionNode current : currentLevel) {
        Set<CompactionNode> successors = mutableGraph.successors(current);
        for (CompactionNode succNode : successors) {
          sb.append(succNode.fileName).append(" ");
          nextLevel.add(succNode);
        }
      }
      LOG.debug("{}", sb);
      currentLevel = nextLevel;
    }
  }

  synchronized void printMutableGraph(String srcSnapId, String destSnapId,
      MutableGraph<CompactionNode> mutableGraph) {

    LOG.debug("Gathering all SST file nodes from src '{}' to dest '{}'",
        srcSnapId, destSnapId);

    final Queue<CompactionNode> nodeQueue = new LinkedList<>();
    // Queue source snapshot SST file nodes
    for (CompactionNode node : mutableGraph.nodes()) {
      if (srcSnapId == null ||
          node.snapshotId.compareToIgnoreCase(srcSnapId) == 0) {
        nodeQueue.add(node);
      }
    }

    final Set<CompactionNode> allNodesSet = new HashSet<>();
    while (!nodeQueue.isEmpty()) {
      CompactionNode node = nodeQueue.poll();
      Set<CompactionNode> succSet = mutableGraph.successors(node);
      LOG.debug("Current node: {}", node);
      if (succSet.isEmpty()) {
        LOG.debug("Has no successor node");
        allNodesSet.add(node);
        continue;
      }
      for (CompactionNode succNode : succSet) {
        LOG.debug("Has successor node: {}", succNode);
        if (srcSnapId == null ||
            succNode.snapshotId.compareToIgnoreCase(destSnapId) == 0) {
          allNodesSet.add(succNode);
          continue;
        }
        nodeQueue.add(succNode);
      }
    }

    LOG.debug("Files are: {}", allNodesSet);
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
  private CompactionNode addNodeToDAG(String file, long seqNum) {
    long numKeys = 0L;
    try {
      numKeys = getSSTFileSummary(file);
    } catch (RocksDBException e) {
      LOG.warn("Can't get num of keys in SST '{}': {}", file, e.getMessage());
    }
    CompactionNode fileNode = new CompactionNode(file, null, numKeys, seqNum);
    forwardCompactionDAG.addNode(fileNode);
    backwardCompactionDAG.addNode(fileNode);

    return fileNode;
  }

  /**
   * Populate the compaction DAG with input and output SST files lists.
   */
  private void populateCompactionDAG(List<String> inputFiles,
      List<String> outputFiles, long seqNum) {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Input files: {} -> Output files: {}", inputFiles, outputFiles);
    }

    for (String outfile : outputFiles) {
      final CompactionNode outfileNode = compactionNodeMap.computeIfAbsent(
          outfile, file -> addNodeToDAG(file, seqNum));

      for (String infile : inputFiles) {
        final CompactionNode infileNode = compactionNodeMap.computeIfAbsent(
            infile, file -> addNodeToDAG(file, seqNum));
        // Draw the edges
        if (!outfileNode.fileName.equals(infileNode.fileName)) {
          forwardCompactionDAG.putEdge(outfileNode, infileNode);
          backwardCompactionDAG.putEdge(infileNode, outfileNode);
        }
      }
    }

  }

  @VisibleForTesting
  public synchronized void traverseGraph(
      MutableGraph<CompactionNode> reverseMutableGraph,
      MutableGraph<CompactionNode> fwdMutableGraph) {

    List<CompactionNode> nodeList = compactionNodeMap.values().stream()
        .sorted(new NodeComparator()).collect(Collectors.toList());

    for (CompactionNode infileNode : nodeList) {
      // fist go through fwdGraph to find nodes that don't have successors.
      // These nodes will be the top level nodes in reverse graph
      Set<CompactionNode> successors = fwdMutableGraph.successors(infileNode);
      if (successors.size() == 0) {
        LOG.debug("No successors. Cumulative keys: {}, total keys: {}",
            infileNode.cumulativeKeysReverseTraversal,
            infileNode.totalNumberOfKeys);
        infileNode.cumulativeKeysReverseTraversal =
            infileNode.totalNumberOfKeys;
      }
    }

    HashSet<CompactionNode> visited = new HashSet<>();
    for (CompactionNode infileNode : nodeList) {
      if (visited.contains(infileNode)) {
        continue;
      }
      visited.add(infileNode);
      LOG.debug("Visiting node '{}'", infileNode.fileName);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.add(infileNode);
      int level = 1;
      while (!currentLevel.isEmpty()) {
        LOG.debug("BFS Level: {}. Current level has {} nodes",
            level++, currentLevel.size());
        final Set<CompactionNode> nextLevel = new HashSet<>();
        for (CompactionNode current : currentLevel) {
          LOG.debug("Expanding node: {}", current.fileName);
          Set<CompactionNode> successors =
              reverseMutableGraph.successors(current);
          if (successors.isEmpty()) {
            LOG.debug("No successors. Cumulative keys: {}",
                current.cumulativeKeysReverseTraversal);
            continue;
          }
          for (CompactionNode node : successors) {
            LOG.debug("Adding to the next level: {}", node.fileName);
            LOG.debug("'{}' cumulative keys: {}. parent '{}' total keys: {}",
                node.fileName, node.cumulativeKeysReverseTraversal,
                current.fileName, current.totalNumberOfKeys);
            node.cumulativeKeysReverseTraversal +=
                current.cumulativeKeysReverseTraversal;
            nextLevel.add(node);
          }
        }
        currentLevel = nextLevel;
      }
    }
  }

  @VisibleForTesting
  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }

  @VisibleForTesting
  public static Logger getLog() {
    return LOG;
  }
}
