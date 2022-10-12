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

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.Checkpoint;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
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
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// TODO
//  1. Create a local instance of RocksDiff-local-RocksDB. This is the
//  rocksDB that we can use for maintaining DAG and any other state. This is
//  a per node state so it it doesn't have to go through RATIS anyway.
//  2. Store fwd DAG in Diff-Local-RocksDB in Compaction Listener
//  3. Store fwd DAG in Diff-Local-RocksDB in Compaction Listener
//  4. Store last-Snapshot-counter/Compaction-generation-counter in Diff-Local
//  -RocksDB in Compaction Listener
//  5. System Restart handling. Read the DAG from Disk and load it in memory.
//  6. Take the base snapshot. All the SST file nodes in the base snapshot
//  should be arked with that Snapshot generation. Subsequently, all SST file
//  node should have a snapshot-generation count and Compaction-generation
//  count.
//  6. DAG based SST file pruning. Start from the oldest snapshot and we can
//  unlink any SST
//  file from the SaveCompactedFilePath directory that is reachable in the
//  reverse DAG.
//  7. DAG pruning : For each snapshotted bucket, We can recycle the part of
//  the DAG that is older than the predefined policy for the efficient snapdiff.
//  E.g. we may decide not to support efficient snapdiff from any snapshot that
//  is older than 2 weeks.
//  Note on 8. & 9 .
//  ==================
//  A simple handling is to just iterate over all keys in keyspace when the
//  compaction DAG is lost, instead of optimizing every case. And start
//  Compaction-DAG afresh from the latest snapshot.
//  --
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
 *  RocksDBCheckpointDiffer class.
 */
public class RocksDBCheckpointDiffer {
  private final String rocksDbPath;
  private String cpPath;
  private String cfDBPath;
  private String saveCompactedFilePath;
  private int maxSnapshots;
  private static final Logger LOG =
      LoggerFactory.getLogger(RocksDBCheckpointDiffer.class);

  // keeps track of all the snapshots created so far.
  private int lastSnapshotCounter;
  private String lastSnapshotPrefix;

  // tracks number of compactions so far
  private static final long UNKNOWN_COMPACTION_GEN = 0;
  private long currentCompactionGen = 0;

  // Something to track all the snapshots created so far. TODO: REMOVE
  private Snapshot[] allSnapshots;

  private String compactionLogParentDir = null;
  private String compactionLogDir = null;

  // Name of the directory to hold compaction logs (under parent dir)
  private final String COMPACTION_LOG_DIR = "compaction-log/";

  // For DB compaction SST DAG persistence and reconstruction
  // Should be initialized to the latest sequence number
  private volatile String currentCompactionLogFilename = null;

  private final String COMPACTION_LOG_FILENAME_SUFFIX = ".log";

  public void setCompactionLogParentDir(String parentDir) {
    this.compactionLogParentDir = parentDir;

    // Append /
    if (!compactionLogParentDir.endsWith("/")) {
      compactionLogParentDir += "/";
    }

    File pDir = new File(compactionLogParentDir);
    if (!pDir.exists()) {
      if (!pDir.mkdir()) {
        LOG.error("Error creating compaction log parent dir.");
        return;
      }
    }

    compactionLogDir =
        Paths.get(compactionLogParentDir, COMPACTION_LOG_DIR).toString();
    File clDir = new File(compactionLogDir);
    if (!clDir.mkdir()) {
      LOG.error("Error creating compaction log dir.");
      return;
    }

    // TODO: Write a README there explaining what the dir is for
  }

  final int LONG_MAX_STRLEN = String.valueOf(Long.MAX_VALUE).length();

  public void setCompactionLogFilenameBySeqNum(long latestSequenceId) {
    String latestSequenceIdStr = String.valueOf(latestSequenceId);

    if (latestSequenceIdStr.length() < LONG_MAX_STRLEN) {
      // Pad zeroes to the left for sequential listing later
      latestSequenceIdStr =
          StringUtils.leftPad(latestSequenceIdStr, LONG_MAX_STRLEN, "0");
    }

    this.currentCompactionLogFilename = compactionLogParentDir +
        COMPACTION_LOG_DIR + latestSequenceIdStr +
        COMPACTION_LOG_FILENAME_SUFFIX;

    // Create empty file when if it doesn't exist.
    // Detect any write permission issue (if any) early.
    File clFile = new File(currentCompactionLogFilename);
    if (clFile.exists()) {
      LOG.warn("Compaction log exists: {}. Will append",
          currentCompactionLogFilename);
    } else {
      try {
        // Create empty file
        new FileOutputStream(clFile).close();
      } catch (IOException e) {
        // TODO: Throw some checked exception?
        throw new RuntimeException("Unable to start compaction log " +
            currentCompactionLogFilename, e);
      }
    }
  }

  public RocksDBCheckpointDiffer(String dbPath,
                                 int maxSnapshots,
                                 String checkpointPath,
                                 String sstFileSaveDir,
                                 String cfPath,
                                 int initialSnapshotCounter,
                                 String snapPrefix) {
    this.maxSnapshots = maxSnapshots;
    allSnapshots = new Snapshot[this.maxSnapshots];
    cpPath = checkpointPath;

    saveCompactedFilePath = sstFileSaveDir;

    // Append /
    if (!saveCompactedFilePath.endsWith("/")) {
      saveCompactedFilePath += "/";
    }

    // Create the directory if SST backup path does not already exist
    File dir = new File(saveCompactedFilePath);
    if (dir.exists()) {
      deleteDirectory(dir);  // TODO: FOR EASE OF TESTING ONLY. DO NOT DELETE DIR WHEN MERGING
    }
    if (!dir.mkdir()) {
      LOG.error("Failed to create SST file backup directory!");
      // TODO: Throw unrecoverable exception and Crash OM ?
      throw new RuntimeException("Failed to create SST file backup directory. "
          + "Check write permission.");
    }

    rocksDbPath = dbPath;
    cfDBPath = cfPath;

    // TODO: This module should be self sufficient in tracking the last
    //  snapshotCounter and currentCompactionGen for a given dbPath. It needs
    //  to be persisted.
    lastSnapshotCounter = initialSnapshotCounter;
    lastSnapshotPrefix = snapPrefix;
    currentCompactionGen = lastSnapshotCounter;

    // TODO: this should also independently persist every compaction e.g.
    //  (input files) ->
    //  {  (output files) + lastSnapshotCounter + currentCompactionGen }
    //  mapping.
  }

  /**
   * Helper function that recursively deletes the dir. TODO: REMOVE
   */
  boolean deleteDirectory(File directoryToBeDeleted) {
    File[] allContents = directoryToBeDeleted.listFiles();
    if (allContents != null) {
      for (File file : allContents) {
        deleteDirectory(file);
      }
    }
    return directoryToBeDeleted.delete();
  }

  // Node in the DAG to represent an SST file
  private class CompactionNode {
    public String fileName;   // Name of the SST file
    public String snapshotId; // The last snapshot that was created before this
    // node came into existance;
    public long snapshotGeneration;
    public long totalNumberOfKeys;
    public long cumulativeKeysReverseTraversal;

    CompactionNode (String f, String sid, long numKeys, long compactionGen) {
      fileName = f;
      snapshotId = sid;
      snapshotGeneration = lastSnapshotCounter;
      totalNumberOfKeys = numKeys;
      cumulativeKeysReverseTraversal = 0;
    }
  }

  private static class Snapshot {
    String dbPath;
    String snapshotID;
    long snapshotGeneration;

    Snapshot(String db, String id, long gen) {
      dbPath = db;
      snapshotID = id;
      snapshotGeneration = gen;
    }
  }

  public enum GType {FNAME, KEYSIZE, CUMUTATIVE_SIZE};


  // Hash table to track Compaction node for a given SST File.
  private ConcurrentHashMap<String, CompactionNode> compactionNodeTable =
      new ConcurrentHashMap<>();

  // We are mainiting a two way DAG. This allows easy traversal from
  // source snapshot to destination snapshot as well as the other direction.
  // TODO : Persist this information to the disk.
  // TODO: A system crash while the edge is inserted in DAGFwd but not in
  //  DAGReverse will compromise the two way DAG. Set of input/output files shud
  //  be written to // disk(RocksDB) first, would avoid this problem.

  private MutableGraph<CompactionNode> compactionDAGFwd =
      GraphBuilder.directed().build();

  private MutableGraph<CompactionNode> compactionDAGReverse =
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

  // Flushes the WAL and Creates a RocksDB checkpoint
  @SuppressWarnings({"NM_METHOD_NAMING_CONVENTION"})
  public void createCheckPoint(String dbPathArg, String cpPathArg,
                               RocksDB rocksDB) {
    LOG.warn("Creating Checkpoint for RocksDB instance : " +
        dbPathArg + "in a CheckPoint Location" + cpPathArg);
    try {
      rocksDB.flush(new FlushOptions());
      Checkpoint cp = Checkpoint.create(rocksDB);
      cp.createCheckpoint(cpPathArg);
    } catch (RocksDBException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  public void setRocksDBForCompactionTracking(DBOptions rocksOptions)
      throws RocksDBException {
    setRocksDBForCompactionTracking(rocksOptions, new ArrayList<>());
  }

  /**
   * This takes DBOptions.
   */
  public void setRocksDBForCompactionTracking(
      DBOptions rocksOptions, List<AbstractEventListener> list) {
    final AbstractEventListener onCompactionCompletedListener =
        new AbstractEventListener() {
          @Override
          @SuppressFBWarnings({
              "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
              "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
          public void onCompactionCompleted(
              final RocksDB db, final CompactionJobInfo compactionJobInfo) {
            synchronized (db) {

              LOG.warn(compactionJobInfo.compactionReason().toString());
              LOG.warn("List of input files:");

              if (compactionJobInfo.inputFiles().size() == 0) {
                LOG.error("Compaction input files list is empty?");
                return;
              }

              final StringBuilder sb = new StringBuilder();

              // kLevelL0FilesNum / kLevelMaxLevelSize. TODO: REMOVE
              sb.append("# ").append(compactionJobInfo.compactionReason()).append('\n');

              // Trim DB path, only keep the SST file name
              final int filenameBegin =
                  compactionJobInfo.inputFiles().get(0).lastIndexOf("/");

              for (String file : compactionJobInfo.inputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');  // TODO: Trim last delimiter

                // Create hardlink backups for the SST files that are going
                // to be deleted after this RDB compaction.
                LOG.warn(file);
                String saveLinkFileName =
                    saveCompactedFilePath + new File(file).getName();
                Path link = Paths.get(saveLinkFileName);
                Path srcFile = Paths.get(file);
                try {
                  Files.createLink(link, srcFile);
                } catch (IOException e) {
                  LOG.warn("Exception in creating hardlink");
                  e.printStackTrace();
                }
              }
              sb.append('\n');

              LOG.warn("List of output files:");
              for (String file : compactionJobInfo.outputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');
                LOG.warn(file + ",");
              }
              sb.append('\n');

              // Persist infile/outfile to file
              appendToCurrentCompactionLog(sb.toString());

              // Let us also build the graph
//              populateCompactionDAG(compactionJobInfo.inputFiles(),
//                  compactionJobInfo.outputFiles());

//              if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
//                printMutableGraph(null, null, compactionDAGFwd);
//              }
            }
          }
        };

    list.add(onCompactionCompletedListener);
    rocksOptions.setListeners(list);
  }

  /**
   * Append (then flush) to the current compaction log file path.
   */
  public void appendToCurrentCompactionLog(String content) {
    if (currentCompactionLogFilename == null) {
      LOG.error("Unable to append compaction log. "
          + "Current compaction log filename is not set. "
          + "Please check initialization.");
      return;
    }
    try (BufferedWriter bw = Files.newBufferedWriter(
        Paths.get(currentCompactionLogFilename),
        StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
      bw.write(content);
      bw.flush();
    } catch (IOException e) {
      throw new RuntimeException("Failed to append compaction log to " +
          currentCompactionLogFilename, e);
    }
  }

  public void setRocksDBForCompactionTracking(Options rocksOptions) {
    setRocksDBForCompactionTracking(rocksOptions, new ArrayList<>());
  }

  /**
   * This takes Options.
   */
  public void setRocksDBForCompactionTracking(
      Options rocksOptions, List<AbstractEventListener> list) {
    final AbstractEventListener onCompactionCompletedListener =
        new AbstractEventListener() {
          @Override
          @SuppressFBWarnings({
              "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
              "NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
          public void onCompactionCompleted(
              final RocksDB db,final CompactionJobInfo compactionJobInfo) {
            synchronized (db) {

              LOG.warn(compactionJobInfo.compactionReason().toString());
              LOG.warn("List of input files:");

              if (compactionJobInfo.inputFiles().size() == 0) {
                LOG.error("Compaction input files list is empty?");
                return;
              }

              final StringBuilder sb = new StringBuilder();

              // kLevelL0FilesNum / kLevelMaxLevelSize. TODO: Can remove
              sb.append("# ").append(compactionJobInfo.compactionReason()).append('\n');

              // Trim DB path, only keep the SST file name
              final int filenameBegin =
                  compactionJobInfo.inputFiles().get(0).lastIndexOf("/");

              for (String file : compactionJobInfo.inputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');  // TODO: nit: Trim last delimiter

                LOG.warn(file);
                String saveLinkFileName =
                    saveCompactedFilePath + new File(file).getName();
                Path link = Paths.get(saveLinkFileName);
                Path srcFile = Paths.get(file);
                try {
                  Files.createLink(link, srcFile);
                } catch (IOException e) {
                  LOG.warn("Exception in creating hardlink");
                  e.printStackTrace();
                }
              }
              sb.append('\n');

              LOG.warn("List of output files:");
              for (String file : compactionJobInfo.outputFiles()) {
                final String fn = file.substring(filenameBegin + 1);
                sb.append(fn).append('\t');
                LOG.warn(file);
              }
              sb.append('\n');

              // Persist infile/outfile to file
              appendToCurrentCompactionLog(sb.toString());

              // Let us also build the graph
//              populateCompactionDAG(compactionJobInfo.inputFiles(),
//                  compactionJobInfo.outputFiles());

//              if (debugEnabled(DEBUG_DAG_BUILD_UP)) {
//                printMutableGraph(null, null,
//                    compactionDAGFwd);
//              }
            }
          }
        };

    list.add(onCompactionCompletedListener);
    rocksOptions.setListeners(list);
  }

  public RocksDB getRocksDBInstanceWithCompactionTracking(String dbPath)
      throws RocksDBException {
    final Options opt = new Options().setCreateIfMissing(true)
//        .setWriteBufferSize(1L)  // Default is 64 MB. Unit in bytes.
//        .setMaxWriteBufferNumber(1)  // Default is 2
//        .setCompressionType(CompressionType.NO_COMPRESSION)
//        .setMaxBytesForLevelMultiplier(2)
        ;
    setRocksDBForCompactionTracking(opt);
    return RocksDB.open(opt, dbPath);
  }

  // Get a summary of a given SST file
  public long getSSTFileSummary(String filename)
      throws RocksDBException {
    Options option = new Options();
    SstFileReader reader = new SstFileReader(option);
    try {
      reader.open(saveCompactedFilePath + filename);
    } catch (RocksDBException e) {
      reader.open(rocksDbPath + "/"+ filename);
    }
    TableProperties properties = reader.getTableProperties();
    LOG.warn("getSSTFileSummary " + filename + ":: " +
        properties.getNumEntries());
    return properties.getNumEntries();
  }

  // Read the current Live manifest for a given RocksDB instance (Active or
  // Checkpoint). Returns the list of currently active SST FileNames.
  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public HashSet<String> readRocksDBLiveFiles(String dbPathArg) {
    RocksDB rocksDB = null;
    HashSet<String> liveFiles = new HashSet<>();
    //
    try (final Options options =
             new Options().setParanoidChecks(true)
                 .setCreateIfMissing(true)
                 .setCompressionType(CompressionType.NO_COMPRESSION)
                 .setForceConsistencyChecks(false)) {
      rocksDB = RocksDB.openReadOnly(options, dbPathArg);
      List<LiveFileMetaData> liveFileMetaDataList =
          rocksDB.getLiveFilesMetaData();
      LOG.warn("Live File Metadata for DB: " + dbPathArg);
      for (LiveFileMetaData m : liveFileMetaDataList) {
        LOG.warn("\tFile :" + m.fileName());
        LOG.warn("\tLevel :" + m.level());
        liveFiles.add(Paths.get(m.fileName()).getFileName().toString());
      }
    } catch (RocksDBException e) {
      e.printStackTrace();
    } finally {
      if (rocksDB != null) {
        rocksDB.close();
      }
    }
    return liveFiles;
  }

  /**
   * Process each line of compaction log text file input and populate the DAG.
   * TODO: Drop synchronized? and make this thread safe?
   */
  private synchronized void processCompactionLogLine(String line) {
    LOG.info("Processing line: {}", line);

    // Skip comments
    if (line.startsWith("#")) {
      LOG.info("Skipped comment.");
      return;
    }

    if (sstTokensRead == null) {
      // Store the tokens in the first line
      sstTokensRead = line.split("\t");
      LOG.info("Length of inputFiles = {}", sstTokensRead.length);
      if (sstTokensRead.length == 0) {
        // Sanity check. inputFiles should never be empty. outputFiles could.
        throw new RuntimeException(
            "Compaction inputFiles list is empty. File is corrupted?");
      }
    } else {
      final String[] outputFilesRead = line.split("\t");
      LOG.info("Length of outputFiles = {}", outputFilesRead.length);

      // Populate the compaction DAG
      populateCompactionDAG(sstTokensRead, outputFilesRead);

      // Reset inputFilesRead to null so
      sstTokensRead = null;
    }
  }

  private String[] sstTokensRead;

  /**
   * Helper to read compaction log to the internal DAG.
   * <p>
   * DO NOT use this function in another context without understanding what it
   * does, due to the state preserved between calls (in sstTokensRead).
   */
  private void readCompactionLogToDAG(String currCompactionLogPath) {
    LOG.debug("Loading compaction log: {}", currCompactionLogPath);
    try (Stream<String> stream =
        Files.lines(Paths.get(currCompactionLogPath), StandardCharsets.UTF_8)) {
      assert(sstTokensRead == null);
      stream.forEach(this::processCompactionLogLine);
      if (sstTokensRead != null) {
        // Sanity check. Temp variable must be null after parsing.
        // Otherwise it means the compaction log is corrupted.
        throw new RuntimeException("Missing output files line. Corrupted?");
      }
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }
  }

  /**
   * Read compaction log until all dest (and src) db checkpoint SST
   * nodes show up in the graph, or when it reaches the end of the log.
   */
  private boolean loadCompactionDAGBySSTSet(HashSet<String> sstSet) {

    // Make a copy of the sstSet
    HashSet<String> loadSet = new HashSet<>(sstSet);

    // Check if all the nodes in the provided SST set is already loaded in DAG
    for (String sstFile : sstSet) {
      if (compactionNodeTable.containsKey(sstFile)) {
        loadSet.remove(sstFile);
      }
    }

    if (loadSet.size() == 0) {
      // All expected nodes in the sstSet are already there,
      //  no need to read any compaction log
      return true;
    }

    // Otherwise, load compaction logs in order until all nodes are present in
    //  the DAG.
    try {
      try (Stream<Path> pathStream = Files.list(Paths.get(compactionLogDir))
          .filter(e -> e.toString().toLowerCase().endsWith(".log"))
          .sorted()) {
        for (Path logPath : pathStream.collect(Collectors.toList())) {

          // TODO: Potential optimization: stop reading as soon as all nodes are
          //  there. Currently it loads an entire file at a time.
          readCompactionLogToDAG(logPath.toString());

          for (Iterator<String> it = loadSet.iterator(); it.hasNext();) {
            String sstFile = it.next();
            if (compactionNodeTable.containsKey(sstFile)) {
              LOG.debug("Found SST node: {}", sstFile);
              it.remove();
            }
          }

          if (loadSet.size() == 0) {
            break;
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Error listing compaction log dir " +
          compactionLogDir, e);
    }

    // Just in case there are still nodes to be expected not loaded.
    if (loadSet.size() > 0) {
      LOG.warn("The following nodes are missing from the compaction log: {}. "
          + "Possibly because those a newly flushed SSTs that haven't gone "
          + "though any compaction yet", loadSet);
      return false;
    }

    return true;
  }

  /**
   * Given the src and destination Snapshots, it prints a Diff list.
   *
   * Expected input: src is a checkpoint taken AFTER dest checkpoint.
   *
   * @param src
   * @param dest
   * @throws RocksDBException
   */
  private synchronized List<String> printSnapdiffSSTFiles(
      Snapshot src, Snapshot dest) {

    LOG.warn("src db checkpoint: {}", src.dbPath);  // from
    HashSet<String> srcSnapFiles = readRocksDBLiveFiles(src.dbPath);
    LOG.warn("dest db checkpoint: {}", dest.dbPath);  //to
    HashSet<String> destSnapFiles = readRocksDBLiveFiles(dest.dbPath);
    System.out.println();

    // Read compaction log until all dest (and src) db checkpoint SST
    // nodes show up in the graph
//    loadCompactionDAGBySSTSet(destSnapFiles);
    loadCompactionDAGBySSTSet(srcSnapFiles);
    // TODO: Check what happens when we insert the same edge / node to the graph
    //  should not create new edge / node in this case

    HashSet<String> fwdDAGSameFiles = new HashSet<>();
    HashSet<String> fwdDAGDifferentFiles = new HashSet<>();

    LOG.warn("Doing forward diff between source and destination " +
        "Snapshots:" + src.dbPath + ", " + dest.dbPath);
    realPrintSnapdiffSSTFiles(src, dest, srcSnapFiles, destSnapFiles,
        compactionDAGFwd,
        fwdDAGSameFiles,
        fwdDAGDifferentFiles);

    LOG.warn("Overall Summary \n" +
            "Doing Overall diff between source and destination Snapshots:" +
        src.dbPath + ", " + dest.dbPath);
    System.out.print("Fwd DAG Same files: ");
    for (String file : fwdDAGSameFiles) {
      System.out.print(file + ", ");
    }
    LOG.warn("");

    List<String> res = new ArrayList<>();

    System.out.print("\nFwd DAG Different files: ");
    for (String file : fwdDAGDifferentFiles) {
      CompactionNode n = compactionNodeTable.get(file);
      System.out.print(file + ", ");
      res.add(file);
    }
    LOG.warn("");

    return res;
  }

  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public synchronized void realPrintSnapdiffSSTFiles(
      Snapshot src, Snapshot dest,
      HashSet<String> srcSnapFiles,
      HashSet<String> destSnapFiles,
      MutableGraph<CompactionNode> mutableGraph,
      HashSet<String> sameFiles, HashSet<String> differentFiles) {


    for (String fileName : srcSnapFiles) {
      if (destSnapFiles.contains(fileName)) {
        LOG.warn("Src Snapshot: " + src.dbPath + " and Dest " +
            "Snapshot: " + dest.dbPath + " contain the same file " + fileName);
        sameFiles.add(fileName);
        continue;
      }
      CompactionNode infileNode =
          compactionNodeTable.get(Paths.get(fileName).getFileName().toString());
      if (infileNode == null) {
        LOG.warn("Src Snapshot: " + src.dbPath + " File " + fileName +
            " was never compacted");
        differentFiles.add(fileName);
        continue;
      }
      System.out.print(" Expanding File: " + fileName + ":\n");
      Set<CompactionNode> nextLevel = new HashSet<>();
      nextLevel.add(infileNode);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
      int i = 1;
      while (currentLevel.size() != 0) {
        LOG.warn("DAG Level :" + i++);
        for (CompactionNode current : currentLevel) {
          LOG.warn("acknowledging file " + current.fileName);
          if (current.snapshotGeneration <= dest.snapshotGeneration) {
            LOG.warn("Reached dest generation count. SrcSnapshot : " +
                src.dbPath + " and Dest " + "Snapshot" + dest.dbPath +
                " Contain Different file " + current.fileName);
            differentFiles.add(current.fileName);
            continue;
          }
          Set<CompactionNode> successors = mutableGraph.successors(current);
          if (successors == null || successors.size() == 0) {
            LOG.warn("No further compaction for the file" +
                ".SrcSnapshot : " + src.dbPath + " and Dest " +
                "Snapshot" + dest.dbPath + " Contain Diffrent file " +
                current.fileName);
            differentFiles.add(current.fileName);
          } else {
            for (CompactionNode oneSucc : successors) {
              if (sameFiles.contains(oneSucc.fileName) ||
                  differentFiles.contains(oneSucc.fileName)) {
                LOG.warn("Skipping file :" + oneSucc.fileName);
                continue;
              }
              if (destSnapFiles.contains(oneSucc.fileName)) {
                LOG.warn("SrcSnapshot : " + src.dbPath + " and Dest " +
                    "Snapshot" + dest.dbPath + " Contain Same file " +
                    oneSucc.fileName);
                sameFiles.add(oneSucc.fileName);
                continue;
              } else {
                LOG.warn("SrcSnapshot : " + src.dbPath + " and Dest " +
                    "Snapshot" + dest.dbPath + " Contain Different file " +
                    oneSucc.fileName);
                nextLevel.add(oneSucc);
              }
            }
          }
        }
        currentLevel = new HashSet<>();
        currentLevel.addAll(nextLevel);
        nextLevel = new HashSet<>();
        LOG.warn("");
      }
    }
    LOG.warn("Summary: ");
    for (String file : sameFiles) {
      System.out.println("Same File: " + file);
    }

    for (String file : differentFiles) {
      System.out.println("Different File: " + file);
    }
  }

  @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
  class NodeComparator implements Comparator<CompactionNode>
  {
    public int compare(CompactionNode a, CompactionNode b)
    {
      return a.fileName.compareToIgnoreCase(b.fileName);
    }

    @Override
    public Comparator<CompactionNode> reversed() {
      return null;
    }
  }


  public void dumpCompactioNodeTable() {
    List<CompactionNode> nodeList =
        compactionNodeTable.values().stream().collect(Collectors.toList());
    Collections.sort(nodeList, new NodeComparator());
    for (CompactionNode n : nodeList ) {
      LOG.warn("File : " + n.fileName + " :: Total keys : "
          + n.totalNumberOfKeys);
      LOG.warn("File : " + n.fileName + " :: Cumulative keys : "  +
          n.cumulativeKeysReverseTraversal);
    }
  }

  @SuppressFBWarnings({"NP_NULL_ON_SOME_PATH_FROM_RETURN_VALUE"})
  public synchronized void printMutableGraphFromAGivenNode(
      String fileName, int level, MutableGraph<CompactionNode> mutableGraph) {
    CompactionNode infileNode =
        compactionNodeTable.get(Paths.get(fileName).getFileName().toString());
    if (infileNode == null) {
      return;
    }
    System.out.print("\nCompaction Level : " + level + " Expandin File:" +
        fileName + ":\n");
    Set<CompactionNode> nextLevel = new HashSet<>();
    nextLevel.add(infileNode);
    Set<CompactionNode> currentLevel = new HashSet<>();
    currentLevel.addAll(nextLevel);
    int i = 1;
    while (currentLevel.size() != 0) {
      LOG.warn("DAG Level :" + i++);
      for (CompactionNode current : currentLevel) {
        Set<CompactionNode> successors = mutableGraph.successors(current);
        for (CompactionNode oneSucc : successors) {
          System.out.print(oneSucc.fileName + " ");
          nextLevel.add(oneSucc);
        }
      }
      currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
      LOG.warn("");
    }
  }

  public synchronized void printMutableGraph(
      String srcSnapId, String destSnapId,
      MutableGraph<CompactionNode> mutableGraph) {
    LOG.warn("Printing the Graph");
    Set<CompactionNode> topLevelNodes = new HashSet<>();
    Set<CompactionNode> allNodes = new HashSet<>();
    for (CompactionNode n : mutableGraph.nodes()) {
      if (srcSnapId == null ||
          n.snapshotId.compareToIgnoreCase(srcSnapId) == 0) {
        topLevelNodes.add(n);
      }
    }
    Iterator iter = topLevelNodes.iterator();
    while (iter.hasNext()) {
      CompactionNode n = (CompactionNode) iter.next();
      Set<CompactionNode> succ = mutableGraph.successors(n);
      LOG.warn("Parent Node :" + n.fileName);
      if (succ.size() == 0) {
        LOG.warn("No Children Node ");
        allNodes.add(n);
        iter.remove();
        iter = topLevelNodes.iterator();
        continue;
      }
      for (CompactionNode oneSucc : succ) {
        LOG.warn("Children Node :" + oneSucc.fileName);
        if (srcSnapId == null||
            oneSucc.snapshotId.compareToIgnoreCase(destSnapId) == 0) {
          allNodes.add(oneSucc);
        } else {
          topLevelNodes.add(oneSucc);
        }
      }
      iter.remove();
      iter = topLevelNodes.iterator();
    }
    LOG.warn("src snap:" + srcSnapId);
    LOG.warn("dest snap:" + destSnapId);
    for (CompactionNode n : allNodes) {
      LOG.warn("Files are :" + n.fileName);
    }
  }

  public void createSnapshot(RocksDB rocksDB) throws InterruptedException {

    LOG.warn("Current time is::" + System.currentTimeMillis());
    long t1 = System.currentTimeMillis();

    cpPath = cpPath + lastSnapshotCounter;
    // Delete the checkpoint dir if it already exists
    File dir = new File(cpPath);
    if (dir.exists()) {
      deleteDirectory(dir);  // TODO: FOR EASE OF TESTING ONLY. DO NOT DELETE DIR WHEN MERGING
    }

    createCheckPoint(rocksDbPath, cpPath, rocksDB);
    allSnapshots[lastSnapshotCounter] = new Snapshot(cpPath,
    lastSnapshotPrefix, lastSnapshotCounter);

    setCompactionLogParentDir(".");
    setCompactionLogFilenameBySeqNum(rocksDB.getLatestSequenceNumber());

    long t2 = System.currentTimeMillis();
    LOG.warn("Current time is: " + t2);
    LOG.warn("millisecond difference is: " + (t2 - t1));
    Thread.sleep(100);
    ++lastSnapshotCounter;
    lastSnapshotPrefix = "sid_" + lastSnapshotCounter;
    LOG.warn("done.");
  }

  public void printAllSnapshots() {
    for (Snapshot snap : allSnapshots) {
      if (snap == null) {
        break;
      }
      LOG.warn("Snapshot id" + snap.snapshotID);
      LOG.warn("Snapshot path" + snap.dbPath);
      LOG.warn("Snapshot Generation" + snap.snapshotGeneration);
      LOG.warn("");
    }
  }

  public void diffAllSnapshots() {
    for (Snapshot snap : allSnapshots) {
      if (snap == null) {
        break;
      }
      // Returns a list of SST files to be fed into RocksDiff
      List<String> sstListForRocksDiff =
          printSnapdiffSSTFiles(allSnapshots[lastSnapshotCounter - 1], snap);
    }
  }

  public MutableGraph<CompactionNode> getCompactionFwdDAG() {
    return compactionDAGFwd;
  }

  public MutableGraph<CompactionNode> getCompactionReverseDAG() {
    return compactionDAGFwd;
  }

  /**
   * Populate the compaction DAG with input and outout SST files lists.
   */
  @SuppressFBWarnings({"AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION"})
  private void populateCompactionDAG(String[] inputFiles,
      String[] outputFiles) {

    LOG.info("Populating compaction DAG with lists of input and output files");

    for (String outfile : outputFiles) {
      CompactionNode outfileNode = compactionNodeTable.get(outfile);
      if (outfileNode == null) {
        long numKeys = 0;
        try {
          numKeys = getSSTFileSummary(outfile);
        } catch (Exception e) {
          LOG.warn(e.getMessage());
        }
        outfileNode = new CompactionNode(outfile,
            lastSnapshotPrefix,
            numKeys, currentCompactionGen);
        compactionDAGFwd.addNode(outfileNode);
        compactionDAGReverse.addNode(outfileNode);
        compactionNodeTable.put(outfile, outfileNode);
      }

      for (String infile : inputFiles) {
        CompactionNode infileNode = compactionNodeTable.get(infile);
        if (infileNode == null) {
          long numKeys = 0;
          try {
            numKeys = getSSTFileSummary(infile);
          } catch (Exception e) {
            LOG.warn(e.getMessage());
          }
          infileNode = new CompactionNode(infile,
              lastSnapshotPrefix, numKeys,
              UNKNOWN_COMPACTION_GEN);
          compactionDAGFwd.addNode(infileNode);
          compactionDAGReverse.addNode(infileNode);
          compactionNodeTable.put(infile, infileNode);
        }
        if (outfileNode.fileName.compareToIgnoreCase(
            infileNode.fileName) != 0) {
          compactionDAGFwd.putEdge(outfileNode, infileNode);
          compactionDAGReverse.putEdge(infileNode, outfileNode);
        }
      }
    }

  }

  public synchronized void traverseGraph(
      MutableGraph<CompactionNode> reverseMutableGraph,
      MutableGraph<CompactionNode> fwdMutableGraph) {

      List<CompactionNode> nodeList =
        compactionNodeTable.values().stream().collect(Collectors.toList());
    Collections.sort(nodeList, new NodeComparator());

    for (CompactionNode  infileNode : nodeList ) {
      // fist go through fwdGraph to find nodes that don't have succesors.
      // These nodes will be the top level nodes in reverse graph
      Set<CompactionNode> successors = fwdMutableGraph.successors(infileNode);
      if (successors == null || successors.size() == 0) {
        LOG.warn("traverseGraph : No successors. cumulative " +
            "keys : " + infileNode.cumulativeKeysReverseTraversal + "::total " +
            "keys ::" + infileNode.totalNumberOfKeys);
        infileNode.cumulativeKeysReverseTraversal =
            infileNode.totalNumberOfKeys;
      }
    }

    HashSet<CompactionNode> visited = new HashSet<>();
    for (CompactionNode  infileNode : nodeList ) {
      if (visited.contains(infileNode)) {
        continue;
      }
      visited.add(infileNode);
      System.out.print("traverseGraph: Visiting node " + infileNode.fileName +
          ":\n");
      Set<CompactionNode> nextLevel = new HashSet<>();
      nextLevel.add(infileNode);
      Set<CompactionNode> currentLevel = new HashSet<>();
      currentLevel.addAll(nextLevel);
      nextLevel = new HashSet<>();
      int i = 1;
      while (currentLevel.size() != 0) {
        LOG.warn("traverseGraph : DAG Level :" + i++);
        for (CompactionNode current : currentLevel) {
          LOG.warn("traverseGraph : expanding node " + current.fileName);
          Set<CompactionNode> successors =
              reverseMutableGraph.successors(current);
          if (successors == null || successors.size() == 0) {
            LOG.warn("traverseGraph : No successors. cumulative " +
                "keys : " + current.cumulativeKeysReverseTraversal);
          } else {
            for (CompactionNode oneSucc : successors) {
              LOG.warn("traverseGraph : Adding to the next level : " +
                  oneSucc.fileName);
              LOG.warn("traverseGraph : " + oneSucc.fileName + "cum" + " keys"
                  + oneSucc.cumulativeKeysReverseTraversal + "parent" + " " +
                  current.fileName + " total " + current.totalNumberOfKeys);
              oneSucc.cumulativeKeysReverseTraversal +=
                  current.cumulativeKeysReverseTraversal;
              nextLevel.add(oneSucc);
            }
          }
        }
        currentLevel = new HashSet<>();
        currentLevel.addAll(nextLevel);
        nextLevel = new HashSet<>();
        LOG.warn("");
      }
    }
  }

  public boolean debugEnabled(Integer level) {
    return DEBUG_LEVEL.contains(level);
  }
}
