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

package org.apache.hadoop.ozone.debug.om;

import static org.apache.hadoop.ozone.OzoneConsts.COMPACTION_LOG_TABLE;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.ozone.compaction.log.CompactionFileInfo;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.compaction.log.PopulateCompactionTable;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ozone.rocksdiff.CompactionNode;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Handler to generate image for current compaction DAG in the OM leader node.
 * ozone debug om print-log-dag.
 */
@CommandLine.Command(
    name = "print-log-dag",
    aliases = "pld",
    description = "Create an image of the current compaction log DAG. " +
        "This command is an offline command. i.e., it can run on any instance of om.db " +
        "and does not require OM to be up.")
public class CompactionLogDagPrinter extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.Option(names = {"-o", "--output-file"},
      required = true,
      description = "Path to location at which image will be downloaded. " +
          "Should include the image file name with \".png\" extension.")
  private String imageLocation;

  @CommandLine.Option(names = {"--db"},
      required = true,
      scope = CommandLine.ScopeType.INHERIT,
      description = "Path to OM RocksDB")
  private String dbPath;

  @CommandLine.Option(names = {"--compaction-log"},
      scope = CommandLine.ScopeType.INHERIT,
      description = "Path to compaction-log directory.")
  private String compactionLogDir;

  // TODO: Change graphType to enum.
  @CommandLine.Option(names = {"-t", "--graph-type"},
      description = "Type of node name to use in the graph image. (optional)\n Accepted values are: \n" +
          "  FILE_NAME (default) : to use file name as node name in DAG,\n" +
          "  KEY_SIZE: to show the no. of keys in the file along with file name in the DAG node name,\n" +
          "  CUMULATIVE_SIZE: to show the cumulative size along with file name in the DAG node name.",
      defaultValue = "FILE_NAME")
  private String graphType;

  @Override
  public Void call() throws Exception {
    try {
      CreateCompactionDag createCompactionDag = new CreateCompactionDag(dbPath, compactionLogDir);
      createCompactionDag.pngPrintMutableGraph(imageLocation, PrintableGraph.GraphType.valueOf(graphType));
      out().println("Graph was generated at '" + imageLocation + "'.");
    } catch (RocksDBException ex) {
      err().println("Failed to open RocksDB: " + ex);
      throw new IOException(ex);
    }
    return null;
  }

  class CreateCompactionDag {
    // Hash table to track CompactionNode for a given SST File.
    private final ConcurrentHashMap<String, CompactionNode> compactionNodeMap =
        new ConcurrentHashMap<>();
    private final MutableGraph<CompactionNode> backwardCompactionDAG =
        GraphBuilder.directed().build();

    private ColumnFamilyHandle compactionLogTableCFHandle;
    private ManagedRocksDB activeRocksDB;
    private PopulateCompactionTable populateCompactionTable;

    CreateCompactionDag(String dbPath, String compactDir) throws RocksDBException {
      final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
      List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(dbPath);
      activeRocksDB = ManagedRocksDB.openReadOnly(dbPath, cfDescList, cfHandleList);
      compactionLogTableCFHandle = RocksDBUtils.getColumnFamilyHandle(COMPACTION_LOG_TABLE, cfHandleList);
      populateCompactionTable = new PopulateCompactionTable(compactDir, activeRocksDB, compactionLogTableCFHandle);
    }

    public void pngPrintMutableGraph(String filePath, PrintableGraph.GraphType gType)
        throws IOException, RocksDBException {
      Objects.requireNonNull(filePath, "Image file path is required.");
      Objects.requireNonNull(gType, "Graph type is required.");

      loadAllCompactionLogs();

      PrintableGraph graph;
      graph = new PrintableGraph(backwardCompactionDAG, gType);
      graph.generateImage(filePath);
    }

    public void loadAllCompactionLogs() {
      populateCompactionTable.preconditionChecksForLoadAllCompactionLogs(false);
      if (compactionLogDir != null) {
        populateCompactionTable.addEntriesFromLogFilesToDagAndCompactionLogTable();
      }
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

    /**
     * Populate the compaction DAG with input and output SST files lists.
     * @param inputFiles List of compaction input files.
     * @param outputFiles List of compaction output files.
     * @param seqNum DB transaction sequence number.
     */
    private void populateCompactionDAG(List<CompactionFileInfo> inputFiles,
                                       List<CompactionFileInfo> outputFiles,
                                       long seqNum) {

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
            backwardCompactionDAG.putEdge(infileNode, outfileNode);
          }
        }
      }
    }

    private CompactionNode addNodeToDAG(String file, long seqNum, String startKey,
                                        String endKey, String columnFamily) {
      long numKeys = 0L;
      try {
        numKeys = populateCompactionTable.getSSTFileSummary(file, dbPath);
      } catch (RocksDBException e) {
        err().println("Warning: Can't get num of keys in SST '" + file + "'. Reason: " + e.getMessage());
      } catch (FileNotFoundException e) {
        out().println("Warning: Can't find SST : " + file);
      }

      CompactionNode fileNode = new CompactionNode(file, numKeys,
          seqNum, startKey, endKey, columnFamily);
      backwardCompactionDAG.addNode(fileNode);
      return fileNode;
    }
  }

}
