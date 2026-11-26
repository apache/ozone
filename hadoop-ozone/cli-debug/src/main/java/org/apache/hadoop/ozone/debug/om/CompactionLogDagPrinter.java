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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.ozone.compaction.log.CompactionLogEntry;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ozone.rocksdiff.CompactionDag;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Handler to generate image for current compaction DAG.
 * ozone debug om generate-compaction-dag.
 */
@CommandLine.Command(
    name = "generate-compaction-dag",
    aliases = "gcd",
    description = "Create an image of the current compaction log DAG. " +
        "This command is an offline command. i.e., it can run on any instance of om.db " +
        "and does not require OM to be up.")
public class CompactionLogDagPrinter extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMDebug parent;

  @CommandLine.Option(names = {"-o", "--output-file"},
      required = true,
      description = "Path to location at which image will be downloaded. " +
          "Should include the image file name with \".png\" extension.")
  private String imageLocation;

  @Override
  public Void call() throws Exception {
    try {
      final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
      List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
      ManagedRocksDB activeRocksDB = ManagedRocksDB.openReadOnly(parent.getDbPath(), cfDescList, cfHandleList);
      ColumnFamilyHandle compactionLogTableCFHandle =
          RocksDBUtils.getColumnFamilyHandle(COMPACTION_LOG_TABLE, cfHandleList);

      CompactionDag compactionDag = new CompactionDag();
      loadCompactionDagFromDB(activeRocksDB, compactionLogTableCFHandle, compactionDag);

      pngPrintMutableGraph(compactionDag, imageLocation);
      out().println("Graph was generated at '" + imageLocation + "'.");
    } catch (RocksDBException ex) {
      err().println("Failed to open RocksDB: " + ex);
      throw ex;
    }
    return null;
  }

  /**
   * Read a compactionLofTable and create entries in the dags.
   */
  private void loadCompactionDagFromDB(ManagedRocksDB activeRocksDB,
      ColumnFamilyHandle compactionLogTableCFHandle, CompactionDag compactionDag) {
    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(compactionLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();
      while (managedRocksIterator.get().isValid()) {
        byte[] value = managedRocksIterator.get().value();
        CompactionLogEntry compactionLogEntry =
            CompactionLogEntry.getFromProtobuf(HddsProtos.CompactionLogEntryProto.parseFrom(value));
        compactionDag.populateCompactionDAG(compactionLogEntry.getInputFileInfoList(),
            compactionLogEntry.getOutputFileInfoList(), compactionLogEntry.getDbSequenceNumber());
        managedRocksIterator.get().next();
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public void pngPrintMutableGraph(CompactionDag helper, String filePath)
      throws IOException {
    Objects.requireNonNull(filePath, "Image file path is required.");
    PrintableGraph graph;
    graph = new PrintableGraph(helper.getBackwardCompactionDAG(), PrintableGraph.GraphType.FILE_NAME);
    graph.generateImage(filePath);
  }
}
