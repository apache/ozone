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

import static org.apache.hadoop.ozone.OzoneConsts.FLUSH_LOG_TABLE;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.hadoop.hdds.cli.AbstractSubcommand;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksDB;
import org.apache.hadoop.hdds.utils.db.managed.ManagedRocksIterator;
import org.apache.hadoop.ozone.debug.RocksDBUtils;
import org.apache.ozone.compaction.log.FlushFileInfo;
import org.apache.ozone.compaction.log.FlushLogEntry;
import org.apache.ozone.graph.PrintableGraph;
import org.apache.ozone.rocksdiff.FlushLinkedList;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import picocli.CommandLine;

/**
 * Handler to generate image for current flush log timeline.
 * ozone debug om generate-flush-log
 */
@CommandLine.Command(
    name = "generate-flush-log",
    aliases = "gfl",
    description = "Create an image of the current flush log timeline. " +
        "This command is an offline command. i.e., it can run on any instance of om.db " +
        "and does not require OM to be up.")
public class FlushLogPrinter extends AbstractSubcommand implements Callable<Void> {

  @CommandLine.ParentCommand
  private OMDebug parent;

  @CommandLine.Option(names = {"-o", "--output-file"},
      required = true,
      description = "Path to location at which image will be downloaded. " +
          "Should include the image file name with \".png\" extension.")
  private String imageLocation;

  @CommandLine.Option(names = {"-t", "--type"},
      defaultValue = "FILE_NAME",
      description = "Graph type: FILE_NAME, SEQUENCE_NUMBER, FLUSH_TIME, DETAILED. Default: ${DEFAULT-VALUE}")
  private PrintableGraph.GraphType graphType;

  @Override
  public Void call() throws Exception {
    try {
      final List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();
      List<ColumnFamilyDescriptor> cfDescList = RocksDBUtils.getColumnFamilyDescriptors(parent.getDbPath());
      ManagedRocksDB activeRocksDB = ManagedRocksDB.openReadOnly(parent.getDbPath(), cfDescList, cfHandleList);
      ColumnFamilyHandle flushLogTableCFHandle =
          RocksDBUtils.getColumnFamilyHandle(FLUSH_LOG_TABLE, cfHandleList);

      FlushLinkedList flushLinkedList = new FlushLinkedList();
      loadFlushLogFromDB(activeRocksDB, flushLogTableCFHandle, flushLinkedList);

      if (flushLinkedList.isEmpty()) {
        out().println("Flush log is empty. No graph generated.");
        return null;
      }

      PrintableGraph graph = new PrintableGraph(flushLinkedList, graphType);
      graph.generateImage(imageLocation);
      out().println("Graph was generated at '" + imageLocation + "'.");
      out().println("Total flush entries: " + flushLinkedList.size());
    } catch (RocksDBException ex) {
      err().println("Failed to open RocksDB: " + ex);
      throw ex;
    }
    return null;
  }

  /**
   * Read FlushLogTable and populate the FlushLinkedList.
   */
  private void loadFlushLogFromDB(ManagedRocksDB activeRocksDB,
      ColumnFamilyHandle flushLogTableCFHandle, FlushLinkedList flushLinkedList) {
    try (ManagedRocksIterator managedRocksIterator = new ManagedRocksIterator(
        activeRocksDB.get().newIterator(flushLogTableCFHandle))) {
      managedRocksIterator.get().seekToFirst();
      while (managedRocksIterator.get().isValid()) {
        byte[] value = managedRocksIterator.get().value();
        FlushLogEntry flushLogEntry =
            FlushLogEntry.getFromProtobuf(HddsProtos.FlushLogEntryProto.parseFrom(value));
        FlushFileInfo fileInfo = flushLogEntry.getFileInfo();

        flushLinkedList.addFlush(
            fileInfo.getFileName(),
            flushLogEntry.getDbSequenceNumber(),
            flushLogEntry.getFlushTime(),
            fileInfo.getStartKey(),
            fileInfo.getEndKey(),
            fileInfo.getColumnFamily());

        managedRocksIterator.get().next();
      }
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Failed to parse flush log entry", e);
    }
  }
}
