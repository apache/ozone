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

package org.apache.hadoop.ozone.repair.om;

import static org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type.EchoRPC;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.ozone.om.helpers.OMRatisHelper;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.repair.RepairTool;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.metrics.SegmentedRaftLogMetrics;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.LogSegmentStartEnd;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.SizeInBytes;
import picocli.CommandLine;


/**
 * Tool to omit a raft log in a ratis segment file.
 */
@CommandLine.Command(
    name = "ratislogrepair",
    description = "CLI to omit a raft log in a ratis segment file. " +
        "Requires admin privileges.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class OMRatisLogRepair extends RepairTool {

  @CommandLine.Option(names = {"-s", "--segment-path", "--segmentPath"},
      required = true,
      description = "Path of the input segment file")
  private File segmentFile;
  @CommandLine.Option(names = {"-o", "--output-path", "--outputPath"},
      required = true,
      description = "Path of the repaired segment file")
  private String outputPath;

  @CommandLine.Option(names = {"--index"},
      required = true,
      description = "Path of the segment file")
  private long index;

  @Override
  public void execute() throws Exception {

    LogSegmentPath pi = LogSegmentPath.matchLogSegment(this.segmentFile.toPath());
    if (pi == null) {
      error("Invalid segment file");
      throw new RuntimeException("Invalid Segment File");
    }
    info("Processing Raft Log file: " + this.segmentFile.getAbsolutePath() + " size:" + this.segmentFile.length());
    SegmentedRaftLogOutputStream outputStream = null;
    SegmentedRaftLogInputStream logInputStream = null;

    try {
      logInputStream = getInputStream(pi);
      File outputFile = createOutputFile(outputPath);
      outputStream = new SegmentedRaftLogOutputStream(outputFile, false,
          1024, 1024, ByteBuffer.allocateDirect(10 * 1024));

      RaftProtos.LogEntryProto next;
      for (RaftProtos.LogEntryProto prev = null; (next = logInputStream.nextEntry()) != null; prev = next) {
        if (prev != null) {
          Preconditions.assertTrue(next.getIndex() == prev.getIndex() + 1L,
              "gap between entry %s and entry %s", prev, next);
        }

        if (next.getIndex() != index) {
          // all other logs will be written as it is
          outputStream.write(next);
        } else {
          // replace the transaction with a dummy OmEcho operation
          OzoneManagerProtocolProtos.OMRequest oldRequest = OMRatisHelper
              .convertByteStringToOMRequest(next.getStateMachineLogEntry().getLogData());
          OzoneManagerProtocolProtos.OMRequest.Builder newRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
              .setCmdType(EchoRPC)
              .setClientId(oldRequest.getClientId())
              .setEchoRPCRequest(OzoneManagerProtocolProtos.EchoRPCRequest.newBuilder().build());

          if (oldRequest.hasUserInfo()) {
            newRequest.setUserInfo(oldRequest.getUserInfo());
          }
          if (oldRequest.hasTraceID()) {
            newRequest.setTraceID(oldRequest.getTraceID());
          }
          if (oldRequest.hasLayoutVersion()) {
            newRequest.setLayoutVersion(oldRequest.getLayoutVersion());
          }
          if (oldRequest.hasVersion()) {
            newRequest.setVersion(oldRequest.getVersion());
          }

          RaftProtos.StateMachineLogEntryProto oldEntry = next.getStateMachineLogEntry();
          RaftProtos.StateMachineLogEntryProto.Builder newEntry =
              RaftProtos.StateMachineLogEntryProto.newBuilder()
                  .setCallId(oldEntry.getCallId())
                  .setClientId(oldEntry.getClientId())
                  .setType(oldEntry.getType())
                  .setLogData(OMRatisHelper.convertRequestToByteString(newRequest.build()));
          if (oldEntry.hasStateMachineEntry()) {
            newEntry.setStateMachineEntry(oldEntry.getStateMachineEntry());
          }

          RaftProtos.LogEntryProto newLogEntry = RaftProtos.LogEntryProto.newBuilder()
              .setTerm(next.getTerm())
              .setIndex(next.getIndex())
              .setStateMachineLogEntry(newEntry)
              .build();
          outputStream.write(newLogEntry);
        }
      }
    } catch (Exception ex) {
      error("Exception: " + ex);
    } finally {
      if (logInputStream != null) {
        logInputStream.close();
      }
      if (outputStream != null) {
        outputStream.flush();
        outputStream.close();
      }
    }
  }

  private File createOutputFile(String name) throws IOException {
    File temp = new File(name);
    try {
      if (temp.exists()) {
        throw new IOException("Error: File already exists - " + temp.getAbsolutePath());
      } else {
        if (temp.createNewFile()) {
          System.out.println("File created successfully: " + temp.getAbsolutePath());
        } else {
          throw new IOException("Error: Failed to create file - " + temp.getAbsolutePath());
        }
      }
    } catch (IOException e) {
      error("Exception while trying to open output file: " + e.getMessage());
      throw e;
    }
    return temp;
  }

  private SegmentedRaftLogInputStream getInputStream(LogSegmentPath pi) {
    try {
      Class<?> logInputStreamClass =
          Class.forName("org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogInputStream");
      Constructor<?> constructor = logInputStreamClass.getDeclaredConstructor(File.class, LogSegmentStartEnd.class,
          SizeInBytes.class, SegmentedRaftLogMetrics.class);
      constructor.setAccessible(true);
      SegmentedRaftLogInputStream inputStream =
          (SegmentedRaftLogInputStream) constructor.newInstance(segmentFile, pi.getStartEnd(),
              SizeInBytes.valueOf("32MB"), null);
      if (inputStream == null) {
        throw new RuntimeException("logInputStream is null. Constructor might have failed.");
      }
      return inputStream;

    } catch (ClassNotFoundException | NoSuchMethodException | SecurityException |
             InvocationTargetException | InstantiationException | IllegalAccessException ex) {
      error("Exception while trying to get input stream for segment file : " + ex);
      throw new RuntimeException(ex);
    }
  }
}
