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
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
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
    name = "skip-ratis-transaction",
    aliases = "srt",
    description = "CLI to omit a raft log in a ratis segment file. The raft log at the index specified " +
        "is replaced with an EchoOM command (which is a dummy command). It is an offline command " +
        "i.e., doesn't require OM to be running. " +
        "The command should be run for the same transaction on all 3 OMs only when they all OMs are crashing " +
        "while applying the same transaction. If there is only one OM that is crashing, " +
        "then the DB should manually copied from one of the good OMs to the crashing OM instead.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class OMRatisLogRepair extends RepairTool {

  @CommandLine.Option(names = {"-s", "--segment-path", "--segmentPath"},
      required = true,
      description = "Path of the input segment file")
  private File segmentFile;
  @CommandLine.Option(names = {"-b", "--backup"},
      required = true,
      description = "Path to put the backup of the original repaired segment file")
  private File backupPath;

  @CommandLine.Option(names = {"--index"},
      required = true,
      description = "Index of the failing transaction that should be removed")
  private long index;

  @Override
  public void execute() throws Exception {

    info("Taking back up of Raft Log file: " + this.segmentFile.getAbsolutePath() + " to location: " + backupPath);
    if (!segmentFile.exists()) {
      error("Error: Source segment file \"" + segmentFile + "\" does not exist.");
      return;
    }
    if (backupPath.exists()) {
      error("Error: Backup file for segment file  \"" + backupPath + "\" already exists.");
      return;
    }
    try {
      if (!isDryRun()) {
        Files.copy(segmentFile.toPath(), backupPath.toPath());
      }
      System.out.println("File backed-up successfully!");
    } catch (IOException ex) {
      throw new RuntimeException("Error: Failed to take backup of the file. " +
          "It might be due to file locks or permission issues.");
    }

    LogSegmentPath pi = LogSegmentPath.matchLogSegment(this.segmentFile.toPath());
    if (pi == null) {
      error("Deleting backup file as provided segmentFile is invalid.");
      backupPath.delete();
      throw new RuntimeException("Invalid Segment File");
    }
    String tempOutput = segmentFile.getAbsolutePath() + ".skr.output";
    File outputFile = createOutputFile(tempOutput);

    info("Processing Raft Log file: " + this.segmentFile.getAbsolutePath() + " size:" + this.segmentFile.length());
    SegmentedRaftLogOutputStream outputStream = null;
    SegmentedRaftLogInputStream logInputStream = null;

    try {
      logInputStream = getInputStream(pi);
      if (!isDryRun()) {
        outputStream = new SegmentedRaftLogOutputStream(outputFile, false,
            1024, 1024, ByteBuffer.allocateDirect(10 * 1024));
      }

      RaftProtos.LogEntryProto next;
      for (RaftProtos.LogEntryProto prev = null; (next = logInputStream.nextEntry()) != null; prev = next) {
        if (prev != null) {
          Preconditions.assertTrue(next.getIndex() == prev.getIndex() + 1L,
              "gap between entry %s and entry %s", prev, next);
        }

        if (next.getIndex() != index && !isDryRun()) {
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

          info("Replacing {" + oldRequest + "} with EchoRPC command at index "
              + next.getIndex());

          if (!isDryRun()) {
            outputStream.write(newLogEntry);
          }
        }
      }

      if (!isDryRun()) {
        outputStream.flush();
        outputStream.close();
        Files.move(outputFile.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
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
      if (isDryRun()) {
        outputFile.delete();
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
