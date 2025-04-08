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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
        "while applying the same transaction. If there is only one OM that is crashing and " +
        "other OMs have executed the log successfully, then the DB should manually copied from one of the good OMs " +
        "to the crashing OM instead.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class OMRatisLogRepair extends RepairTool {

  @CommandLine.Option(names = {"-s", "--segment-path", "--segmentPath"},
      description = "Path of the input segment file")
  private File segmentFile;

  @CommandLine.Option(names = {"-d", "--ratis-log-dir", "--ratisLogDir"},
      description = "Path of the ratis log directory")
  private File logDir;

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

    if (segmentFile == null && logDir == null) {
      throw new IllegalArgumentException("Path to either a segment-file or ratis-log-dir must be provided.");
    }
    if (segmentFile == null) {
      segmentFile = findSegmentFileContainingIndex();
    }

    if (segmentFile.toPath().equals(backupPath)) {
      throw new IOException("Backup path cannot be same as segment file path.");
    }

    LogSegmentPath pi = LogSegmentPath.matchLogSegment(this.segmentFile.toPath());
    if (pi == null) {
      throw new IOException("Invalid Segment File");
    }

    if (!segmentFile.exists()) {
      throw new IOException("Error: Source segment file \"" + segmentFile + "\" does not exist.");
    }
    if (backupPath.exists()) {
      throw new IOException("Error: Backup file for segment file  \"" + backupPath + "\" already exists.");
    }
    try {
      info("Taking back up of Raft Log file: " + this.segmentFile.getAbsolutePath() + " to location: " + backupPath);
      if (!isDryRun()) {
        Files.copy(segmentFile.toPath(), backupPath.toPath());
      }
      info("File backed-up successfully!");
    } catch (IOException ex) {
      throw new IOException("Error: Failed to take backup of the file. Exception: " + ex, ex);
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
            1024, 1024, ByteBuffer.allocateDirect(SizeInBytes.valueOf("8MB").getSizeInt()));
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
          outputStream.flush();
          info("Copied raft log for index (" + next.getIndex() + ").");
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

          if (!isDryRun()) {
            outputStream.write(newLogEntry);
            outputStream.flush();
          }
          info("Replaced {" + oldRequest + "} with EchoRPC command at index "
              + next.getIndex());
        }
      }

      if (!isDryRun()) {
        outputStream.flush();
        outputStream.close();
        Files.move(outputFile.toPath(), segmentFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        info("Moved temporary output file to correct raft log location : " + segmentFile.toPath());
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
        error("Warning: Temporary output file already exists - " + temp.getAbsolutePath() +
            ". Trying to delete it and create a new one.");
        boolean success = temp.delete();
        if (!success) {
          throw new IOException("Unable to delete old temporary file.");
        }
      }
      temp.createNewFile();
      info("Temporary output file created successfully: " + temp.getAbsolutePath());
    } catch (IOException e) {
      throw new IOException("Error: Failed to create temporary output file - " + temp.getAbsolutePath(), e);
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

  private File findSegmentFileContainingIndex() {
    if (!logDir.exists() || !logDir.isDirectory()) {
      throw new IllegalArgumentException("Invalid log directory: " + logDir);
    }

    // Pattern to match Ratis log files: log_<start>-<end> or log_inprogress_<start>
    Pattern pattern = Pattern.compile("log(?:_inprogress)?_(\\d+)(?:-(\\d+))?");

    File[] segmentFiles = logDir.listFiles();
    if (segmentFiles == null) {
      throw new IllegalArgumentException("Invalid log directory: " + logDir + ". No segment files present.");
    }

    for (File file : segmentFiles) {
      Matcher matcher = pattern.matcher(file.getName());
      if (matcher.matches()) {
        long start = Long.parseLong(matcher.group(1));
        String endStr = matcher.group(2);

        // If it's an in-progress file, assume it contains all entries from start onwards
        if (endStr == null) {
          if (index >= start) {
            info("Segment file \"" + file + "\" contains the index (" + index + ").");
            return file;
          }
        } else {
          long end = Long.parseLong(endStr);
          if (index >= start && index <= end) {
            info("Segment file \"" + file + "\" contains the index (" + index + ").");
            return file;
          }
        }
      }
    }

    throw new IllegalArgumentException("Invalid index (" + index
        + ") for log directory: \"" + logDir + "\". None of the segment files have the index.");
  }
}
