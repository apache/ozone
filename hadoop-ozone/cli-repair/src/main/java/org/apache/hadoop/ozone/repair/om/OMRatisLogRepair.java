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

import jakarta.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
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
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.raftlog.segmented.LogSegment;
import org.apache.ratis.server.raftlog.segmented.LogSegmentPath;
import org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogOutputStream;
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
        "The command should be run for the same transaction on all 3 OMs only when all the OMs are crashing " +
        "while applying the same transaction. If only one OM is crashing and the " +
        "other OMs have executed the log successfully, then the DB should be manually copied " +
        "from one of the good OMs to the crashing OM instead." + 
        " OM should be stopped for this tool.",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class
)
public class OMRatisLogRepair extends RepairTool {

  @CommandLine.ArgGroup(multiplicity = "1")
  private ExclusiveArguments exclusiveArguments;

  @CommandLine.Option(names = {"-b", "--backup"},
      required = true,
      description = "Directory to put the backup of the original repaired segment file before the repair.")
  private File backupDir;

  @CommandLine.Option(names = {"--index"},
      required = true,
      description = "Index of the failing transaction that should be removed")
  private long index;

  private SegmentedRaftLogOutputStream outputStream = null;

  @Nonnull
  @Override
  protected Component serviceToBeOffline() {
    return Component.OM;
  }

  @Override
  public void execute() throws Exception {

    if (exclusiveArguments.logDir != null) {
      exclusiveArguments.segmentFile = findSegmentFileContainingIndex();
    }

    if (exclusiveArguments.segmentFile.getParentFile().toPath().equals(backupDir.toPath())) {
      throw new IOException("Backup directory cannot be same as segment file's parent directory.");
    }

    LogSegmentPath pi = LogSegmentPath.matchLogSegment(this.exclusiveArguments.segmentFile.toPath());
    if (pi == null) {
      throw new IOException("Invalid Segment File");
    }

    if (!exclusiveArguments.segmentFile.exists()) {
      throw new IOException("Error: Source segment file \"" + exclusiveArguments.segmentFile + "\" does not exist.");
    }
    if (!backupDir.exists()) {
      info("BackupDir \"" + backupDir + "\" does not exist. Creating the directory path.");
      if (!isDryRun()) {
        Files.createDirectories(backupDir.toPath());
      }
    }

    File backupPath = new File(backupDir, exclusiveArguments.segmentFile.getName());
    if (backupPath.exists()) {
      throw new IOException("Error: Backup file for segment file  \"" + exclusiveArguments.segmentFile +
          "\" already exists. Either delete the old backup or provide a different directory to take the backup.");
    }
    info("Taking back up of Raft Log file: " + this.exclusiveArguments.segmentFile.getAbsolutePath() +
        " to location: " + backupPath);
    if (!isDryRun()) {
      Files.copy(exclusiveArguments.segmentFile.toPath(), backupPath.toPath());
    }
    info("File backed-up successfully!");

    File outputFile = null;
    if (!isDryRun()) {
      outputFile = File.createTempFile("srt-output", null, backupDir);
      outputFile.deleteOnExit();
    }
    info("Created temporary output file: " + (outputFile == null ? "<None>" : outputFile.toPath()));

    info("Processing Raft Log file: " + this.exclusiveArguments.segmentFile.getAbsolutePath() + " size:" +
        this.exclusiveArguments.segmentFile.length());

    if (!isDryRun()) {
      outputStream = new SegmentedRaftLogOutputStream(outputFile, false,
          1024, 1024, ByteBuffer.allocateDirect(SizeInBytes.valueOf("8MB").getSizeInt()));
    }

    int entryCount = LogSegment.readSegmentFile(exclusiveArguments.segmentFile, pi.getStartEnd(),
        SizeInBytes.valueOf("32MB"), RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION, null, this::processLogEntry);
    if (!isDryRun()) {
      outputStream.flush();
      outputStream.close();
      Files.move(outputFile.toPath(), exclusiveArguments.segmentFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
    info("Finished processing all the entries (" + entryCount + " logs) from the segment file.");
    info("Moved temporary output file to correct raft log location : " + exclusiveArguments.segmentFile.toPath());

  }

  private void processLogEntry(RaftProtos.LogEntryProto proto) {
    try {
      RaftProtos.LogEntryProto newLogEntry = proto.getIndex() != index ? proto : getOmEchoLogEntry(proto);
      if (!isDryRun()) {
        outputStream.write(newLogEntry);
        outputStream.flush();
      }
    } catch (IOException ex) {
      throw new RuntimeException("Error while processing logEntry: (" + proto.getIndex() + "). Exception: " + ex);
    }
  }

  private RaftProtos.LogEntryProto getOmEchoLogEntry(RaftProtos.LogEntryProto proto) throws IOException {
    OzoneManagerProtocolProtos.OMRequest.Builder newRequest = OzoneManagerProtocolProtos.OMRequest.newBuilder()
        .setCmdType(EchoRPC)
        .setClientId("skip-ratis-transaction-repair-tool")
        .setEchoRPCRequest(OzoneManagerProtocolProtos.EchoRPCRequest.newBuilder().build());
    RaftProtos.StateMachineLogEntryProto.Builder entry =  proto.getStateMachineLogEntry().toBuilder()
        .setLogData(OMRatisHelper.convertRequestToByteString(newRequest.build()));
    OzoneManagerProtocolProtos.OMRequest oldRequest = OMRatisHelper
        .convertByteStringToOMRequest(proto.getStateMachineLogEntry().getLogData());
    info("Replacing {" + oldRequest.toString().replace("\n", "  ")
        + "} with EchoRPC command at index " + proto.getIndex());
    return proto.toBuilder()
        .setStateMachineLogEntry(entry)
        .build();
  }

  private File findSegmentFileContainingIndex() {
    if (!exclusiveArguments.logDir.exists() || !exclusiveArguments.logDir.isDirectory()) {
      throw new IllegalArgumentException("Invalid log directory: " + exclusiveArguments.logDir);
    }

    // Pattern to match Ratis log files: log_<start>-<end> or log_inprogress_<start>
    Pattern pattern = Pattern.compile("log(?:_inprogress)?_(\\d+)(?:-(\\d+))?");

    File[] segmentFiles = exclusiveArguments.logDir.listFiles();
    if (segmentFiles == null) {
      throw new IllegalArgumentException("Invalid log directory: " + exclusiveArguments.logDir +
          ". No segment files present.");
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
        + ") for log directory: \"" + exclusiveArguments.logDir + "\". None of the segment files have the index.");
  }

  private static final class ExclusiveArguments {
    @CommandLine.Option(names = {"-s", "--segment-path"},
        description = "Path of the input segment file")
    private File segmentFile;

    @CommandLine.Option(names = {"-d", "--ratis-log-dir"},
        description = "Path of the ratis log directory")
    private File logDir;
  }
}
