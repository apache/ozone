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

package org.apache.hadoop.ozone.jobworker.commands;

import com.google.common.io.BaseEncoding;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerCommandRecord;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.JobworkerReceiptManifest;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.apache.hadoop.ozone.jobworker.JobworkerConfiguration;
import org.apache.ratis.util.AtomicFileOutputStream;

/**
 * Worker-owned receipts and results, separate from disposable task workspace.
 * Records are never discarded on heartbeat success. Acknowledged receipts prevent delayed duplicate execution.
 */
public final class JobworkerCommandJournal implements Closeable {
  private final Path directory;
  private final FileChannel lockChannel;
  private final FileLock lock;
  private final UUID workerId;
  private final int maxRecords;
  private final Map<String, JobworkerCommandRecord> records = new HashMap<>();
  private boolean manifestDirty;
  private boolean closed;

  public JobworkerCommandJournal(ConfigurationSource conf) throws IOException {
    JobworkerConfiguration config = conf.getObject(JobworkerConfiguration.class);
    String configured = config.getMetadataDir();
    if (configured.isEmpty()) {
      String firstVolume = config.getStorageDirs().split(",")[0].trim();
      if (firstVolume.isEmpty()) {
        throw new IOException("Jobworker requires a persistent metadata directory or a storage volume directory");
      }
      directory = Paths.get(firstVolume, "jobworker-state").toAbsolutePath();
    } else {
      directory = Paths.get(configured).toAbsolutePath();
    }
    maxRecords = config.getJournalMaxRecords();
    if (maxRecords <= 0) {
      throw new IOException("Jobworker journal record limit must be positive");
    }
    Files.createDirectories(directory);
    syncDirectory(directory.getParent());
    lockChannel = FileChannel.open(directory.resolve("journal.lock"),
        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
    FileLock acquired = null;
    try {
      acquired = lockChannel.tryLock();
      if (acquired == null) {
        throw new IOException("Jobworker metadata directory is already in use: " + directory);
      }
      Path identity = directory.resolve("worker.id");
      Path manifest = directory.resolve("receipts.manifest");
      if (!Files.exists(identity)) {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.pb")) {
          if (stream.iterator().hasNext()) {
            throw new IOException("Missing Jobworker identity for existing durable receipts");
          }
        }
        if (Files.exists(manifest)
            && JobworkerReceiptManifest.parseFrom(Files.readAllBytes(manifest)).getReceiptKeysCount() != 0) {
          throw new IOException("Missing Jobworker identity for an existing receipt manifest");
        }
        write(manifest, JobworkerReceiptManifest.getDefaultInstance().toByteArray());
        write(identity, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
      }
      workerId = UUID.fromString(new String(Files.readAllBytes(identity), StandardCharsets.UTF_8));
      JobworkerReceiptManifest expected = JobworkerReceiptManifest.parseFrom(Files.readAllBytes(manifest));
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory, "*.pb")) {
        for (Path path : stream) {
          JobworkerCommandRecord record = JobworkerCommandRecord.parseFrom(Files.readAllBytes(path));
          validate(record);
          if (!path.equals(file(record.getStatus()))) {
            throw new IOException("Unexpected Jobworker receipt filename: " + path);
          }
          records.put(key(record.getStatus()), record);
          if (records.size() > maxRecords) {
            throw new IOException("Jobworker journal exceeds configured record limit");
          }
        }
      }
      for (String receiptKey : expected.getReceiptKeysList()) {
        if (!records.containsKey(receiptKey)) {
          throw new IOException("Missing durable Jobworker receipt: " + receiptKey);
        }
      }
      // A crash may leave a durable PENDING receipt just before its admission manifest was committed.
      writeManifest();
      for (JobworkerCommandRecord record : new ArrayList<>(records.values())) {
        if (!record.getAcknowledged() && record.getStatus().getStatus() == CommandStatus.Status.EXECUTING) {
          save(record.toBuilder().setStatus(record.getStatus().toBuilder()
              .setStatus(CommandStatus.Status.UNCERTAIN)
              .setMsg("Worker restarted after execution may have begun; reconciliation is required")).build());
        }
      }
      lock = acquired;
    } catch (IOException | IllegalArgumentException | OverlappingFileLockException e) {
      if (acquired != null) {
        acquired.release();
      }
      lockChannel.close();
      throw new IOException("Cannot open Jobworker command journal " + directory, e);
    }
  }

  public UUID getWorkerId() {
    return workerId;
  }

  public synchronized List<JobworkerCommandRecord> records() {
    return new ArrayList<>(records.values());
  }

  public synchronized JobworkerCommandRecord receipt(String service, OMJobworkerCommandProto command)
      throws IOException {
    requireOpen();
    boolean reconcileOnly = command.getReconcileOnly();
    command = command.toBuilder().clearReconcileOnly().build();
    CommandStatus pending = CommandStatus.newBuilder().setCmdId(command.getCmdId())
        .setType(command.getCommandType()).setOmServiceId(service).setAttemptId(command.getAttemptId())
        .setStatus(reconcileOnly ? CommandStatus.Status.UNCERTAIN : CommandStatus.Status.PENDING).build();
    JobworkerCommandRecord proposed = JobworkerCommandRecord.newBuilder()
        .setOmServiceId(service).setCommand(command).setStatus(pending).build();
    validate(proposed);
    JobworkerCommandRecord previous = records.get(key(pending));
    if (previous != null) {
      if (!previous.getCommand().equals(command)) {
        throw new IOException("Jobworker job identity was reused with a different attempt or payload");
      }
      if (manifestDirty) {
        writeManifest();
      }
      return previous;
    }
    if (records.size() >= maxRecords) {
      throw new IOException("Jobworker durable receipt capacity reached; refusing new work without dropping results");
    }
    save(proposed);
    manifestDirty = true;
    writeManifest();
    return proposed;
  }

  public synchronized void update(CommandStatus status) throws IOException {
    requireOpen();
    JobworkerCommandRecord previous = records.get(key(status));
    if (previous == null || !previous.getStatus().getAttemptId().equals(status.getAttemptId())
        || previous.getAcknowledged()) {
      throw new IOException("Cannot update an unknown or acknowledged Jobworker attempt");
    }
    if (terminal(previous.getStatus()) && !previous.getStatus().equals(status)) {
      throw new IOException("Cannot replace a terminal Jobworker result");
    }
    save(previous.toBuilder().setStatus(status).build());
  }

  public synchronized boolean acknowledge(CommandStatus result) throws IOException {
    requireOpen();
    JobworkerCommandRecord previous = records.get(key(result));
    if (previous == null || !previous.getStatus().getAttemptId().equals(result.getAttemptId())
        || !terminal(result) || (terminal(previous.getStatus()) && !previous.getStatus().equals(result))) {
      return false;
    }
    if (!previous.getAcknowledged()) {
      save(previous.toBuilder().setStatus(result).setAcknowledged(true).build());
    }
    return true;
  }

  private static boolean terminal(CommandStatus status) {
    return status.getStatus() == CommandStatus.Status.SUCCEEDED || status.getStatus() == CommandStatus.Status.FAILED;
  }

  private static void validate(JobworkerCommandRecord record) throws IOException {
    OMJobworkerCommandProto command = record.getCommand();
    CommandStatus status = record.getStatus();
    try {
      UUID.fromString(command.getAttemptId());
    } catch (IllegalArgumentException e) {
      throw new IOException("Invalid durable attempt ID", e);
    }
    if (!command.hasCmdId() || command.getCmdId() <= 0 || record.getOmServiceId().isEmpty()
        || command.getCommandType() == OMJobworkerCommandProto.Type.unknownCommand
        || command.getCommandType() == OMJobworkerCommandProto.Type.reregisterCommand
        || !record.getOmServiceId().equals(status.getOmServiceId())
        || command.getCmdId() != status.getCmdId() || command.getCommandType() != status.getType()
        || !command.getAttemptId().equals(status.getAttemptId())
        || status.getStatus() == CommandStatus.Status.UNKNOWN_STATUS
        || (record.getAcknowledged() && !terminal(status))) {
      throw new IOException("Invalid durable Jobworker receipt");
    }
  }

  private static String key(CommandStatus status) {
    return BaseEncoding.base16().lowerCase().encode(status.getOmServiceId().getBytes(StandardCharsets.UTF_8))
        + "." + status.getType().getNumber() + "." + status.getCmdId();
  }

  private Path file(CommandStatus status) {
    return directory.resolve(key(status) + ".pb");
  }

  private void save(JobworkerCommandRecord record) throws IOException {
    validate(record);
    write(file(record.getStatus()), record.toByteArray());
    records.put(key(record.getStatus()), record);
  }

  private void writeManifest() throws IOException {
    write(directory.resolve("receipts.manifest"), JobworkerReceiptManifest.newBuilder()
        .addAllReceiptKeys(new java.util.TreeSet<>(records.keySet())).build().toByteArray());
    manifestDirty = false;
  }

  private void write(Path path, byte[] bytes) throws IOException {
    AtomicFileOutputStream output = new AtomicFileOutputStream(path.toFile());
    try {
      output.write(bytes);
      output.close();
      syncDirectory(directory);
    } catch (IOException e) {
      output.abort();
      throw e;
    }
  }

  private static void syncDirectory(Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      channel.force(true);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    try {
      lock.release();
    } finally {
      lockChannel.close();
    }
  }

  private void requireOpen() throws IOException {
    if (closed) {
      throw new IOException("Jobworker command journal is closed");
    }
  }
}
