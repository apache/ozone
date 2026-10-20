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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.CommandStatus;
import org.apache.hadoop.hdds.protocol.jobworker.proto.JobworkerServiceProtocolProtos.OMJobworkerCommandProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Crash recovery, result acknowledgement and duplicate suppression at the worker. */
class TestJobworkerCommandJournal {
  @TempDir
  private Path directory;

  private OzoneConfiguration conf() {
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set("ozone.jobworker.metadata.dir", directory.toString());
    return conf;
  }

  private OMJobworkerCommandProto command(long id) {
    return OMJobworkerCommandProto.newBuilder().setCommandType(OMJobworkerCommandProto.Type.mockCommand)
        .setCmdId(id).setAttemptId(UUID.randomUUID().toString()).build();
  }

  @Test
  void testIdentityAndExclusiveOwnership() throws Exception {
    UUID identity;
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      identity = journal.getWorkerId();
      assertThrows(IOException.class, () -> new JobworkerCommandJournal(conf()));
    }
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      assertEquals(identity, journal.getWorkerId());
    }
  }

  @Test
  void testClosedJournalCannotMutateStateOwnedByRestartedWorker() throws Exception {
    OMJobworkerCommandProto command = command(42);
    JobworkerCommandJournal previous = new JobworkerCommandJournal(conf());
    CommandStatus pending;
    try {
      pending = previous.receipt("service", command).getStatus();
    } finally {
      previous.close();
    }
    try (JobworkerCommandJournal restarted = new JobworkerCommandJournal(conf())) {
      CommandStatus terminal = pending.toBuilder().setStatus(CommandStatus.Status.SUCCEEDED).build();
      assertThrows(IOException.class, () -> previous.receipt("service", command(43)));
      assertThrows(IOException.class, () -> previous.update(terminal));
      assertThrows(IOException.class, () -> previous.acknowledge(terminal));
      previous.close();
      assertEquals(pending, restarted.records().get(0).getStatus());
    }
    try (JobworkerCommandJournal restarted = new JobworkerCommandJournal(conf())) {
      assertEquals(pending, restarted.records().get(0).getStatus());
    }
  }

  @Test
  void testCompletedResultSurvivesRestartUntilCommittedAcknowledgement() throws Exception {
    OMJobworkerCommandProto command = command(42);
    CommandStatus completed;
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      manager.acceptDurableCommand(command, "service", 1);
      manager.getCmdStatus("service", 42L)
          .updateStatusAndMessage(CommandStatus.Status.SUCCEEDED, "done", null);
      completed = manager.getCmdStatus("service", 42L).getProtobufMessage();
    }
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      assertEquals(completed, manager.getCmdStatus("service", 42L).getProtobufMessage());
      manager.acknowledge(completed.toBuilder().setAttemptId(UUID.randomUUID().toString()).build());
      assertNotNull(manager.getCmdStatus("service", 42L));
      manager.acknowledge(completed);
      assertNull(manager.getCmdStatus("service", 42L));
    }
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      manager.acceptDurableCommand(command, "service", 10);
      assertNull(manager.getNextCommand(), "An acknowledged attempt must not execute after redelivery/restart");
      assertNull(manager.getCmdStatus("service", 42L));
      assertTrue(journal.records().get(0).getAcknowledged());
    }
  }

  @Test
  void testWorkerCrashDoesNotReexecuteAnUncertainAttempt() throws Exception {
    OMJobworkerCommandProto command = command(42);
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      manager.acceptDurableCommand(command, "service", 1);
      assertNotNull(manager.getNextCommand());
      manager.getCmdStatus("service", 42L)
          .updateStatusAndMessage(CommandStatus.Status.EXECUTING, null, null);
    }
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      manager.acceptDurableCommand(command, "service", 2);
      assertEquals(CommandStatus.Status.UNCERTAIN, manager.getCmdStatus("service", 42L).getStatus());
      assertNull(manager.getNextCommand());
    }
  }

  @Test
  void testPendingRecoveryWaitsForLeaderRedeliveryAndDeduplicates() throws Exception {
    OMJobworkerCommandProto command = command(42);
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      journal.receipt("service", command);
    }
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      assertNull(manager.getNextCommand());
      manager.updateTermOfLeaderOM("service", 5);
      manager.acceptDurableCommand(command, "service", 5);
      manager.acceptDurableCommand(command, "service", 6);
      assertEquals(command.getAttemptId(), manager.getNextCommand().getAttemptId());
      manager.acceptDurableCommand(command, "service", 6);
      assertNull(manager.getNextCommand());
      assertThrows(IOException.class, () -> manager.acceptDurableCommand(
          command.toBuilder().setAttemptId(UUID.randomUUID().toString()).build(), "service", 6));
    }
  }

  @Test
  void testAdmissionAtCapacityPreservesExistingResults() throws Exception {
    OzoneConfiguration conf = conf();
    conf.setInt("ozone.jobworker.journal.max.records", 1);
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf)) {
      journal.receipt("service", command(1));
      assertThrows(IOException.class, () -> journal.receipt("service", command(2)));
      assertEquals(1, journal.records().size());
    }
  }

  @Test
  void testMissingReceiptForExecutingAssignmentCannotStartWork() throws Exception {
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      manager.acceptDurableCommand(command(42).toBuilder().setReconcileOnly(true).build(), "service", 2);
      assertEquals(CommandStatus.Status.UNCERTAIN, manager.getCmdStatus("service", 42L).getStatus());
      assertNull(manager.getNextCommand());
    }
  }

  @Test
  void testAuthoritativelyResolvedUncertaintyCanBeAcknowledgedWithoutExecution() throws Exception {
    OMJobworkerCommandProto command = command(42).toBuilder().setReconcileOnly(true).build();
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      JobworkerCommandManager manager = new JobworkerCommandManager(conf());
      manager.setJournal(journal);
      manager.acceptDurableCommand(command, "service", 2);
      CommandStatus result = manager.getCmdStatus("service", 42L).getProtobufMessage().toBuilder()
          .setStatus(CommandStatus.Status.SUCCEEDED).setMsg("Authoritative effects were reconciled").build();
      manager.acknowledge(result);
      assertNull(manager.getNextCommand());
      assertNull(manager.getCmdStatus("service", 42L));
      assertTrue(journal.records().get(0).getAcknowledged());
      assertEquals(result, journal.records().get(0).getStatus());
    }
  }

  @Test
  void testCorruptReceiptFailsClosed() throws Exception {
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      journal.receipt("service", command(1));
    }
    try (java.nio.file.DirectoryStream<Path> files = Files.newDirectoryStream(directory, "*.pb")) {
      Files.write(files.iterator().next(), new byte[]{0});
    }
    assertThrows(IOException.class, () -> new JobworkerCommandJournal(conf()));
  }

  @Test
  void testMissingReceiptFailsClosedInsteadOfForgettingExecution() throws Exception {
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      journal.receipt("service", command(42));
    }
    try (java.nio.file.DirectoryStream<Path> files = Files.newDirectoryStream(directory, "*.pb")) {
      Files.delete(files.iterator().next());
    }
    assertThrows(IOException.class, () -> new JobworkerCommandJournal(conf()));
  }

  @Test
  void testMissingIdentityOrManifestCannotInitializeANewWorkerOverOldReceipts() throws Exception {
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      journal.receipt("service", command(42));
    }
    Path identity = directory.resolve("worker.id");
    byte[] savedIdentity = Files.readAllBytes(identity);
    Files.delete(identity);
    assertThrows(IOException.class, () -> new JobworkerCommandJournal(conf()));
    Files.write(identity, savedIdentity);
    Files.delete(directory.resolve("receipts.manifest"));
    assertThrows(IOException.class, () -> new JobworkerCommandJournal(conf()));
  }

  @Test
  void testServiceNamesDoNotAliasOnCaseInsensitiveFilesystems() throws Exception {
    // Their Base64 encodings differ only by case (YWFH / YWFh).
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      journal.receipt("aaG", command(42));
      journal.receipt("aaa", command(42));
    }
    try (JobworkerCommandJournal journal = new JobworkerCommandJournal(conf())) {
      assertEquals(2, journal.records().size());
    }
  }
}
