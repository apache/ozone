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

package org.apache.hadoop.ozone.admin.scm;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.DeletedBlocksTransactionSummary;
import org.apache.hadoop.hdds.protocol.proto.StorageContainerLocationProtocolProtos
    .RepairDeletedBlocksTxnSummaryFromCheckpointResponseProto;
import org.apache.hadoop.hdds.scm.cli.ScmSubcommand;
import org.apache.hadoop.hdds.scm.client.ScmClient;
import picocli.CommandLine;

/**
 * Handler of getting deleted blocks summary from SCM side.
 *
 * <ul>
 *   <li><b>(no flag)</b> — print in-memory summary.</li>
 *   <li><b>--detail</b> — take a RocksDB checkpoint and show Checkpoint-Actual
 *       vs Checkpoint-Persisted for each dimension.</li>
 *   <li><b>--repair</b> — same as --detail, then compute the diff, ask the
 *       operator to confirm, and apply it to the live in-memory counters.</li>
 * </ul>
 */
@CommandLine.Command(
    name = "summary",
    description = "Get DeletedBlocksTransaction summary",
    mixinStandardHelpOptions = true,
    versionProvider = HddsVersionProvider.class)
public class GetDeletedBlockSummarySubcommand extends ScmSubcommand {

  @CommandLine.Option(names = {"--detail"},
      description = "Take a RocksDB checkpoint and compare the actual row "
          + "counts in the checkpoint with the persisted summary stored in the "
          + "same checkpoint.")
  private boolean detail = false;

  @CommandLine.Option(names = {"--repair"},
      description = "Same as --detail: shows the diff between checkpoint-actual "
          + "and checkpoint-persisted, then prompts the operator before applying "
          + "the diff to the live in-memory counters.")
  private boolean repair = false;

  @Override
  public void execute(ScmClient client) throws IOException {
    if (repair) {
      executeRepair(client);
    } else if (detail) {
      executeDetail(client);
    } else {
      executeDefault(client);
    }
  }

  private void executeDefault(ScmClient client) throws IOException {
    DeletedBlocksTransactionSummary summary = client.getDeletedBlockSummary();
    if (summary == null) {
      System.out.println("DeletedBlocksTransaction summary is not available.");
      return;
    }
    System.out.println("DeletedBlocksTransaction summary:");
    System.out.println("Total number of transactions: " + summary.getTotalTransactionCount());
    System.out.println("Total number of blocks: " + summary.getTotalBlockCount());
    System.out.println("Total size of blocks: " + summary.getTotalBlockSize());
    System.out.println("Total replicated size of blocks: " + summary.getTotalBlockReplicatedSize());
  }

  private void executeDetail(ScmClient client) throws IOException {
    System.out.println("Taking RocksDB checkpoint and scanning deletedBlocks table ...");
    RepairDeletedBlocksTxnSummaryFromCheckpointResponseProto resp =
        client.getDeletedBlockSummaryFromCheckpoint();
    printComparisonTable(resp);
  }

  private void executeRepair(ScmClient client) throws IOException {
    System.out.println("Taking RocksDB checkpoint and scanning deletedBlocks table ...");
    RepairDeletedBlocksTxnSummaryFromCheckpointResponseProto resp =
        client.getDeletedBlockSummaryFromCheckpoint();
    printComparisonTable(resp);

    DeletedBlocksTransactionSummary actual =
        resp.hasCheckpointActual() ? resp.getCheckpointActual() : null;
    DeletedBlocksTransactionSummary persisted =
        resp.hasCheckpointPersisted() ? resp.getCheckpointPersisted() : null;

    if (!hasDiff(actual, persisted)) {
      System.out.println("\nCheckpoint is consistent and nothing to repair.");
      return;
    }

    System.out.println("\nDiff (Checkpoint-Actual minus Checkpoint-Persisted):");
    printDiff(actual, persisted);

    System.out.print("\nApply this diff to live in-memory counters? [y/N]: ");
    System.out.flush();
    String answer = new Scanner(System.in, StandardCharsets.UTF_8.name()).nextLine().trim();
    if (!answer.equalsIgnoreCase("y") && !answer.equalsIgnoreCase("yes")) {
      System.out.println("Aborted. No changes applied.");
      return;
    }

    System.out.println("Applying diff ...");
    RepairDeletedBlocksTxnSummaryFromCheckpointResponseProto repaired =
        client.repairDeletedBlockSummaryFromCheckpoint();

    System.out.println("\nRepair complete.");
    System.out.println("  Live in-memory before: "
        + inline(repaired.hasLiveInMemBefore() ? repaired.getLiveInMemBefore() : null));
    System.out.println("  Live in-memory after : "
        + inline(repaired.hasLiveInMemAfter()  ? repaired.getLiveInMemAfter()  : null));
  }

  private static void printComparisonTable(
      RepairDeletedBlocksTxnSummaryFromCheckpointResponseProto resp) {

    DeletedBlocksTransactionSummary actual =
        resp.hasCheckpointActual() ? resp.getCheckpointActual() : null;
    DeletedBlocksTransactionSummary persisted =
        resp.hasCheckpointPersisted() ? resp.getCheckpointPersisted() : null;

    System.out.println();
    System.out.println("=== DeletedBlocksTransaction Checkpoint Comparison ===");
    System.out.printf("%-36s  %-18s  %-18s%n",
        "Metric", "Checkpoint Actual", "Checkpoint Persisted");

    printRow("Total transactions",
        get(actual,    DeletedBlocksTransactionSummary::getTotalTransactionCount),
        get(persisted, DeletedBlocksTransactionSummary::getTotalTransactionCount));
    printRow("Total blocks",
        get(actual,    DeletedBlocksTransactionSummary::getTotalBlockCount),
        get(persisted, DeletedBlocksTransactionSummary::getTotalBlockCount));
    printRow("Total Block size (bytes)",
        get(actual,    DeletedBlocksTransactionSummary::getTotalBlockSize),
        get(persisted, DeletedBlocksTransactionSummary::getTotalBlockSize));
    printRow("Total Replicated size (bytes)",
        get(actual,    DeletedBlocksTransactionSummary::getTotalBlockReplicatedSize),
        get(persisted, DeletedBlocksTransactionSummary::getTotalBlockReplicatedSize));
  }

  private static void printRow(String metric, long actual, long persisted) {
    String tag = (actual >= 0 && persisted >= 0 && actual != persisted)
        ? "  <-- diff=" + (actual - persisted)
        : "";
    System.out.printf("%-36s  %-18s  %-18s%s%n",
        metric, fmt(actual), fmt(persisted), tag);
  }

  private static void printDiff(DeletedBlocksTransactionSummary actual,
      DeletedBlocksTransactionSummary persisted) {
    if (actual == null || persisted == null) {
      System.out.println("  (unavailable)");
      return;
    }
    System.out.printf("  transactions   : %+d%n",
        actual.getTotalTransactionCount()    - persisted.getTotalTransactionCount());
    System.out.printf("  blocks         : %+d%n",
        actual.getTotalBlockCount()          - persisted.getTotalBlockCount());
    System.out.printf("  blockSize      : %+d%n",
        actual.getTotalBlockSize()           - persisted.getTotalBlockSize());
    System.out.printf("  replicatedSize : %+d%n",
        actual.getTotalBlockReplicatedSize() - persisted.getTotalBlockReplicatedSize());
  }

  private static boolean hasDiff(DeletedBlocksTransactionSummary actual,
      DeletedBlocksTransactionSummary persisted) {
    if (actual == null || persisted == null) {
      return false;
    }
    return actual.getTotalTransactionCount()    != persisted.getTotalTransactionCount()
        || actual.getTotalBlockCount()          != persisted.getTotalBlockCount()
        || actual.getTotalBlockSize()           != persisted.getTotalBlockSize()
        || actual.getTotalBlockReplicatedSize() != persisted.getTotalBlockReplicatedSize();
  }

  private static String inline(DeletedBlocksTransactionSummary s) {
    if (s == null) {
      return "N/A";
    }
    return String.format("txns=%d blocks=%d size=%d repl=%d",
        s.getTotalTransactionCount(), s.getTotalBlockCount(),
        s.getTotalBlockSize(), s.getTotalBlockReplicatedSize());
  }

  @FunctionalInterface
  private interface LongGetter {
    long get(DeletedBlocksTransactionSummary s);
  }

  private static long get(DeletedBlocksTransactionSummary s, LongGetter getter) {
    return s == null ? -1L : getter.get(s);
  }

  private static String fmt(long v) {
    return v < 0 ? "N/A" : String.valueOf(v);
  }
}
