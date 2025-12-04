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

package org.apache.hadoop.hdds.scm.block;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hdds.protocol.DatanodeID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Metrics related to Block Deleting Service running in SCM.
 */
@Metrics(name = "ScmBlockDeletingService Metrics", about = "Metrics related to "
    + "background block deleting service in SCM", context = "SCM")
public final class ScmBlockDeletingServiceMetrics implements MetricsSource {

  private static ScmBlockDeletingServiceMetrics instance;
  public static final String SOURCE_NAME =
      SCMBlockDeletingService.class.getSimpleName();
  private final MetricsRegistry registry;
  private final BlockManager blockManager;

  /**
   * Given all commands are finished and no new coming deletes from OM.
   * If there is no command resent,
   * deleteTxSent = deleteTxSuccess + deleteTxFailure
   * deleteTxCreated = deleteTxCompleted
   * deleteTxSent = deleteTxCreated * replication factor
   * deleteTxCmdSent = deleteTxCmdSuccess + deleteTxCmdFailure
   *
   * If there is command resent,
   * deleteTxSent > deleteTxSuccess + deleteTxFailure
   * deleteTxCreated = deleteTxCompleted
   * deleteTxSent > deleteTxCreated * replication factor
   * deleteTxCmdSent > deleteTxCmdSuccess + deleteTxCmdFailure
   */
  @Metric(about = "The number of individual delete transaction commands sent " +
      "to all DN.")
  private MutableCounterLong numBlockDeletionCommandSent;

  @Metric(about = "The number of success ACK of delete transaction commands.")
  private MutableCounterLong numBlockDeletionCommandSuccess;

  @Metric(about = "The number of failure ACK of delete transaction commands.")
  private MutableCounterLong numBlockDeletionCommandFailure;

  @Metric(about = "The number of individual delete transactions sent to " +
      "all DN.")
  private MutableCounterLong numBlockDeletionTransactionsOnDatanodes;

  @Metric(about = "The number of success execution of delete transactions.")
  private MutableCounterLong numBlockDeletionTransactionSuccessOnDatanodes;

  @Metric(about = "The number of failure execution of delete transactions.")
  private MutableCounterLong numBlockDeletionTransactionFailureOnDatanodes;

  @Metric(about = "The number of completed txs which are removed from DB.")
  private MutableCounterLong numBlockDeletionTransactionCompleted;

  @Metric(about = "The number of created txs which are added into DB.")
  private MutableCounterLong numBlockDeletionTransactionCreated;

  @Metric(about = "The number of skipped transactions")
  private MutableCounterLong numSkippedTransactions;

  @Metric(about = "The number of processed transactions")
  private MutableCounterLong numProcessedTransactions;

  @Metric(about = "The number of dataNodes of delete transactions.")
  private MutableGaugeLong numBlockDeletionTransactionDataNodes;

  @Metric(about = "Total blocks sent to DN for deletion.")
  private MutableGaugeLong numBlockAddedForDeletionToDN;

  private final Map<DatanodeID, DatanodeCommandDetails> numCommandsDatanode = new ConcurrentHashMap<>();

  private static final MetricsInfo NUM_BLOCK_DELETION_TRANSACTIONS = Interns.info(
      "numBlockDeletionTransactions",
      "The number of transactions in DB.");

  private static final MetricsInfo NUM_BLOCK_OF_ALL_DELETION_TRANSACTIONS = Interns.info(
      "numBlockOfAllDeletionTransactions",
      "The number of blocks in all transactions in DB.");

  private static final MetricsInfo BLOCK_SIZE_OF_ALL_DELETION_TRANSACTIONS = Interns.info(
      "blockSizeOfAllDeletionTransactions",
      "The size of all blocks in all transactions in DB.");

  private static final MetricsInfo REPLICATED_BLOCK_SIZE_OF_ALL_DELETION_TRANSACTIONS = Interns.info(
      "replicatedBlockSizeOfAllDeletionTransactions",
      "The replicated size of all blocks in all transactions in DB.");

  private ScmBlockDeletingServiceMetrics(BlockManager blockManager) {
    this.registry = new MetricsRegistry(SOURCE_NAME);
    this.blockManager = blockManager;
  }

  public static synchronized ScmBlockDeletingServiceMetrics create(BlockManager blockManager) {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(SOURCE_NAME, "SCMBlockDeletingService",
          new ScmBlockDeletingServiceMetrics(blockManager));
    }

    return instance;
  }

  /**
   * Unregister the metrics instance.
   */
  public static void unRegister() {
    instance = null;
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  public void incrBlockDeletionCommandSent() {
    this.numBlockDeletionCommandSent.incr();
  }

  public void incrBlockDeletionCommandSuccess() {
    this.numBlockDeletionCommandSuccess.incr();
  }

  public void incrBlockDeletionCommandFailure() {
    this.numBlockDeletionCommandFailure.incr();
  }

  public void incrBlockDeletionTransactionsOnDatanodes(long count) {
    this.numBlockDeletionTransactionsOnDatanodes.incr(count);
  }

  public void incrBlockDeletionTransactionFailureOnDatanodes() {
    this.numBlockDeletionTransactionFailureOnDatanodes.incr();
  }

  public void incrBlockDeletionTransactionSuccessOnDatanodes() {
    this.numBlockDeletionTransactionSuccessOnDatanodes.incr();
  }

  public void incrBlockDeletionTransactionCompleted(long count) {
    this.numBlockDeletionTransactionCompleted.incr(count);
  }

  public void incrBlockDeletionTransactionCreated(long count) {
    this.numBlockDeletionTransactionCreated.incr(count);
  }

  public void incrSkippedTransaction() {
    this.numSkippedTransactions.incr();
  }

  public void incrProcessedTransaction() {
    this.numProcessedTransactions.incr();
  }

  public void setNumBlockDeletionTransactionDataNodes(long dataNodes) {
    this.numBlockDeletionTransactionDataNodes.set(dataNodes);
  }

  public void incrDNCommandsSent(DatanodeID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandDetails())
        .incrCommandsSent(delta);
  }

  public void incrDNCommandsSuccess(DatanodeID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandDetails())
        .incrCommandsSuccess(delta);
  }

  public void incrDNCommandsFailure(DatanodeID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandDetails())
        .incrCommandsFailure(delta);
  }

  public void incrDNCommandsTimeout(DatanodeID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandDetails())
        .incrCommandsTimeout(delta);
  }

  public void incrNumBlockDeletionSentDN(DatanodeID id, long delta) {
    this.numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandDetails())
        .incrBlocksSent(delta);
  }

  public void incrTotalBlockSentToDNForDeletion(long count) {
    this.numBlockAddedForDeletionToDN.incr(count);
  }

  public long getNumBlockDeletionCommandSent() {
    return numBlockDeletionCommandSent.value();
  }

  public long getNumBlockDeletionCommandSuccess() {
    return numBlockDeletionCommandSuccess.value();
  }

  public long getBNumBlockDeletionCommandFailure() {
    return numBlockDeletionCommandFailure.value();
  }

  public long getNumBlockDeletionTransactionsOnDatanodes() {
    return numBlockDeletionTransactionsOnDatanodes.value();
  }

  public long getNumBlockDeletionTransactionFailureOnDatanodes() {
    return numBlockDeletionTransactionFailureOnDatanodes.value();
  }

  public long getNumBlockDeletionTransactionSuccessOnDatanodes() {
    return numBlockDeletionTransactionSuccessOnDatanodes.value();
  }

  public long getNumBlockDeletionTransactionCompleted() {
    return numBlockDeletionTransactionCompleted.value();
  }

  public long getNumBlockDeletionTransactionCreated() {
    return numBlockDeletionTransactionCreated.value();
  }

  public long getNumSkippedTransactions() {
    return numSkippedTransactions.value();
  }

  public long getNumProcessedTransactions() {
    return numProcessedTransactions.value();
  }

  public long getNumBlockDeletionTransactionDataNodes() {
    return numBlockDeletionTransactionDataNodes.value();
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder builder = metricsCollector.addRecord(SOURCE_NAME);
    numBlockDeletionCommandSent.snapshot(builder, all);
    numBlockDeletionCommandSuccess.snapshot(builder, all);
    numBlockDeletionCommandFailure.snapshot(builder, all);
    numBlockDeletionTransactionsOnDatanodes.snapshot(builder, all);
    numBlockDeletionTransactionSuccessOnDatanodes.snapshot(builder, all);
    numBlockDeletionTransactionFailureOnDatanodes.snapshot(builder, all);
    numBlockDeletionTransactionCompleted.snapshot(builder, all);
    numBlockDeletionTransactionCreated.snapshot(builder, all);
    numSkippedTransactions.snapshot(builder, all);
    numProcessedTransactions.snapshot(builder, all);
    numBlockDeletionTransactionDataNodes.snapshot(builder, all);
    numBlockAddedForDeletionToDN.snapshot(builder, all);

    // add metrics for deleted block transaction summary
    HddsProtos.DeletedBlocksTransactionSummary summary = blockManager.getDeletedBlockLog().getTransactionSummary();
    if (summary != null) {
      builder = builder.endRecord().addRecord(SOURCE_NAME)
          .addGauge(NUM_BLOCK_DELETION_TRANSACTIONS, summary.getTotalTransactionCount());
      builder = builder.endRecord().addRecord(SOURCE_NAME)
          .addGauge(NUM_BLOCK_OF_ALL_DELETION_TRANSACTIONS, summary.getTotalBlockCount());
      builder = builder.endRecord().addRecord(SOURCE_NAME)
          .addGauge(BLOCK_SIZE_OF_ALL_DELETION_TRANSACTIONS, summary.getTotalBlockSize());
      builder = builder.endRecord().addRecord(SOURCE_NAME)
          .addGauge(REPLICATED_BLOCK_SIZE_OF_ALL_DELETION_TRANSACTIONS, summary.getTotalBlockReplicatedSize());
    }

    MetricsRecordBuilder recordBuilder = builder;
    for (Map.Entry<DatanodeID, DatanodeCommandDetails> e : numCommandsDatanode.entrySet()) {
      recordBuilder = recordBuilder.endRecord().addRecord(SOURCE_NAME)
          .add(new MetricsTag(Interns.info("datanode",
              "Datanode host for deletion commands"), e.getKey().toString()))
          .addGauge(DatanodeCommandDetails.COMMANDS_SENT_TO_DN,
              e.getValue().getCommandsSent())
          .addGauge(DatanodeCommandDetails.COMMANDS_SUCCESSFUL_EXECUTION_BY_DN,
              e.getValue().getCommandsSuccess())
          .addGauge(DatanodeCommandDetails.COMMANDS_FAILED_EXECUTION_BY_DN,
              e.getValue().getCommandsFailure())
          .addGauge(DatanodeCommandDetails.COMMANDS_TIMEOUT_BY_DN,
              e.getValue().getCommandsTimeout())
          .addGauge(DatanodeCommandDetails.BLOCKS_SENT_TO_DN_COMMAND,
          e.getValue().getBlocksSent());
    }
    recordBuilder.endRecord();
  }

  /**
   *  Class contains metrics related to the ScmBlockDeletingService for each datanode.
   */
  public static final class DatanodeCommandDetails {
    private long commandsSent;
    private long commandsSuccess;
    private long commandsFailure;
    private long commandsTimeout;
    private long blocksSent;

    private static final MetricsInfo COMMANDS_SENT_TO_DN = Interns.info(
        "CommandsSent",
        "Number of commands sent from SCM to the datanode for deletion");
    private static final MetricsInfo COMMANDS_SUCCESSFUL_EXECUTION_BY_DN = Interns.info(
        "CommandsSuccess",
        "Number of commands sent from SCM to the datanode for deletion for which execution succeeded.");
    private static final MetricsInfo COMMANDS_FAILED_EXECUTION_BY_DN = Interns.info(
        "CommandsFailed",
        "Number of commands sent from SCM to the datanode for deletion for which execution failed.");
    
    private static final MetricsInfo COMMANDS_TIMEOUT_BY_DN = Interns.info(
        "CommandsTimeout",
        "Number of commands timeout from SCM to DN");

    private static final MetricsInfo BLOCKS_SENT_TO_DN_COMMAND = Interns.info(
        "BlocksSent",
        "Number of blocks sent to DN in a command for deletion.");

    public DatanodeCommandDetails() {
      this.commandsSent = 0;
      this.commandsSuccess = 0;
      this.commandsFailure = 0;
      this.commandsTimeout = 0;
      this.blocksSent = 0;
    }

    public void incrCommandsSent(long delta) {
      this.commandsSent += delta;
    }

    public void incrCommandsSuccess(long delta) {
      this.commandsSuccess += delta;
    }

    public void incrCommandsFailure(long delta) {
      this.commandsFailure += delta;
    }
    
    public void incrCommandsTimeout(long delta) {
      this.commandsTimeout += delta;
    }

    public void incrBlocksSent(long delta) {
      this.blocksSent += delta;
    }

    public long getCommandsSent() {
      return commandsSent;
    }

    public long getCommandsSuccess() {
      return commandsSuccess;
    }

    public long getCommandsFailure() {
      return commandsFailure;
    }
    
    public long getCommandsTimeout() {
      return commandsTimeout;
    }

    public long getBlocksSent() {
      return blocksSent;
    }

    @Override
    public String toString() {
      return "Sent=" + commandsSent + ", Success=" + commandsSuccess + ", Failed=" + commandsFailure + 
          ", Timeout=" + commandsTimeout + ", BlocksSent = " + blocksSent;
    }
  }

  public long getNumCommandsDatanodeSent() {
    long sent = 0;
    for (DatanodeCommandDetails v : numCommandsDatanode.values()) {
      sent += v.commandsSent;
    }
    return sent;
  }

  public long getNumCommandsDatanodeSuccess() {
    long successCount = 0;
    for (DatanodeCommandDetails v : numCommandsDatanode.values()) {
      successCount += v.commandsSuccess;
    }
    return successCount;
  }

  public long getNumCommandsDatanodeFailed() {
    long failCount = 0;
    for (DatanodeCommandDetails v : numCommandsDatanode.values()) {
      failCount += v.commandsFailure;
    }
    return failCount;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder()
        .append("numBlockDeletionTransactionCreated = ").append(numBlockDeletionTransactionCreated.value()).append('\t')
        .append("numBlockDeletionTransactionCompleted = ")
        .append(numBlockDeletionTransactionCompleted.value()).append('\t')
        .append("numBlockDeletionCommandSent = ").append(numBlockDeletionCommandSent.value()).append('\t')
        .append("numBlockDeletionCommandSuccess = ").append(numBlockDeletionCommandSuccess.value()).append('\t')
        .append("numBlockDeletionCommandFailure = ").append(numBlockDeletionCommandFailure.value()).append('\t')
        .append("numBlockDeletionTransactionsOnDatanodes = ").append(numBlockDeletionTransactionsOnDatanodes.value())
        .append('\t')
        .append("numBlockDeletionTransactionSuccessOnDatanodes = ")
        .append(numBlockDeletionTransactionSuccessOnDatanodes.value()).append('\t')
        .append("numBlockDeletionTransactionFailureOnDatanodes = ")
        .append(numBlockDeletionTransactionFailureOnDatanodes.value()).append('\t')
        .append("numBlockAddedForDeletionToDN = ")
        .append(numBlockAddedForDeletionToDN.value()).append('\t')
        .append("numDeletionCommandsPerDatanode = ").append(numCommandsDatanode);
    return buffer.toString();
  }
}
