/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.scm.block;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.Interns;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
  private MutableCounterLong numBlockDeletionTransactionSent;

  @Metric(about = "The number of success execution of delete transactions.")
  private MutableCounterLong numBlockDeletionTransactionSuccess;

  @Metric(about = "The number of failure execution of delete transactions.")
  private MutableCounterLong numBlockDeletionTransactionFailure;

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

  private final Map<UUID, DatanodeCommandCounts> numCommandsDatanode = new ConcurrentHashMap<>();

  private ScmBlockDeletingServiceMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  public static synchronized ScmBlockDeletingServiceMetrics create() {
    if (instance == null) {
      MetricsSystem ms = DefaultMetricsSystem.instance();
      instance = ms.register(SOURCE_NAME, "SCMBlockDeletingService",
          new ScmBlockDeletingServiceMetrics());
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

  public void incrBlockDeletionTransactionSent(long count) {
    this.numBlockDeletionTransactionSent.incr(count);
  }

  public void incrBlockDeletionTransactionFailure() {
    this.numBlockDeletionTransactionFailure.incr();
  }

  public void incrBlockDeletionTransactionSuccess() {
    this.numBlockDeletionTransactionSuccess.incr();
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

  public void incrDNCommandsSent(UUID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandCounts())
        .incrCommandsSent(delta);
  }
  public void incrDNCommandsSuccess(UUID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandCounts())
        .incrCommandsSuccess(delta);
  }
  public void incrDNCommandsFailure(UUID id, long delta) {
    numCommandsDatanode.computeIfAbsent(id, k -> new DatanodeCommandCounts())
        .incrCommandsFailure(delta);
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

  public long getNumBlockDeletionTransactionSent() {
    return numBlockDeletionTransactionSent.value();
  }

  public long getNumBlockDeletionTransactionFailure() {
    return numBlockDeletionTransactionFailure.value();
  }

  public long getNumBlockDeletionTransactionSuccess() {
    return numBlockDeletionTransactionSuccess.value();
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
    numBlockDeletionTransactionSent.snapshot(builder, all);
    numBlockDeletionTransactionSuccess.snapshot(builder, all);
    numBlockDeletionTransactionFailure.snapshot(builder, all);
    numBlockDeletionTransactionCompleted.snapshot(builder, all);
    numBlockDeletionTransactionCreated.snapshot(builder, all);
    numSkippedTransactions.snapshot(builder, all);
    numProcessedTransactions.snapshot(builder, all);
    numBlockDeletionTransactionDataNodes.snapshot(builder, all);

    MetricsRecordBuilder recordBuilder = builder;
    for (Map.Entry<UUID, DatanodeCommandCounts> e : numCommandsDatanode.entrySet()) {
      recordBuilder = recordBuilder.endRecord().addRecord(SOURCE_NAME)
          .add(new MetricsTag(Interns.info("datanode",
              "Datanode host for deletion commands"), e.getKey().toString()))
          .addGauge(DatanodeCommandCounts.COMMANDS_SENT_TO_DN,
              e.getValue().getCommandsSent())
          .addGauge(DatanodeCommandCounts.COMMANDS_SUCCESSFUL_EXECUTION_BY_DN,
              e.getValue().getCommandsSuccess())
          .addGauge(DatanodeCommandCounts.COMMANDS_FAILED_EXECUTION_BY_DN,
              e.getValue().getCommandsFailure());
    }
    recordBuilder.endRecord();
  }

  /**
   *  Class contains metrics related to the ScmBlockDeletingService for each datanode.
   */
  public static final class DatanodeCommandCounts {
    private long commandsSent;
    private long commandsSuccess;
    private long commandsFailure;

    private static final MetricsInfo COMMANDS_SENT_TO_DN = Interns.info(
        "CommandsSent",
        "Number of commands sent from SCM to the datanode for deletion");
    private static final MetricsInfo COMMANDS_SUCCESSFUL_EXECUTION_BY_DN = Interns.info(
        "CommandsSuccess",
        "Number of commands sent from SCM to the datanode for deletion for which execution succeeded.");
    private static final MetricsInfo COMMANDS_FAILED_EXECUTION_BY_DN = Interns.info(
        "CommandsFailed",
        "Number of commands sent from SCM to the datanode for deletion for which execution failed.");

    public DatanodeCommandCounts() {
      this.commandsSent = 0;
      this.commandsSuccess = 0;
      this.commandsFailure = 0;
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

    public long getCommandsSent() {
      return commandsSent;
    }

    public long getCommandsSuccess() {
      return commandsSuccess;
    }

    public long getCommandsFailure() {
      return commandsFailure;
    }

    @Override
    public String toString() {
      return "Sent=" + commandsSent + ", Success=" + commandsSuccess + ", Failed=" + commandsFailure;
    }
  }

  public long getNumCommandsDatanodeSent() {
    long sent = 0;
    for (DatanodeCommandCounts v : numCommandsDatanode.values()) {
      sent += v.commandsSent;
    }
    return sent;
  }
  public long getNumCommandsDatanodeSuccess() {
    long successCount = 0;
    for (DatanodeCommandCounts v : numCommandsDatanode.values()) {
      successCount += v.commandsSuccess;
    }
    return successCount;
  }
  public long getNumCommandsDatanodeFailed() {
    long failCount = 0;
    for (DatanodeCommandCounts v : numCommandsDatanode.values()) {
      failCount += v.commandsFailure;
    }
    return failCount;
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("numBlockDeletionTransactionCreated = "
        + numBlockDeletionTransactionCreated.value()).append("\t")
        .append("numBlockDeletionTransactionCompleted = "
            + numBlockDeletionTransactionCompleted.value()).append("\t")
        .append("numBlockDeletionCommandSent = "
            + numBlockDeletionCommandSent.value()).append("\t")
        .append("numBlockDeletionCommandSuccess = "
            + numBlockDeletionCommandSuccess.value()).append("\t")
        .append("numBlockDeletionCommandFailure = "
            + numBlockDeletionCommandFailure.value()).append("\t")
        .append("numBlockDeletionTransactionSent = "
            + numBlockDeletionTransactionSent.value()).append("\t")
        .append("numBlockDeletionTransactionSuccess = "
            + numBlockDeletionTransactionSuccess.value()).append("\t")
        .append("numBlockDeletionTransactionFailure = "
            + numBlockDeletionTransactionFailure.value()).append("\t")
        .append("numDeletionCommandsPerDatanode = "
            + numCommandsDatanode);
    return buffer.toString();
  }
}
