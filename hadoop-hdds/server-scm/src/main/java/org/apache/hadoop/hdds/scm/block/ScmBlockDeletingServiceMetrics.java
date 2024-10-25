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

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

/**
 * Metrics related to Block Deleting Service running in SCM.
 */
@Metrics(name = "ScmBlockDeletingService Metrics", about = "Metrics related to "
    + "background block deleting service in SCM", context = "SCM")
public final class ScmBlockDeletingServiceMetrics {

  private static ScmBlockDeletingServiceMetrics instance;
  public static final String SOURCE_NAME =
      SCMBlockDeletingService.class.getSimpleName();

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

  private ScmBlockDeletingServiceMetrics() {
  }

  public static ScmBlockDeletingServiceMetrics create() {
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
            + numBlockDeletionTransactionFailure.value());
    return buffer.toString();
  }
}
