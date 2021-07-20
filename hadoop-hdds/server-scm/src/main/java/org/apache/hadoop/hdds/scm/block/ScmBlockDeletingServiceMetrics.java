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

/**
 * Metrics related to Block Deleting Service running in SCM.
 */
@Metrics(name = "ScmBlockDeletingService Metrics", about = "Metrics related to "
    + "background block deleting service in SCM", context = "SCM")
public final class ScmBlockDeletingServiceMetrics {

  private static ScmBlockDeletingServiceMetrics instance;
  final public static String SOURCE_NAME =
      SCMBlockDeletingService.class.getSimpleName();

  /**
   * Without command retry and command loss, for a brand new cluster
   * deleteTxSent = deleteTxSuccess + deleteTxFailure
   * deleteTxCreated = deleteTxCompleted
   * deleteTxSent = deleteTxCreated * replication factor
   * <p>
   * deleteTxCmdSent = deleteTxCmdSuccess + deleteTxCmdFailure
   * <p>
   * With command retry and command loss
   * deleteTxSent >= deleteTxSuccess + deleteTxFailure
   * deleteTxCreated = deleteTxCompleted
   * deleteTxSent >= deleteTxCreated * replication factor
   * <p>
   * deleteTxCmdSent >= deleteTxCmdSuccess + deleteTxCmdFailure
   */
  @Metric(about = "The number of individual delete transaction commands sent " +
      "to all DN.")
  private MutableCounterLong deleteTxCmdSent;

  @Metric(about = "The number of success ACK of delete transaction cmds.")
  private MutableCounterLong deleteTxCmdSuccess;

  @Metric(about = "The number of failure ACK of delete transaction cmds.")
  private MutableCounterLong deleteTxCmdFailure;

  @Metric(about = "The number of individual delete transactions sent to " +
      "all DN.")
  private MutableCounterLong deleteTxSent;

  @Metric(about = "The number of success execution of delete transactions.")
  private MutableCounterLong deleteTxSuccess;

  @Metric(about = "The number of failure execution of delete transactions.")
  private MutableCounterLong deleteTxFailure;

  @Metric(about = "The number of completed txs which are removed from DB.")
  private MutableCounterLong deleteTxCompleted;

  @Metric(about = "The number of created txs which are added into DB.")
  private MutableCounterLong deleteTxCreated;

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

  public void incrementDeleteTxCmdSent() {
    this.deleteTxCmdSent.incr();
  }

  public void incrementDeleteTxCmdSuccess() {
    this.deleteTxCmdSuccess.incr();
  }

  public void incrementDeleteTxCmdFailure() {
    this.deleteTxCmdFailure.incr();
  }

  public void incrementDeleteTxSent(long count) {
    this.deleteTxSent.incr(count);
  }

  public void incrementDeleteTxFailure() {
    this.deleteTxFailure.incr();
  }

  public void incrementDeleteTxSuccess() {
    this.deleteTxSuccess.incr();
  }

  public void incrementDeleteTxCompleted(long count) {
    this.deleteTxCompleted.incr(count);
  }

  public void incrementDeleteTxCreated(long count) {
    this.deleteTxCreated.incr(count);
  }

  public long getDeleteTxCmdSent() {
    return deleteTxCmdSent.value();
  }

  public long getDeleteTxCmdSuccess() {
    return deleteTxCmdSuccess.value();
  }

  public long getDeleteTxCmdFailure() {
    return deleteTxCmdFailure.value();
  }

  public long getDeleteTxSent() {
    return deleteTxSent.value();
  }

  public long getDeleteTxFailure() {
    return deleteTxFailure.value();
  }

  public long getDeleteTxSuccess() {
    return deleteTxSuccess.value();
  }

  public long getDeleteTxCompleted() {
    return deleteTxCompleted.value();
  }

  public long getDeleteTxCreated() {
    return deleteTxCreated.value();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("DeleteTxCreated = " + deleteTxCreated.value()).append("\t")
        .append("DeleteTxCompleted = " + deleteTxCompleted.value()).append("\t")
        .append("DeleteTxCmdSent = " + deleteTxCmdSent.value()).append("\t")
        .append("DeleteTxCmdSuccess = " + deleteTxCmdSuccess.value())
        .append("\t")
        .append("DeleteTxCmdFailure = " + deleteTxCmdFailure.value())
        .append("\t")
        .append("DeleteTxSent = " + deleteTxSent.value()).append("\t")
        .append("DeleteTxSuccess = " + deleteTxSuccess.value()).append("\t")
        .append("DeleteTxFailure = " + deleteTxFailure.value());
    return buffer.toString();
  }
}
