package org.apache.hadoop.ozone.recon.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This interface is to be implemented by the various metrics for different tasks
 */
public abstract class ReconOmTaskMetrics implements MetricsSource {

  private final MetricsRegistry registry = new MetricsRegistry("ReconOMTasks");
  private final String taskName;
  private final String source;

  private @Metric MutableCounterLong taskReprocessCount;
  private @Metric MutableCounterLong taskReprocessFailedCount;
  private @Metric MutableRate reprocessLatency;

  private @Metric MutableCounterLong taskProcessCount;
  private @Metric MutableCounterLong taskProcessFailedCount;
  private @Metric MutableRate processLatency;

  protected ReconOmTaskMetrics(String taskName, String source) {
    this.taskName = taskName;
    this.source = source;
  }

  /**
   * Update the number of reprocess() method calls by the task
   */
  public void incrTaskReprocessCount() {
   this.taskReprocessCount.incr();
  }

  /**
   * Update the number of times reprocess() method call encountered exception
   */
  public void incrTaskReprocessFailureCount() {
    this.taskReprocessFailedCount.incr();
  }

  /**
   * Update the time taken by one call of reprocess()
   * @param time The amount of time that was taken to reprocess
   */
  public void updateTaskReprocessLatency(long time) {
    this.reprocessLatency.add(time);
  }

  /**
   * Update the number of process() method calls by the task
   */
  public void incrTaskProcessCount() {
    this.taskProcessCount.incr();
  }

  /**
   * Update the number of times process() method call encountered exception
   */
  public void incrTaskProcessFailureCount() {
    this.taskProcessFailedCount.incr();
  }

  /**
   * Updated the time taken by one call of process()
   * @param time The amount of time taken to process
   */
  public void updateTaskProcessLatency(long time) {
    this.processLatency.add(time);
  }

  @VisibleForTesting
  public long getTaskReprocessCount() {
    return this.taskReprocessCount.value();
  }

  @VisibleForTesting
  public long getTaskReprocessFailureCount() {
    return this.taskReprocessFailedCount.value();
  }

  @VisibleForTesting
  public long getTaskProcessCount() {
    return this.taskProcessCount.value();

  }

  @VisibleForTesting
  public long getTaskProcessFailureCount() {
    return this.taskProcessFailedCount.value();
  }

  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(source);
    builder.add(new MetricsTag(
        Interns.info("taskName", "ReconOmTask Name"), taskName));
    builder.endRecord();
    registry.snapshot(builder, all);
  }
}
