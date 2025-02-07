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

  private @Metric MutableCounterLong reprocessCount;
  private @Metric MutableCounterLong reprocessFailedCount;
  private @Metric MutableRate reprocessLatency;

  private @Metric MutableCounterLong processCount;
  private @Metric MutableCounterLong processFailedCount;
  private @Metric MutableRate processLatency;

  protected ReconOmTaskMetrics(String taskName, String source) {
    this.taskName = taskName;
    this.source = source;
  }

  /**
   * Update the number of reprocess() method calls by the task
   */
  public void incrTaskReprocessCount() {
   this.reprocessCount.incr();
  }

  /**
   * Update the number of times reprocess() method call encountered exception
   */
  public void incrTaskReprocessFailureCount() {
    this.reprocessFailedCount.incr();
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
    this.processCount.incr();
  }

  /**
   * Update the number of times process() method call encountered exception
   */
  public void incrTaskProcessFailureCount() {
    this.processFailedCount.incr();
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
    return this.reprocessCount.value();
  }

  @VisibleForTesting
  public long getTaskProcessCount() {
    return this.processCount.value();

  }

  @VisibleForTesting
  public long getTaskProcessFailureCount() {
    return this.processFailedCount.value();
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
