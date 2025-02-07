package org.apache.hadoop.ozone.recon.metrics;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;

/**
 * This interface is to be implemented by the various metrics for different tasks
 */
public abstract class ReconOmTaskMetrics implements MetricsSource {

  private final MetricsRegistry registry = new MetricsRegistry("ReconOMTasks");
  private final MetricsInfo mInfo;
  private final String taskName;
  private final String source;

  private @Metric MutableCounterLong taskReprocessCount;
  private @Metric MutableCounterLong taskReprocessFailedCount;
  private @Metric MutableRate reprocessLatency;

  private @Metric MutableCounterLong taskProcessCount;
  private @Metric MutableCounterLong taskProcessFailedCount;
  private @Metric MutableRate processLatency;

  protected ReconOmTaskMetrics(MetricsInfo info, String taskName, String source) {
    this.mInfo = info;
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


  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    registry.snapshot(collector.addRecord(registry.info()), all);
    MetricsRecordBuilder builder = collector.addRecord(source);
    builder.tag(mInfo, taskName);
  }
}
