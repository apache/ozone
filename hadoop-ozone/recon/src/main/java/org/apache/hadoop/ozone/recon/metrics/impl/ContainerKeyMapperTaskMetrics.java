package org.apache.hadoop.ozone.recon.metrics.impl;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.recon.metrics.ReconOmTaskMetrics;

@Metrics(about="Metrics for ContainerKeyMapper task", context="recon")
public class ContainerKeyMapperTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = ContainerKeyMapperTaskMetrics.class.getSimpleName();
  private static ContainerKeyMapperTaskMetrics instance;

  ContainerKeyMapperTaskMetrics() {
    super("ContainerKeyMapperTask", SOURCE_NAME);
  }

  public static synchronized ContainerKeyMapperTaskMetrics register() {
    if (null == instance) {
      instance = DefaultMetricsSystem.instance().register(
          SOURCE_NAME,
          "ContainerKeyMapperTask metrics",
          new ContainerKeyMapperTaskMetrics()
      );
    }
    return instance;
  }

  public void unregister() {
    if (null != instance) {
      DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
      instance = null;
    }
  }

  private @Metric MutableCounterLong putKeyEventCount;
  private @Metric MutableCounterLong deleteKeyEventCount;
  private @Metric MutableCounterLong updateKeyEventCount;

  private @Metric MutableCounterLong taskWriteToDBCount;
  private @Metric MutableCounterLong taskWriteToDBFailureCount;
  private @Metric MutableRate writeToDBLatency;

  public void incrPutKeyEventCount() {
    this.putKeyEventCount.incr();
  }

  public void incrDeleteKeyEventCount() {
    this.deleteKeyEventCount.incr();
  }

  public void incrUpdateKeyEventCount() {
    this.updateKeyEventCount.incr();
  }

  public void incrTaskWriteToDBCount() {
    this.taskWriteToDBCount.incr();
  }

  public void incrTaskWriteToDBFailureCount() {
    this.taskWriteToDBFailureCount.incr();
  }

  public void updateWriteToDBLatency(long time) {
    this.writeToDBLatency.add(time);
  }
}
