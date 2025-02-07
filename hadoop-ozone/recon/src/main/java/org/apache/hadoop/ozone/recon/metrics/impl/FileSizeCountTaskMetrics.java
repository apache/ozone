package org.apache.hadoop.ozone.recon.metrics.impl;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.recon.metrics.ReconOmTaskMetrics;

@Metrics(about="Metrics for FileSizeCount task", context="recon")
public class FileSizeCountTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = FileSizeCountTaskMetrics.class.getSimpleName();
  private static FileSizeCountTaskMetrics instance;

  FileSizeCountTaskMetrics() {
    super("FileSizeCountTask", SOURCE_NAME);
  }

  public static synchronized FileSizeCountTaskMetrics register() {
    if (null == instance) {
      instance = DefaultMetricsSystem.instance().register(
          SOURCE_NAME,
          "FileSizeCountTask metrics",
          new FileSizeCountTaskMetrics()
      );
    }
    return  instance;
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

  public void updateWriteToDBLatency(long time) {
    this.writeToDBLatency.add(time);
  }
}
