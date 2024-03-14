package org.apache.hadoop.hdds.scm.container.placement.metrics;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.*;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.Time;

@InterfaceAudience.Private
@Metrics(about = "SCM Performance Metrics", context = OzoneConsts.OZONE)
public final class SCMPerformanceMetrics implements MetricsSource {
  private static final String SOURCE_NAME =
      SCMPerformanceMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static SCMPerformanceMetrics instance;

  @Metric private MutableCounterLong deleteKeyFailure;
  @Metric private MutableCounterLong deleteKeySuccess;
  @Metric(about = "Latency for deleteKey failure in nanoseconds")
  private MutableRate deleteKeyFailureLatencyNs;
  @Metric(about = "Latency for deleteKey success in nanoseconds")
  private MutableRate deleteKeySuccessLatencyNs;

  public SCMPerformanceMetrics() {
    this.registry = new MetricsRegistry(SOURCE_NAME);
  }

  public static SCMPerformanceMetrics create() {
    if (instance != null) {
      return instance;
    }
    MetricsSystem ms = DefaultMetricsSystem.instance();
    instance = ms.register(SOURCE_NAME, "SCM Performance Metrics",
        new SCMPerformanceMetrics());
    return instance;
  }

  public void unRegister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder recordBuilder = collector.addRecord(SOURCE_NAME);
    deleteKeySuccess.snapshot(recordBuilder, true);
    deleteKeySuccessLatencyNs.snapshot(recordBuilder, true);
    deleteKeyFailure.snapshot(recordBuilder, true);
    deleteKeyFailureLatencyNs.snapshot(recordBuilder, true);
  }

  public void updateDeleteKeySuccessStats(long startNanos) {
    deleteKeySuccess.incr();
    deleteKeySuccessLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }

  public void updateDeleteKeyFailureStats(long startNanos) {
    deleteKeyFailure.incr();
    deleteKeyFailureLatencyNs.add(Time.monotonicNowNanos() - startNanos);
  }
}

