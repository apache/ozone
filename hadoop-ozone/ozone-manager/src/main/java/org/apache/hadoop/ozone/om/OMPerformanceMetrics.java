package org.apache.hadoop.ozone.om;

import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableRate;

public class OMPerformanceMetrics {
  private static final String SOURCE_NAME =
      OMPerformanceMetrics.class.getSimpleName();

  public static OMPerformanceMetrics register() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    return ms.register(SOURCE_NAME,
            "OzoneManager Latency",
            new OMPerformanceMetrics());
  }

  public static void unregister() {
    MetricsSystem ms = DefaultMetricsSystem.instance();
    ms.unregisterSource(SOURCE_NAME);
  }

  @Metric(about = "Overall lookupKey in nanoseconds")
  private MutableRate lookupKeyLatencyNs;

  @Metric(about = "Read key info from meta in nanoseconds")
  private MutableRate readKeyInfoLatencyNs;

  @Metric(about = "Block token generation latency in nanoseconds")
  private MutableRate blockTokenLatencyNs;

  @Metric(about = "Refresh location nanoseconds")
  private MutableRate refreshLatencyNs;

  @Metric(about = "ACLs check nanoseconds")
  private MutableRate aclCheckLatencyNs;

  @Metric(about = "s3VolumeInfo latency nanoseconds")
  private MutableRate s3VolumeInfoLatencyNs;

  @Metric(about = "resolveBucketLink latency nanoseconds")
  private MutableRate resolveBucketLinkLatencyNs;

  public void addLookupKeyLatency(long latencyInNs) {
    lookupKeyLatencyNs.add(latencyInNs);
  }

  public void addBlockTokenLatency(long latencyInNs) {
    blockTokenLatencyNs.add(latencyInNs);
  }

  public void addRefreshLatency(long latencyInNs) {
    refreshLatencyNs.add(latencyInNs);
  }

  public void addAckCheckLatency(long latencyInNs) {
    aclCheckLatencyNs.add(latencyInNs);
  }

  public void addReadKeyInfoLatency(long latencyInNs) {
    readKeyInfoLatencyNs.add(latencyInNs);
  }

  public void addS3VolumeInfoLatencyNs(long latencyInNs) {
    s3VolumeInfoLatencyNs.add(latencyInNs);
  }

  public void addResolveBucketLinkLatencyNs(long latencyInNs) {
    resolveBucketLinkLatencyNs.add(latencyInNs);
  }
}
