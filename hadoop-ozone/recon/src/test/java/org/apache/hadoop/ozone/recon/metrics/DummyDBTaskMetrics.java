package org.apache.hadoop.ozone.recon.metrics;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;

@Metrics(about="Metrics for DummyDB task", context="recon")
public class DummyDBTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = DummyDBTaskMetrics.class.getSimpleName();
  private static final MetricsInfo TASK_INFO = Interns.info(
      "ReconTaskMetrics", "Task metrics for DummyDB"
  );

  DummyDBTaskMetrics() {
    super("DummyDBTask", SOURCE_NAME);
  }

  public static DummyDBTaskMetrics register() {
    return DefaultMetricsSystem.instance().register(
        SOURCE_NAME,
        "DummyDBTask metrics",
        new DummyDBTaskMetrics()
    );
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }
}
