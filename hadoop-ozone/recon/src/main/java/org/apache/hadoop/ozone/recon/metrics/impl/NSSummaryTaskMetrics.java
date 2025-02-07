package org.apache.hadoop.ozone.recon.metrics.impl;

import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.metrics2.lib.MutableRate;
import org.apache.hadoop.ozone.recon.metrics.ReconOmTaskMetrics;

@Metrics(about="Metrics for NSSummary task", context="recon")
public class NSSummaryTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = NSSummaryTaskMetrics.class.getSimpleName();
  private static final MetricsInfo TASK_INFO = Interns.info(
      "ReconTaskMetrics", "Task metrics for NSSummary"
  );

  NSSummaryTaskMetrics() {
    super(TASK_INFO, "NSSummaryTask", SOURCE_NAME);
  }

  public static NSSummaryTaskMetrics register() {
    return DefaultMetricsSystem.instance().register(
        SOURCE_NAME,
        "NSSummaryTask metrics",
        new NSSummaryTaskMetrics()
    );
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }

  // Metrics relating to processing/reprocessing of FSO based NSSummary task
  private @Metric MutableCounterLong reprocessFSOCount;
  private @Metric MutableCounterLong reprocessFSOFailureCount;
  private @Metric MutableRate reprocessFSOLatency;
  private @Metric MutableCounterLong processFSOCount;
  private @Metric MutableCounterLong processFSOFailureCount;
  private @Metric MutableRate processFSOLatency;
  private @Metric MutableCounterLong fsoPutKeyEventCount;
  private @Metric MutableCounterLong fsoDeleteKeyEventCount;
  private @Metric MutableCounterLong fsoUpdateKeyEventCount;
  private @Metric MutableCounterLong fsoPutDirEventCount;
  private @Metric MutableCounterLong fsoDeleteDirEventCount;
  private @Metric MutableCounterLong fsoUpdateDirEventCount;

  public void incrReprocessFSOCount() {
    this.reprocessFSOCount.incr();
  }

  public void incrReprocessFSOFailureCount() {
    this.reprocessFSOFailureCount.incr();
  }

  public void updateReprocessFSOLatency(long time) {
    this.reprocessFSOLatency.add(time);
  }

  public void incrProcessFSOCount() {
    this.processFSOCount.incr();
  }

  public void incrProcessFSOFailureCount() {
    this.processFSOFailureCount.incr();
  }

  public void updateProcessFSOLatency(long time) {
    this.processFSOLatency.add(time);
  }

  public void incrFSOPutKeyEventCount() {
    this.fsoPutKeyEventCount.incr();
  }

  public void incrFSODeleteKeyEventCount() {
    this.fsoDeleteKeyEventCount.incr();
  }

  public void incrFSOUpdateKeyEventCount() {
    this.fsoUpdateKeyEventCount.incr();
  }

  public void incrFSOPutDirEventCount() {
    this.fsoPutDirEventCount.incr();
  }

  public void incrFSODeleteDirEventCount() {
    this.fsoDeleteDirEventCount.incr();
  }

  public void incrFSOUpdateDirEventCount() {
    this.fsoUpdateDirEventCount.incr();
  }


  // Metrics relating to processing/reprocessing of OBS based NSSummary task
  private @Metric MutableCounterLong reprocessOBSCount;
  private @Metric MutableCounterLong reprocessOBSFailureCount;
  private @Metric MutableRate reprocessOBSLatency;
  private @Metric MutableCounterLong processOBSCount;
  private @Metric MutableCounterLong processOBSFailureCount;
  private @Metric MutableRate processOBSLatency;
  private @Metric MutableCounterLong obsPutKeyEventCount;
  private @Metric MutableCounterLong obsDeleteKeyEventCount;
  private @Metric MutableCounterLong obsUpdateKeyEventCount;

  public void incrReprocessOBSCount() {
    this.reprocessOBSCount.incr();
  }

  public void incrReprocessOBSFailureCount() {
    this.reprocessOBSFailureCount.incr();
  }

  public void updateReprocessOBSLatency(long time) {
    this.reprocessOBSLatency.add(time);
  }

  public void incrProcessOBSCount() {
    this.processOBSCount.incr();
  }

  public void incrProcessOBSFailureCount() {
    this.processOBSFailureCount.incr();
  }

  public void updateProcessOBSLatency(long time) {
    this.processOBSLatency.add(time);
  }

  public void incrOBSPutKeyEventCount() {
    this.obsPutKeyEventCount.incr();
  }

  public void incrOBSDeleteKeyEventCount() {
    this.obsDeleteKeyEventCount.incr();
  }

  public void incrOBSUpdateKeyEventCount() {
    this.obsUpdateKeyEventCount.incr();
  }

  // Metrics relating to processing/reprocessing of Legacy based NSSummary task
  private @Metric MutableCounterLong reprocessLegacyCount;
  private @Metric MutableCounterLong reprocessLegacyFailureCount;
  private @Metric MutableRate reprocessLegacyLatency;
  private @Metric MutableCounterLong processLegacyCount;
  private @Metric MutableCounterLong processLegacyFailureCount;
  private @Metric MutableRate processLegacyLatency;
  private @Metric MutableCounterLong legacyPutKeyEventCount;
  private @Metric MutableCounterLong legacyDeleteKeyEventCount;
  private @Metric MutableCounterLong legacyUpdateKeyEventCount;
  private @Metric MutableCounterLong legacyPutDirEventCount;
  private @Metric MutableCounterLong legacyUpdateDirEventCount;
  private @Metric MutableCounterLong legacyDeleteDirEventCount;

  public void incrReprocessLegacyCount() {
    this.reprocessLegacyCount.incr();
  }

  public void incrReprocessLegacyFailureCount() {
    this.reprocessLegacyFailureCount.incr();
  }

  public void updateReprocessLegacyLatency(long time) {
    this.reprocessLegacyLatency.add(time);
  }

  public void incrProcessLegacyCount() {
    this.processLegacyCount.incr();
  }

  public void incrProcessLegacyFailureCount() {
    this.processLegacyFailureCount.incr();
  }

  public void updateProcessLegacyLatency(long time) {
    this.processLegacyLatency.add(time);
  }

  public void incrLegacyPutKeyEventCount() {
    this.legacyPutKeyEventCount.incr();
  }

  public void incrLegacyDeleteKeyEventCount() {
    this.legacyDeleteKeyEventCount.incr();
  }

  public void incrLegacyUpdateKeyEventCount() {
    this.legacyUpdateKeyEventCount.incr();
  }

  public void incrLegacyPutDirEventCount() {
    this.legacyPutDirEventCount.incr();
  }

  public void incrLegacyDeleteDirEventCount() {
    this.legacyDeleteDirEventCount.incr();
  }

  public void incrLegacyUpdateDirEventCount() {
    this.legacyUpdateDirEventCount.incr();
  }

  private @Metric MutableCounterLong taskWriteToDbCount;
  private @Metric MutableCounterLong taskWriteToDbFailureCount;
  private @Metric MutableRate writeToDBLatency;

  public void incrTaskWriteToDbCount() {
    this.taskWriteToDbCount.incr();
  }

  public void incrTaskWriteToDbFailureCount() {
    this.taskWriteToDbFailureCount.incr();
  }

  public void updateWriteToDBLatency(long time) {
    this.writeToDBLatency.add(time);
  }
}
