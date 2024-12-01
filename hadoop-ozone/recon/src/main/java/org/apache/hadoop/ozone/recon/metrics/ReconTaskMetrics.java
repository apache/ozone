package org.apache.hadoop.ozone.recon.metrics;


import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.utils.MetricsUtil;
import org.apache.hadoop.metrics2.MetricsRecord;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.*;
import org.apache.hadoop.metrics2.util.Metrics2Util;
import org.apache.hadoop.ozone.OzoneConsts;

@InterfaceAudience.Private
@Metrics(about = "Recon Tasks Metrics", context = OzoneConsts.OZONE)
public final class ReconTaskMetrics {

  private static ReconTaskMetrics metricsInstance;
  private static final String SOURCE_NAME =
    ReconTaskMetrics.class.getSimpleName();

  private final MetricsRegistry taskMetricsRegistry;

  private ReconTaskMetrics() {
    this.taskMetricsRegistry = new MetricsRegistry(SOURCE_NAME);
  }

  public void register() {
    if (null == metricsInstance) {
      metricsInstance = DefaultMetricsSystem.instance().register(
        SOURCE_NAME,
        "Recon Task Metrics", new ReconTaskMetrics());
    }
  }

  public void unregister() {
    if (null == metricsInstance) {
      throw new IllegalStateException("Metrics class is not yet used");
    }
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
    metricsInstance = null;
  }

  // ContainerHealthTask metrics
  @Metric(about = "Number of Container Health Tasks started by Recon.")
  private MutableCounterLong numContainerHealthTasks;
  @Metric(about = "Number of Container Health Tasks executed by Recon that failed.")
  private MutableCounterLong numContainerHealthTasksFailed;
  private static long lastContainerHealthTaskTime;
  private static long lastContainerHealthTaskFailureTime;

  public void incrContainerHealthTask() {
    if (null == numContainerHealthTasks) {
      numContainerHealthTasks = taskMetricsRegistry.newCounter(
        Interns.info("containerHealthTasks", "Number of Container Health tasks started by Recon"),
        0L
      );
    }
    numContainerHealthTasks.incr();
    lastContainerHealthTaskTime = System.currentTimeMillis();
  }

  public void incrContainerHealthTaskFailures() {
    if (null == numContainerHealthTasks) {
      numContainerHealthTasksFailed = taskMetricsRegistry.newCounter(
        Interns.info("containerHealthTaskFailures", "Number of Container Health tasks failed."),
        0L
      );
    }
    numContainerHealthTasksFailed.incr();
    lastContainerHealthTaskFailureTime = System.currentTimeMillis();
  }

  public long getContainerHealthTaskCount() { return this.numContainerHealthTasks.value(); }
  public long getContainerHealthTaskFailures() { return this.numContainerHealthTasksFailed.value(); }
  public Pair<Long, Long> getLastContainerHealthTimes() { return Pair.of(lastContainerHealthTaskTime, lastContainerHealthTaskFailureTime); }

  //ContainerKeyMapperTask metrics
  @Metric(about = "Number of Container Key-Mapper Tasks executed by Recon.")
  private MutableCounterLong numContainerKeyMapperTask;
  @Metric(about = "Number of Container Key-Mapper Tasks executed by Recon that failed.")
  private MutableCounterLong numContainerKeyMapperTaskFailed;

  public void incrContainerKeyMapperTask() {
    if (null == numContainerKeyMapperTask) {
      numContainerKeyMapperTask = taskMetricsRegistry.newCounter(
        Interns.info("containerKeyMapperTasks",
          "Number of Container Key Mapper tasks started by Recon"),
        0L
      );
    }
    numContainerKeyMapperTask.incr();
  }

  public void incrContainerKeyMapperTaskFailure() {
    if (null == numContainerKeyMapperTaskFailed) {
      numContainerKeyMapperTaskFailed = taskMetricsRegistry.newCounter(
        Interns.info("containerKeyMapperTaskFailures",
          "Number of Container Key Mapper tasks failed."),
        0L
      );
    }
    numContainerKeyMapperTaskFailed.incr();
  }

  public long getContainerKeyMapperTaskCount() { return this.numContainerKeyMapperTask.value(); }
  public long getContainerKeyMapperTaskFailures() { return this.numContainerKeyMapperTaskFailed.value(); }

  //ContainerSizeCountTask metrics
  @Metric(about = "Number of Container Size Count Tasks executed by Recon.")
  private MutableCounterLong numContainerSizeCountTask;
  @Metric(about = "Number of Container Size Count Tasks executed by Recon that failed.")
  private MutableCounterLong numContainerSizeCountTasksFailed;

  public void incrContainerSizeCountTask() {
    if (null == numContainerSizeCountTask) {
      numContainerSizeCountTask = taskMetricsRegistry.newCounter(
        Interns.info("containerSizeCountTasks",
          "Number of Container Size Count tasks started by Recon"),
        0L
      );
    }
    numContainerSizeCountTask.incr();
  }

  public void incrContainerSizeCountTaskFailures() {
    if (null == numContainerSizeCountTasksFailed) {
      numContainerSizeCountTasksFailed = taskMetricsRegistry.newCounter(
        Interns.info("containerSizeCountTaskFailures",
          "Number of Container Size Count tasks failed."),
        0L
      );
    }
    numContainerSizeCountTasksFailed.incr();
  }

  public long getContainerSizeCountTaskCount() { return this.numContainerSizeCountTask.value(); }
  public long getContainerSizeCountTaskFailures() { return this.numContainerSizeCountTasksFailed.value(); }

  //FileSizeCountTask metrics
  @Metric(about = "Number of File Size Count Tasks executed by Recon.")
  private MutableCounterLong numFileSizeCountTasks;
  @Metric(about = "Number of File Size Count Tasks executed by Recon that failed.")
  private MutableCounterLong numFileSizeCountTasksFailed;

  public void incrFileSizeCountTask() {
    if (null == numFileSizeCountTasks) {
      numFileSizeCountTasks = taskMetricsRegistry.newCounter(
        Interns.info("fileSizeCountTasks",
          "Number of File Size Count tasks started by Recon"),
        0L
      );
    }
    numFileSizeCountTasks.incr();
  }

  public void incrFileSizeCountTaskFailures() {
    if (null == numFileSizeCountTasksFailed) {
      numFileSizeCountTasksFailed = taskMetricsRegistry.newCounter(
        Interns.info("fileSizeCountTaskFailures",
          "Number of File Size Count tasks failed."),
        0L
      );
    }
    numFileSizeCountTasksFailed.incr();
  }

  public long getFileSizeCountTaskCount() { return this.numFileSizeCountTasks.value(); }
  public long getFileSizeCountTaskFailures() { return this.numFileSizeCountTasksFailed.value(); }

  //NSSummaryTask metrics
  @Metric(about = "Number of Namespace Summary Tasks executed by Recon.")
  private MutableCounterLong numNSSummaryTasks;
  @Metric(about = "Number of Namespace Summary Tasks executed by Recon that failed.")
  private MutableCounterLong numNSSummaryTasksFailed;

  public void incrNSSummaryTask() {
    if (null == numNSSummaryTasks) {
      numNSSummaryTasks = taskMetricsRegistry.newCounter(
        Interns.info("nsSummaryTasks",
          "Number of Namespace Summary Tasks started by Recon"),
        0L
      );
    }
    numNSSummaryTasks.incr();
  }

  public void incrNSSummaryTaskFailures() {
    if (null == numNSSummaryTasksFailed) {
      numNSSummaryTasksFailed = taskMetricsRegistry.newCounter(
        Interns.info("nsSummaryTaskFailures",
          "Number of Namespace Summary tasks failed."),
        0L
      );
    }
    numNSSummaryTasksFailed.incr();
  }

  public long getNsSummaryTaskCount() { return this.numNSSummaryTasks.value(); }
  public long getNsSummaryTaskFailures() { return this.numNSSummaryTasksFailed.value(); }


  //OMTableInsightTask metrics
  @Metric(about = "Number of OM Table Insight Tasks executed by Recon.")
  private MutableCounterLong numOmTableInsightTasks;
  @Metric(about = "Number of OM Table Insight Tasks executed by Recon that failed.")
  private MutableCounterLong numOmTableInsightTasksFailed;

  public void incrOmTableInsightTask() {
    if (null == numOmTableInsightTasks) {
      numOmTableInsightTasks = taskMetricsRegistry.newCounter(
        Interns.info("omTableInsightTasks",
          "Number of OM Table Insight tasks started by Recon"),
        0L
      );
    }
    numOmTableInsightTasks.incr();
  }

  public void incrOmTableInsightTaskFailures() {
    if (null == numOmTableInsightTasksFailed) {
      numOmTableInsightTasksFailed = taskMetricsRegistry.newCounter(
        Interns.info("omTableInsightTaskFailures",
          "Number of OM Table Insight tasks failed."),
        0L
      );
    }
    numOmTableInsightTasksFailed.incr();
  }

  public long getOmTableInsightTaskCount() { return this.numOmTableInsightTasks.value(); }
  public long getOmTableInsightTaskFailures() { return this.numOmTableInsightTasksFailed.value(); }

  //PipelineSyncTask metrics
  @Metric(about = "Number of Pipeline Sync Tasks executed by Recon.")
  private MutableCounterLong numPipelineSyncTasks;
  @Metric(about = "Number of Pipeline Sync Tasks executed by Recon that failed.")
  private MutableCounterLong numPipelineSyncTasksFailed;

  public void incrPipelineSyncTask() {
    if (null == numPipelineSyncTasks) {
      numPipelineSyncTasks = taskMetricsRegistry.newCounter(
        Interns.info("pipelineSyncTasks",
          "Number of Pipeline Sync tasks started by Recon"),
        0L
      );
    }
    numPipelineSyncTasks.incr();
  }

  public void incrPipelineSyncTaskFailures() {
    if (null == numPipelineSyncTasksFailed) {
      numPipelineSyncTasksFailed = taskMetricsRegistry.newCounter(
        Interns.info("pipelineSyncTaskFailures",
          "Number of Pipeline Sync tasks failed."),
        0L
      );
    }
    numPipelineSyncTasksFailed.incr();
  }

  public long getPipelineSyncTaskCount() { return this.numPipelineSyncTasks.value(); }
  public long getPipelineSyncTaskFailures() { return this.numPipelineSyncTasksFailed.value(); }
}
