package org.apache.hadoop.ozone.recon.metrics;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;

import java.util.List;

/**
 * Ship ReconTaskStatus table on persistent DB as a metrics.
 */
@Singleton
@Metrics(about = "Recon Task Status Metrics", context = OzoneConsts.OZONE)
public class ReconTaskStatusMetrics implements MetricsSource {

  private static final String SOURCE_NAME =
      ReconTaskStatusMetrics.class.getSimpleName();

  @Inject
  private ReconTaskStatusDao reconTaskStatusDao;

  private static final MetricsInfo RECORD_INFO_LAST_UPDATED_TS =
      Interns.info("lastUpdatedTimestamp",
          "Last updated timestamp of corresponding Recon Task");

  private static final MetricsInfo RECORD_INFO_LAST_UPDATED_SEQ =
      Interns.info("lastUpdatedSeqNumber",
          "Last updated sequence number of corresponding Recon Task");

  public void register() {
    DefaultMetricsSystem.instance()
        .register(SOURCE_NAME, "Recon Task Metrics", this);
  }

  public void unregister() {
    DefaultMetricsSystem.instance()
        .unregisterSource(SOURCE_NAME);
  }

  public void getMetrics(MetricsCollector collector, boolean all) {
    List<ReconTaskStatus> rows = reconTaskStatusDao.findAll();
    rows.forEach((rts) -> {
      MetricsRecordBuilder builder = collector.addRecord(SOURCE_NAME);
      builder.add(
          new MetricsTag(
              Interns.info("type", "Recon Task type"),
              rts.getTaskName()));
      builder.addGauge(RECORD_INFO_LAST_UPDATED_TS,
          rts.getLastUpdatedTimestamp());
      builder.addGauge(RECORD_INFO_LAST_UPDATED_SEQ,
          rts.getLastUpdatedSeqNumber());
      builder.endRecord();
    });
  }
}