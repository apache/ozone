/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.recon.metrics;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.ozone.recon.schema.generated.tables.daos.ReconTaskStatusDao;
import org.apache.ozone.recon.schema.generated.tables.pojos.ReconTaskStatus;

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

  @Override
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
      builder.addCounter(RECORD_INFO_LAST_UPDATED_SEQ,
          rts.getLastUpdatedSeqNumber());
      builder.endRecord();
    });
  }
}
