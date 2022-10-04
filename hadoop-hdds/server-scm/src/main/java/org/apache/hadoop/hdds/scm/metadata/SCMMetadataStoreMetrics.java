/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.metadata;

import org.apache.commons.text.WordUtils;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Class contains metrics related to SCM Metadata Store.
 */
@Metrics(about = "SCM Metadata Store Metrics", context = OzoneConsts.OZONE)
public final class SCMMetadataStoreMetrics implements MetricsSource {

  public static final String METRICS_SOURCE_NAME =
      SCMMetadataStoreMetrics.class.getSimpleName();

  private static final MetricsInfo ESTIMATED_KEY_COUNT = Interns.info(
      "EstimatedKeyCount",
      "Tracked estimated key count of all column families");

  private static final Map<String, MetricsInfo> ESTIMATED_KEY_COUNT_METRICS
      = Collections.unmodifiableMap(new LinkedHashMap<String, MetricsInfo>() {{
          for (DBColumnFamilyDefinition<?, ?> table:
              SCMMetadataStoreImpl.COLUMN_FAMILIES) {
            String name = WordUtils.capitalize(table.getName());
            String metric = name + "EstimatedKeyCount";
            String description = "Estimated key count in table of " + name;
            put(table.getName(), Interns.info(metric, description));
          }
        }});

  private MetricsRegistry registry;

  private SCMMetadataStoreImpl scmMetadataStore;

  public SCMMetadataStoreMetrics(SCMMetadataStoreImpl scmMetadataStoreImpl) {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
    this.scmMetadataStore = scmMetadataStoreImpl;
  }

  public static SCMMetadataStoreMetrics create(SCMMetadataStoreImpl
      scmMetadataStore) {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "SCM Metadata store  related metrics",
        new SCMMetadataStoreMetrics(scmMetadataStore));
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(METRICS_SOURCE_NAME)
        .tag(ESTIMATED_KEY_COUNT, scmMetadataStore.getEstimatedKeyCountStr());

    for (Map.Entry<String, MetricsInfo> e :
        ESTIMATED_KEY_COUNT_METRICS.entrySet()) {
      long count = 0L;
      try {
        count = scmMetadataStore.getTableMap().get(e.getKey())
            .getEstimatedKeyCount();
      } catch (IOException exception) {
        // Ignore exception here.
      }
      builder.addGauge(e.getValue(), count);
    }


  }

  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

}
