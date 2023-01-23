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
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.ozone.OzoneConsts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hadoop.metrics2.lib.Interns.info;

/**
 * Class contains metrics related to SCM Metadata Store.
 */
@Metrics(about = "SCM Metadata Store Metrics", context = OzoneConsts.OZONE)
public final class SCMMetadataStoreMetrics implements MetricsSource {

  private static final Logger LOG =
      LoggerFactory.getLogger(SCMMetadataStoreMetrics.class);

  public static final String METRICS_SOURCE_NAME =
      SCMMetadataStoreMetrics.class.getSimpleName();

  private MetricsRegistry registry;
  private static SCMMetadataStoreMetrics instance;

  private SCMMetadataStoreImpl scmMetadataStore;

  private Map<String, MetricsInfo> columnFamilyMetrics;

  public SCMMetadataStoreMetrics(SCMMetadataStoreImpl scmMetadataStoreImpl) {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
    this.scmMetadataStore = scmMetadataStoreImpl;

    columnFamilyMetrics = scmMetadataStoreImpl.getTableMap().entrySet()
        .stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getMetricsInfo(e.getKey())));
  }

  public static synchronized SCMMetadataStoreMetrics create(SCMMetadataStoreImpl
      scmMetadataStore) {
    if (instance != null) {
      return instance;
    }
    instance = DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "SCM Metadata store related metrics",
        new SCMMetadataStoreMetrics(scmMetadataStore));
    return instance;
  }

  @Override
  public void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector.addRecord(METRICS_SOURCE_NAME);

    for (Map.Entry<String, MetricsInfo> entry: columnFamilyMetrics.entrySet()) {
      long count = 0L;
      try {
        count = scmMetadataStore.getTableMap().get(entry.getKey())
            .getEstimatedKeyCount();
      } catch (IOException e) {
        LOG.error("Can not get estimated key count for table {}",
            entry.getKey(), e);
      }
      builder.addGauge(entry.getValue(), count);
    }
  }

  public static synchronized void unRegister() {
    instance = null;
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  private MetricsInfo getMetricsInfo(String tableName) {
    String name = WordUtils.capitalize(tableName);
    String metric = name + "EstimatedKeyCount";
    String description = "Estimated key count in table of " + name;
    return info(metric, description);
  }
}
