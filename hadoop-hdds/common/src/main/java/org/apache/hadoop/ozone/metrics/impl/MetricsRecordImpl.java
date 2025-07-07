/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.metrics.impl;

import java.util.List;

import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.ozone.metrics.AbstractMetric;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsTag;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.hadoop.ozone.metrics.util.Contracts.checkArg;

@VisibleForTesting
public class MetricsRecordImpl extends AbstractMetricsRecord {
  protected static final String DEFAULT_CONTEXT = "default";

  private final long timestamp;
  private final MetricsInfo info;
  private final List<MetricsTag> tags;
  private final Iterable<AbstractMetric> metrics;

  /**
   * Construct a metrics record
   * @param info  {@link MetricsInfo} of the record
   * @param timestamp of the record
   * @param tags  of the record
   * @param metrics of the record
   */
  public MetricsRecordImpl(MetricsInfo info, long timestamp,
                           List<MetricsTag> tags,
                           Iterable<AbstractMetric> metrics) {
    this.timestamp = checkArg(timestamp, timestamp > 0, "timestamp");
    this.info = checkNotNull(info, "info");
    this.tags = checkNotNull(tags, "tags");
    this.metrics = checkNotNull(metrics, "metrics");
  }

  @Override public long timestamp() {
    return timestamp;
  }

  @Override public String name() {
    return info.name();
  }

  MetricsInfo info() {
    return info;
  }

  @Override public String description() {
    return info.description();
  }

  @Override public String context() {
    // usually the first tag
    for (MetricsTag t : tags) {
      if (t.info() == MsInfo.Context) {
        return t.value();
      }
    }
    return DEFAULT_CONTEXT;
  }

  @Override
  public List<MetricsTag> tags() {
    return tags; // already unmodifiable from MetricsRecordBuilderImpl#tags
  }

  @Override public Iterable<AbstractMetric> metrics() {
    return metrics;
  }
}
