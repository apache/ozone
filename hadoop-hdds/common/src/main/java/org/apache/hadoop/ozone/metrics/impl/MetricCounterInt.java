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

import org.apache.hadoop.ozone.metrics.AbstractMetric;
import org.apache.hadoop.ozone.metrics.MetricType;
import org.apache.hadoop.ozone.metrics.MetricsInfo;
import org.apache.hadoop.ozone.metrics.MetricsVisitor;

class MetricCounterInt extends AbstractMetric {
  final int value;

  MetricCounterInt(MetricsInfo info, int value) {
    super(info);
    this.value = value;
  }

  @Override
  public Integer value() {
    return value;
  }

  @Override
  public MetricType type() {
    return MetricType.COUNTER;
  }

  @Override
  public void visit(MetricsVisitor visitor) {
    visitor.counter(this, value);
  }
}
