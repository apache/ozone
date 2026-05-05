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

package org.apache.ratis.metrics.dropwizard3;

import com.codahale.metrics.MetricRegistry;
import java.util.function.Consumer;
import org.apache.ratis.metrics.RatisMetricRegistry;

/**
 * Utilities for ratis metrics dropwizard3.
 */
public interface RatisMetricsUtils {

  static MetricRegistry getDropWizardMetricRegistry(RatisMetricRegistry r) {
    return ((Dm3RatisMetricRegistryImpl) r).getDropWizardMetricRegistry();
  }

  static Consumer<RatisMetricRegistry> jmxReporter() {
    return Dm3MetricsReporting.jmxReporter();
  }

  static Consumer<RatisMetricRegistry> stopJmxReporter() {
    return Dm3MetricsReporting.stopJmxReporter();
  }
}
