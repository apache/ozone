/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.server.http;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricsReporting;
import org.apache.ratis.metrics.RatisMetricRegistry;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Collect Dropwizard metrics, but rename ratis specific metrics.
 */
public class RatisDropwizardExports extends DropwizardExports {

  /**
   * Creates a new DropwizardExports with a {@link DefaultSampleBuilder}.
   *
   * @param registry a metric registry to export in prometheus.
   */
  public RatisDropwizardExports(MetricRegistry registry) {
    super(registry, new RatisNameRewriteSampleBuilder());
  }

  public static void registerRatisMetricReporters(
      AtomicReference<RatisDropwizardExports> ratisDropwizardExports) {
    //All the Ratis metrics (registered from now) will be published via JMX and
    //via the prometheus exporter (used by the /prom servlet
    MetricRegistries.global()
        .addReporterRegistration(MetricsReporting.jmxReporter(),
            MetricsReporting.stopJmxReporter());
    MetricRegistries.global().addReporterRegistration(
        r1 -> registerDropwizard(r1, ratisDropwizardExports),
        r2 -> deregisterDropwizard(r2, ratisDropwizardExports));
  }

  private static void registerDropwizard(RatisMetricRegistry registry,
    AtomicReference<RatisDropwizardExports> ratisDropwizardExports) {
    RatisDropwizardExports rde = new RatisDropwizardExports(
        registry.getDropWizardMetricRegistry());
    CollectorRegistry.defaultRegistry.register(rde);
    Preconditions.checkArgument(
        ratisDropwizardExports.compareAndSet(null, rde));
  }

  private static void deregisterDropwizard(RatisMetricRegistry registry,
    AtomicReference<RatisDropwizardExports> ratisDropwizardExports) {
    Collector c = ratisDropwizardExports.getAndSet(null);
    if (c != null) {
      CollectorRegistry.defaultRegistry.unregister(c);
      ratisDropwizardExports.set(null);
    }
  }
}