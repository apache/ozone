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
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.ratis.metrics.MetricRegistries;
import org.apache.ratis.metrics.MetricsReporting;
import org.apache.ratis.metrics.RatisMetricRegistry;

import java.util.Map;

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

  public static List<MetricReporter> registerRatisMetricReporters(
      Map<String, RatisDropwizardExports> ratisMetricsMap,
      BooleanSupplier checkStopped) {
    //All the Ratis metrics (registered from now) will be published via JMX and
    //via the prometheus exporter (used by the /prom servlet
    List<MetricReporter> ratisReporterList = new ArrayList<>();
    ratisReporterList.add(new MetricReporter(MetricsReporting.jmxReporter(),
        MetricsReporting.stopJmxReporter()));
    Consumer<RatisMetricRegistry> reporter
        = r1 -> registerDropwizard(r1, ratisMetricsMap, checkStopped);
    Consumer<RatisMetricRegistry> stopper
        = r2 -> deregisterDropwizard(r2, ratisMetricsMap);
    ratisReporterList.add(new MetricReporter(reporter, stopper));
    
    for (MetricReporter metricReporter : ratisReporterList) {
      metricReporter.addToGlobalRegistration();
    }
    return ratisReporterList;
  }

  public static void clear(
      Map<String, RatisDropwizardExports> ratisMetricsMap,
      List<MetricReporter> ratisReporterList) {
    ratisMetricsMap.entrySet().stream().forEach(e -> {
      // remove and deregister from registry only one registered
      // as unregistered element if performed unregister again will
      // cause null pointer exception by registry
      Collector c = ratisMetricsMap.remove(e.getKey());
      if (c != null) {
        CollectorRegistry.defaultRegistry.unregister(c);
      }
    });
    
    if (null != ratisReporterList) {
      for (MetricReporter metricReporter : ratisReporterList) {
        metricReporter.removeFromGlobalRegistration();
      }
    }
    
    MetricRegistries.global().clear();
  }

  private static void registerDropwizard(RatisMetricRegistry registry,
      Map<String, RatisDropwizardExports> ratisMetricsMap,
      BooleanSupplier checkStopped) {
    if (checkStopped.getAsBoolean()) {
      return;
    }
    
    RatisDropwizardExports rde = new RatisDropwizardExports(
        registry.getDropWizardMetricRegistry());
    String name = registry.getMetricRegistryInfo().getName();
    if (null == ratisMetricsMap.putIfAbsent(name, rde)) {
      // new rde is added for the name, so need register
      CollectorRegistry.defaultRegistry.register(rde);
    }
  }

  private static void deregisterDropwizard(RatisMetricRegistry registry,
      Map<String, RatisDropwizardExports> ratisMetricsMap) {
    String name = registry.getMetricRegistryInfo().getName();
    Collector c = ratisMetricsMap.remove(name);
    if (c != null) {
      CollectorRegistry.defaultRegistry.unregister(c);
    }
  }

  /**
   * class for keeping track of reporters and add/remove to registry.
   * 
   */
  public static class MetricReporter {
    private final Consumer<RatisMetricRegistry> reporter;
    private final Consumer<RatisMetricRegistry> stopper;

    MetricReporter(Consumer<RatisMetricRegistry> reporter,
                        Consumer<RatisMetricRegistry> stopper) {
      this.reporter = reporter;
      this.stopper = stopper;
    }

    void addToGlobalRegistration() {
      MetricRegistries.global().addReporterRegistration(reporter, stopper);
    }

    void removeFromGlobalRegistration() {
      MetricRegistries.global().removeReporterRegistration(reporter, stopper);
    }
  }
}