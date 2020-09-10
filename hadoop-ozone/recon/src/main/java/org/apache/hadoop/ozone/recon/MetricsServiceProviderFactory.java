/*
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

package org.apache.hadoop.ozone.recon;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.recon.ReconConfigKeys;
import org.apache.hadoop.ozone.recon.spi.MetricsServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.PrometheusServiceProviderImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;

/**
 * Factory class that is used to get the instance of configured Metrics Service
 * Provider.
 */
@Singleton
public class MetricsServiceProviderFactory {

  private static final Logger LOG =
      LoggerFactory.getLogger(MetricsServiceProviderFactory.class);

  private OzoneConfiguration configuration;
  private ReconUtils reconUtils;

  @Inject
  public MetricsServiceProviderFactory(OzoneConfiguration configuration,
                                       ReconUtils reconUtils) {
    this.configuration = configuration;
    this.reconUtils = reconUtils;
  }

  /**
   * Returns the configured MetricsServiceProvider implementation (defaults
   * to prometheus).
   * If no metrics service providers are configured, returns null.
   *
   * @return MetricsServiceProvider instance that is configured.
   */
  public MetricsServiceProvider getMetricsServiceProvider() {
    String prometheusEndpoint = getPrometheusEndpoint();
    if (StringUtils.isNotEmpty(prometheusEndpoint)) {
      if (LOG.isInfoEnabled()) {
        LOG.info(
            String.format("Choosing Prometheus as Metrics service provider " +
                "with configured endpoint: %s", prometheusEndpoint));
      }
      return new PrometheusServiceProviderImpl(configuration, reconUtils);
    }
    return null;
  }

  /**
   * Returns the Prometheus endpoint if configured. Otherwise returns null.
   *
   * @return Prometheus endpoint if configured, null otherwise.
   */
  private String getPrometheusEndpoint() {
    String endpoint = configuration.getTrimmed(
        ReconConfigKeys.OZONE_RECON_PROMETHEUS_HTTP_ENDPOINT);
    // Remove the trailing slash from endpoint url.
    if (endpoint != null && endpoint.endsWith("/")) {
      endpoint = endpoint.substring(0, endpoint.length() - 1);
    }
    return endpoint;
  }
}
