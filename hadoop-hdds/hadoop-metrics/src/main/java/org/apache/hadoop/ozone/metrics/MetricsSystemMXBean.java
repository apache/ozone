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

package org.apache.hadoop.ozone.metrics;

/**
 * The JMX interface to the metrics system
 */
public interface MetricsSystemMXBean {
  /**
   * Start the metrics system
   * @throws MetricsException Metrics Exception.
   */
  public void start();

  /**
   * Stop the metrics system
   * @throws MetricsException Metrics Exception.
   */
  public void stop();

  /**
   * Start metrics MBeans
   * @throws MetricsException Metrics Exception.
   */
  public void startMetricsMBeans();

  /**
   * Stop metrics MBeans.
   * Note, it doesn't stop the metrics system control MBean,
   * i.e this interface.
   * @throws MetricsException Metrics Exception.
   */
  public void stopMetricsMBeans();

  /**
   * @return the current config
   * Avoided getConfig, as it'll turn into a "Config" attribute,
   * which doesn't support multiple line values in jconsole.
   * @throws MetricsException Metrics Exception.
   */
  public String currentConfig();
}
