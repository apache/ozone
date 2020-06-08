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
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;

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

}