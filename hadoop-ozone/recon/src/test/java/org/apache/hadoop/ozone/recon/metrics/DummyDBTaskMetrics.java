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

package org.apache.hadoop.ozone.recon.metrics;

import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

/**
 * This class contains extra metrics for Dummy task used in Recon task based unit tests.
 */
@Metrics(about = "Metrics for DummyDB task", context = "recon")
public class DummyDBTaskMetrics extends ReconOmTaskMetrics {
  private static final String SOURCE_NAME = DummyDBTaskMetrics.class.getSimpleName();

  DummyDBTaskMetrics() {
    super("DummyDBTask", SOURCE_NAME);
  }

  public static DummyDBTaskMetrics register() {
    return DefaultMetricsSystem.instance().register(
        SOURCE_NAME,
        "DummyDBTask metrics",
        new DummyDBTaskMetrics()
    );
  }

  public void unregister() {
    DefaultMetricsSystem.instance().unregisterSource(SOURCE_NAME);
  }
}
