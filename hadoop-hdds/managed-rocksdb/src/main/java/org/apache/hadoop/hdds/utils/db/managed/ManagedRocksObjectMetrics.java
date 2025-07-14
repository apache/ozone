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

package org.apache.hadoop.hdds.utils.db.managed;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.hadoop.ozone.OzoneConsts;

/**
 * Metrics about managed RockObjects.
 */
@InterfaceAudience.Private
@Metrics(about = "Ozone Manage RocksObject Metrics",
    context = OzoneConsts.OZONE)
public class ManagedRocksObjectMetrics {
  public static final ManagedRocksObjectMetrics INSTANCE = create();

  private static final String SOURCE_NAME =
      ManagedRocksObjectMetrics.class.getSimpleName();

  @Metric(about = "Total number of managed RocksObjects that are not " +
      "closed before being GCed.")
  private MutableCounterLong totalLeakObjects;

  @Metric(about = "Total Number of managed RocksObjects.")
  private MutableCounterLong totalManagedObjects;

  void increaseLeakObject() {
    totalLeakObjects.incr();
  }

  public void assertNoLeaks() {
    final long cnt = totalLeakObjects.value();
    if (cnt > 0) {
      throw new AssertionError("Found " + cnt + " leaked objects, check logs");
    }
  }

  void increaseManagedObject() {
    totalManagedObjects.incr();
  }

  @VisibleForTesting
  long totalLeakObjects() {
    return totalLeakObjects.value();
  }

  @VisibleForTesting
  long totalManagedObjects() {
    return totalManagedObjects.value();
  }

  private static ManagedRocksObjectMetrics create() {
    return DefaultMetricsSystem.instance().register(SOURCE_NAME,
        "OzoneManager DoubleBuffer Metrics",
        new ManagedRocksObjectMetrics());
  }
}
