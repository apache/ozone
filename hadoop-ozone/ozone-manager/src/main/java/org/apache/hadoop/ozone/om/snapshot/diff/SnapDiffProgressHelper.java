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

package org.apache.hadoop.ozone.om.snapshot.diff;

import java.util.function.DoubleConsumer;

/**
 * Shared 10% checkpoint reporting for snapshot-diff scan stages.
 */
public final class SnapDiffProgressHelper {

  private static final double TEN_PCT_STEP = 0.1;

  private SnapDiffProgressHelper() {
  }

  static void reportSlice(DoubleConsumer progress, double sliceStart, double sliceEnd,
      double completedFraction) {
    if (progress == null) {
      return;
    }
    double width = sliceEnd - sliceStart;
    progress.accept(sliceStart + width * Math.min(1.0, Math.max(0.0, completedFraction)));
  }

  static void reportEveryTenPercentInSlice(DoubleConsumer progress, double sliceStart, double sliceEnd,
      long processedUnits, long totalUnits, TenPercentCheckpoint checkpoint) {
    if (progress == null) {
      return;
    }
    long denominator = Math.max(1L, totalUnits);
    double fraction = (double) processedUnits / denominator;
    while (checkpoint.nextCheckpoint <= 1.0 && fraction >= checkpoint.nextCheckpoint) {
      reportSlice(progress, sliceStart, sliceEnd, checkpoint.nextCheckpoint);
      checkpoint.nextCheckpoint += TEN_PCT_STEP;
    }
  }

  static final class TenPercentCheckpoint {
    private double nextCheckpoint = TEN_PCT_STEP;
  }
}
