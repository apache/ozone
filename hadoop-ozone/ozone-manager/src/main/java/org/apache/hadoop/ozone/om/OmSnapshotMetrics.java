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
package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.MutableCounterLong;
import org.apache.ratis.util.MemoizedSupplier;

import java.util.function.Supplier;

/**
 * This class is for maintaining Snapshot Manager statistics.
 */
@InterfaceAudience.Private
@Metrics(about = "Snapshot Manager Metrics", context = "dfs")
public final class OmSnapshotMetrics implements OmMetadataReaderMetrics {
  private static final String SOURCE_NAME =
      OmSnapshotMetrics.class.getSimpleName();

  private OmSnapshotMetrics() {
  }

  private static final Supplier<OmSnapshotMetrics> SUPPLIER =
      MemoizedSupplier.valueOf(() -> {
        MetricsSystem ms = DefaultMetricsSystem.instance();
        return ms.register(SOURCE_NAME,
            "Snapshot Manager Metrics",
            new OmSnapshotMetrics());
      });

  public static OmSnapshotMetrics getInstance() {
    return SUPPLIER.get();
  }

  private @Metric
      MutableCounterLong numKeyLookup;
  private @Metric
      MutableCounterLong numKeyLookupFails;

  @Override
  public void incNumKeyLookups() {
    numKeyOps.incr();
    numKeyLookup.incr();
  }

  @Override
  public void incNumKeyLookupFails() {
    numKeyLookupFails.incr();
  }

  private @Metric
      MutableCounterLong numGetKeyInfo;
  private @Metric
      MutableCounterLong numGetKeyInfoFails;

  @Override
  public void incNumGetKeyInfo() {
    numKeyOps.incr();
    numGetKeyInfo.incr();
  }

  @Override
  public void incNumGetKeyInfoFails() {
    numGetKeyInfoFails.incr();
  }

  private @Metric
      MutableCounterLong numListStatus;
  private @Metric
      MutableCounterLong numListStatusFails;

  @Override
  public void incNumListStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numListStatus.incr();
  }

  @Override
  public void incNumListStatusFails() {
    numListStatusFails.incr();
  }

  private @Metric
      MutableCounterLong numGetFileStatus;
  private @Metric
      MutableCounterLong numGetFileStatusFails;

  @Override
  public void incNumGetFileStatus() {
    numKeyOps.incr();
    numFSOps.incr();
    numGetFileStatus.incr();
  }

  @Override
  public void incNumGetFileStatusFails() {
    numGetFileStatusFails.incr();
  }

  private @Metric
      MutableCounterLong numLookupFile;
  private @Metric
      MutableCounterLong numLookupFileFails;

  @Override
  public void incNumLookupFile() {
    numKeyOps.incr();
    numFSOps.incr();
    numLookupFile.incr();
  }

  @Override
  public void incNumLookupFileFails() {
    numLookupFileFails.incr();
  }

  private @Metric
      MutableCounterLong numKeyLists;

  private @Metric
      MutableCounterLong numKeyListFails;

  @Override
  public void incNumKeyLists() {
    numKeyLists.incr();
  }

  @Override
  public void incNumKeyListFails() {
    numKeyListFails.incr();
  }

  private @Metric
      MutableCounterLong numGetAcl;

  @Override
  public void incNumGetAcl() {
    numGetAcl.incr();
  }

  private @Metric
      MutableCounterLong numKeyOps;
  private @Metric
      MutableCounterLong numFSOps;
}

