/*
 package org.apache.hadoop.ozone.upgrade;
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
package org.apache.hadoop.ozone.upgrade;

import org.apache.hadoop.ozone.upgrade.InjectedUpgradeFinalizationExecutor.UpgradeTestInjectionPoints;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.ozone.common.StorageInfo;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

/**
 * Upgrade related test utility methods.
 */
public final class UpgradeTestUtils {
  private UpgradeTestUtils() { }

  /**
   * Creates a VERSION file for the specified node type under the directory
   * {@code parentDir}.
   */
  public static File createVersionFile(File parentDir,
      HddsProtos.NodeType nodeType, int mlv) throws IOException {

    final String versionFileName = "VERSION";

    StorageInfo info = new StorageInfo(
        nodeType,
        UUID.randomUUID().toString(),
        System.currentTimeMillis(),
        mlv);

    File versionFile = new File(parentDir, versionFileName);
    info.writeTo(versionFile);

    return versionFile;
  }

  /**
   * @param haltingPoint Where to halt finalization in the returned
   *     executor's {@code execute} method.
   * @param pauseLatch The latch that will be counted down 1 by the
   *     executor when the upgrade finalization has been paused.
   * @param unpauseLatch The latch that the caller should count down to
   *     resume upgrade finalization.
   * @param log Where to log messages about pausing and resuming finalization.
   * @return A new InjectedUpgradeFinalizationExecutor
   */
  public static <T> InjectedUpgradeFinalizationExecutor<T>
      newPausingFinalizationExecutor(UpgradeTestInjectionPoints haltingPoint,
      CountDownLatch pauseLatch, CountDownLatch unpauseLatch, Logger log) {
    InjectedUpgradeFinalizationExecutor<T>
        executor = new InjectedUpgradeFinalizationExecutor<>();
    executor.configureTestInjectionFunction(haltingPoint, () -> {
      log.info("Halting upgrade finalization at point: {}", haltingPoint);
      try {
        pauseLatch.countDown();
        unpauseLatch.await();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new IOException("SCM test finalization interrupted.", ex);
      }
      log.info("Upgrade finalization resumed from point: {}", haltingPoint);
      return false;
    });

    return executor;
  }

  /**
   * @param haltingPoint Where to halt finalization in the returned
   *     executor's {@code execute} method.
   * @param terminateLatch The latch that will be counted down 1 by the
   *    executor when the upgrade finalization has been terminated.
   * @param log Where to log messages about pausing and resuming finalization.
   * @return A new InjectedUpgradeFinalizationExecutor
   */
  public static <T> InjectedUpgradeFinalizationExecutor<T>
      newTerminatingFinalizationExecutor(
          UpgradeTestInjectionPoints haltingPoint,
      CountDownLatch terminateLatch, Logger log) {
    InjectedUpgradeFinalizationExecutor<T>
        executor =
        new InjectedUpgradeFinalizationExecutor<>();
    executor.configureTestInjectionFunction(haltingPoint, () -> {
      log.info("Terminating upgrade finalization at point: {}. This is " +
          "expected test execution.", haltingPoint);
      terminateLatch.countDown();
      return true;
    });

    return executor;
  }
}
