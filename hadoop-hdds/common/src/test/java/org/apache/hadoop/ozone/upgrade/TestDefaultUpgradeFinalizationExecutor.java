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

package org.apache.hadoop.ozone.upgrade;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.ozone.common.Storage;
import org.apache.ozone.test.LambdaTestUtils;
import org.junit.jupiter.api.Test;

/**
 * Test for DefaultUpgradeFinalizationExecutor.
 */
public class TestDefaultUpgradeFinalizationExecutor {

  @Test
  public void testPreFinalizeFailureThrowsException() throws Exception {
    AbstractLayoutVersionManager mockLvm =
        mock(AbstractLayoutVersionManager.class);
    when(mockLvm.needsFinalization()).thenReturn(true);

    BasicUpgradeFinalizer uf = new BasicUpgradeFinalizer(mockLvm) {
      @Override
      protected void preFinalizeUpgrade(Object service) throws IOException {
        throw new IOException("Failure!");
      }

      @Override
      protected void postFinalizeUpgrade(Object service) {
      }

      @Override
      public void finalizeLayoutFeature(LayoutFeature layoutFeatture,
          Object service) {
      }

      @Override
      public void runPrefinalizeStateActions(Storage storage, Object service) {
      }
    };

    DefaultUpgradeFinalizationExecutor executor =
        new DefaultUpgradeFinalizationExecutor();
    LambdaTestUtils.intercept(IOException.class,
        "Failure!", () -> executor.execute(new Object(), uf));
  }

  @Test
  public void testPostFinalizeFailureDoesNotThrowException() throws Exception {
    AbstractLayoutVersionManager mockLvm =
        mock(AbstractLayoutVersionManager.class);
    when(mockLvm.needsFinalization()).thenReturn(false);

    BasicUpgradeFinalizer uf =
        new BasicUpgradeFinalizer(mockLvm) {
          @Override
          protected void preFinalizeUpgrade(Object service) {
          }

          @Override
          protected void postFinalizeUpgrade(Object service)
              throws IOException {
            throw new IOException("Failure!");
          }

          @Override
          public void finalizeLayoutFeature(LayoutFeature lf, Object service) {
          }

          @Override
          public void runPrefinalizeStateActions(Storage storage,
                                                 Object service) {
          }
        };

    DefaultUpgradeFinalizationExecutor executor =
        new DefaultUpgradeFinalizationExecutor();
    executor.execute(new Object(), uf);
  }
}
