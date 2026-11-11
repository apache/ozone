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

package org.apache.hadoop.ozone.recon.upgrade;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.inject.Injector;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskReInitializationEvent.ReInitializationReason;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for OpenKeysExcludeMultipartUploadsUpgradeAction.
 */
@ExtendWith(MockitoExtension.class)
public class TestOpenKeysExcludeMultipartUploadsUpgradeAction {

  private final OpenKeysExcludeMultipartUploadsUpgradeAction upgradeAction =
      new OpenKeysExcludeMultipartUploadsUpgradeAction();
  @Mock
  private DataSource mockDataSource;
  @Mock
  private Injector mockInjector;
  @Mock
  private ReconTaskController mockReconTaskController;

  @Test
  public void testExecuteQueuesTaskReinitialization() throws Exception {
    try (MockedStatic<ReconGuiceServletContextListener> mockStaticContext =
             mockStatic(ReconGuiceServletContextListener.class)) {
      mockStaticContext.when(ReconGuiceServletContextListener::getGlobalInjector).thenReturn(mockInjector);
      when(mockInjector.getInstance(ReconTaskController.class)).thenReturn(mockReconTaskController);
      when(mockReconTaskController.queueReInitializationEvent(ReInitializationReason.MANUAL_TRIGGER))
          .thenReturn(ReconTaskController.ReInitializationResult.SUCCESS);

      upgradeAction.execute(mockDataSource);

      verify(mockReconTaskController, times(1)).queueReInitializationEvent(ReInitializationReason.MANUAL_TRIGGER);
    }
  }

  @Test
  public void testExecuteDoesNotThrowWhenQueueingFails() {
    try (MockedStatic<ReconGuiceServletContextListener> mockStaticContext =
             mockStatic(ReconGuiceServletContextListener.class)) {
      mockStaticContext.when(ReconGuiceServletContextListener::getGlobalInjector).thenReturn(mockInjector);
      when(mockInjector.getInstance(ReconTaskController.class)).thenReturn(mockReconTaskController);
      when(mockReconTaskController.queueReInitializationEvent(ReInitializationReason.MANUAL_TRIGGER))
          .thenReturn(ReconTaskController.ReInitializationResult.RETRY_LATER);

      assertDoesNotThrow(() -> upgradeAction.execute(mockDataSource));

      verify(mockReconTaskController, times(1)).queueReInitializationEvent(ReInitializationReason.MANUAL_TRIGGER);
    }
  }

  @Test
  public void testExecuteFailsWithoutInjector() {
    try (MockedStatic<ReconGuiceServletContextListener> mockStaticContext =
             mockStatic(ReconGuiceServletContextListener.class)) {
      mockStaticContext.when(ReconGuiceServletContextListener::getGlobalInjector).thenReturn(null);

      assertThrows(IllegalStateException.class, () -> upgradeAction.execute(mockDataSource));
    }
  }
}
