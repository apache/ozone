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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.inject.Injector;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.spi.ReconNamespaceSummaryManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Test class for ReplicatedSizeOfFilesUpgradeAction.
 */
@ExtendWith(MockitoExtension.class)
public class TestReplicatedSizeOfFilesUpgradeAction {

  private ReplicatedSizeOfFilesUpgradeAction upgradeAction;
  @Mock
  private DataSource mockDataSource;
  @Mock
  private Injector mockInjector;
  @Mock
  private ReconNamespaceSummaryManager mockNsSummaryManager;
  @Mock
  private ReconOMMetadataManager mockOmMetadataManager;

  @BeforeEach
  public void setUp() {
    upgradeAction = new ReplicatedSizeOfFilesUpgradeAction();
  }

  @Test
  public void testExecuteSuccessfullyRebuildsNSSummary() {
    try (MockedStatic<ReconGuiceServletContextListener> mockStaticContext =
             mockStatic(ReconGuiceServletContextListener.class)) {
      mockStaticContext.when(ReconGuiceServletContextListener::getStaticInjector).thenReturn(mockInjector);
      when(mockInjector.getInstance(ReconNamespaceSummaryManager.class)).thenReturn(mockNsSummaryManager);
      when(mockInjector.getInstance(ReconOMMetadataManager.class)).thenReturn(mockOmMetadataManager);

      upgradeAction.execute(mockDataSource);

      // Verify that rebuildNSSummaryTree was called exactly once.
      verify(mockNsSummaryManager, times(1)).rebuildNSSummaryTree(mockOmMetadataManager);
    }
  }

  @Test
  public void testExecuteThrowsRuntimeExceptionOnRebuildFailure() {
    try (MockedStatic<ReconGuiceServletContextListener> mockStaticContext =
             mockStatic(ReconGuiceServletContextListener.class)) {
      mockStaticContext.when(ReconGuiceServletContextListener::getStaticInjector).thenReturn(mockInjector);
      when(mockInjector.getInstance(ReconNamespaceSummaryManager.class)).thenReturn(mockNsSummaryManager);
      when(mockInjector.getInstance(ReconOMMetadataManager.class)).thenReturn(mockOmMetadataManager);

      // Simulate a failure during the rebuild process
      doThrow(new RuntimeException("Simulated rebuild error")).when(mockNsSummaryManager)
          .rebuildNSSummaryTree(any(ReconOMMetadataManager.class));

      RuntimeException thrown = assertThrows(RuntimeException.class, () -> upgradeAction.execute(mockDataSource));
      assertEquals("Failed to rebuild NSSummary during upgrade", thrown.getMessage());
    }
  }

  @Test
  public void testGetTypeReturnsFinalize() {
    assertEquals(ReconUpgradeAction.UpgradeActionType.FINALIZE, upgradeAction.getType());
  }
}
