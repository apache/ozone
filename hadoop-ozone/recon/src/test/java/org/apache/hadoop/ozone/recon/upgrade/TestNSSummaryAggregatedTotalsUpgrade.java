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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.inject.Injector;
import javax.sql.DataSource;
import org.apache.hadoop.ozone.recon.ReconGuiceServletContextListener;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskController;
import org.apache.hadoop.ozone.recon.tasks.ReconTaskReInitializationEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

/**
 * Test class for NSSummaryAggregatedTotalsUpgrade.
 */
public class TestNSSummaryAggregatedTotalsUpgrade {

  private NSSummaryAggregatedTotalsUpgrade upgradeAction;
  private DataSource mockDataSource;
  private Injector mockInjector;
  private ReconTaskController mockReconTaskController;

  @BeforeEach
  public void setUp() {
    upgradeAction = new NSSummaryAggregatedTotalsUpgrade();
    mockDataSource = mock(DataSource.class);
    mockInjector = mock(Injector.class);
    mockReconTaskController = mock(ReconTaskController.class);
  }

  @Test
  public void testExecuteSuccessfulReinitialization() throws Exception {
    try (MockedStatic<ReconGuiceServletContextListener> mockedListener = 
         Mockito.mockStatic(ReconGuiceServletContextListener.class)) {
      
      mockedListener.when(ReconGuiceServletContextListener::getGlobalInjector)
          .thenReturn(mockInjector);
      
      when(mockInjector.getInstance(ReconTaskController.class))
          .thenReturn(mockReconTaskController);
      
      when(mockReconTaskController.queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER))
          .thenReturn(ReconTaskController.ReInitializationResult.SUCCESS);

      assertDoesNotThrow(() -> upgradeAction.execute(mockDataSource));

      verify(mockInjector).getInstance(ReconTaskController.class);
      verify(mockReconTaskController).queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    }
  }

  @Test
  public void testExecuteFailedReinitializationRetryLater() throws Exception {
    try (MockedStatic<ReconGuiceServletContextListener> mockedListener = 
         Mockito.mockStatic(ReconGuiceServletContextListener.class)) {
      
      mockedListener.when(ReconGuiceServletContextListener::getGlobalInjector)
          .thenReturn(mockInjector);
      
      when(mockInjector.getInstance(ReconTaskController.class))
          .thenReturn(mockReconTaskController);
      
      when(mockReconTaskController.queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER))
          .thenReturn(ReconTaskController.ReInitializationResult.RETRY_LATER);

      assertDoesNotThrow(() -> upgradeAction.execute(mockDataSource));

      verify(mockInjector).getInstance(ReconTaskController.class);
      verify(mockReconTaskController).queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    }
  }

  @Test
  public void testExecuteFailedReinitializationMaxRetriesExceeded() throws Exception {
    try (MockedStatic<ReconGuiceServletContextListener> mockedListener = 
         Mockito.mockStatic(ReconGuiceServletContextListener.class)) {
      
      mockedListener.when(ReconGuiceServletContextListener::getGlobalInjector)
          .thenReturn(mockInjector);
      
      when(mockInjector.getInstance(ReconTaskController.class))
          .thenReturn(mockReconTaskController);
      
      when(mockReconTaskController.queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER))
          .thenReturn(ReconTaskController.ReInitializationResult.MAX_RETRIES_EXCEEDED);

      assertDoesNotThrow(() -> upgradeAction.execute(mockDataSource));

      verify(mockInjector).getInstance(ReconTaskController.class);
      verify(mockReconTaskController).queueReInitializationEvent(
          ReconTaskReInitializationEvent.ReInitializationReason.MANUAL_TRIGGER);
    }
  }

  @Test
  public void testExecuteWithNullInjector() {
    try (MockedStatic<ReconGuiceServletContextListener> mockedListener = 
         Mockito.mockStatic(ReconGuiceServletContextListener.class)) {
      
      mockedListener.when(ReconGuiceServletContextListener::getGlobalInjector)
          .thenReturn(null);

      IllegalStateException exception = assertThrows(IllegalStateException.class, 
          () -> upgradeAction.execute(mockDataSource));

      assert exception.getMessage().contains(
          "Guice injector not initialized. NSSummary rebuild cannot proceed during upgrade.");
    }
  }

  @Test
  public void testExecuteWithInjectorException() throws Exception {
    try (MockedStatic<ReconGuiceServletContextListener> mockedListener = 
         Mockito.mockStatic(ReconGuiceServletContextListener.class)) {
      
      mockedListener.when(ReconGuiceServletContextListener::getGlobalInjector)
          .thenReturn(mockInjector);
      
      when(mockInjector.getInstance(ReconTaskController.class))
          .thenThrow(new RuntimeException("Injector failed to provide instance"));

      assertThrows(RuntimeException.class, () -> upgradeAction.execute(mockDataSource));

      verify(mockInjector).getInstance(ReconTaskController.class);
    }
  }

  @Test
  public void testGetType() {
    assert upgradeAction.getType() == ReconUpgradeAction.UpgradeActionType.FINALIZE;
  }
}
