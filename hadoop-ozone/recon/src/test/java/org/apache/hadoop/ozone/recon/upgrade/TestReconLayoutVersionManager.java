/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.ReconContext;
import org.apache.hadoop.ozone.recon.ReconSchemaVersionTableManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;


/**
 * Tests for ReconLayoutVersionManager.
 */
public class TestReconLayoutVersionManager {

  private ReconSchemaVersionTableManager schemaVersionTableManager;
  private ReconLayoutVersionManager layoutVersionManager;
  private MockedStatic<ReconLayoutFeature> mockedEnum;
  private MockedStatic<ReconUpgradeAction.UpgradeActionType> mockedEnumUpgradeActionType;

  @BeforeEach
  public void setUp() throws SQLException {
    schemaVersionTableManager = mock(ReconSchemaVersionTableManager.class);
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(0);

    // Mocking ReconLayoutFeature.values() to return custom enum instances
    mockedEnum = mockStatic(ReconLayoutFeature.class);
    mockedEnumUpgradeActionType = mockStatic(ReconUpgradeAction.UpgradeActionType.class);

    ReconLayoutFeature feature1 = mock(ReconLayoutFeature.class);
    when(feature1.getVersion()).thenReturn(1);
    ReconUpgradeAction action1 = mock(ReconUpgradeAction.class);
    when(feature1.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action1));

    ReconLayoutFeature feature2 = mock(ReconLayoutFeature.class);
    when(feature2.getVersion()).thenReturn(2);
    ReconUpgradeAction action2 = mock(ReconUpgradeAction.class);
    when(feature2.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action2));

    // Define the custom features to be returned
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature1, feature2});

    layoutVersionManager = new ReconLayoutVersionManager(schemaVersionTableManager, mock(ReconContext.class));
  }

  @AfterEach
  public void tearDown() {
    // Close the static mock after each test to deregister it
    mockedEnum.close();
    if (mockedEnumUpgradeActionType != null) {
      mockedEnumUpgradeActionType.close();
    }
  }

  /**
   * Tests the initialization of layout version manager to ensure
   * that the MLV (Metadata Layout Version) is set correctly to 0,
   * and SLV (Software Layout Version) reflects the maximum available version.
   */
  @Test
  public void testInitializationWithMockedValues() {
    assertEquals(0, layoutVersionManager.getCurrentMLV());
    assertEquals(2, layoutVersionManager.getCurrentSLV());
  }

  /**
   * Tests the finalization of layout features and ensure that the updateSchemaVersion for
   * the schemaVersionTable is triggered for each feature version.
   */
  @Test
  public void testFinalizeLayoutFeaturesWithMockedValues() throws SQLException {
    layoutVersionManager.finalizeLayoutFeatures(mock(
        ReconStorageContainerManagerFacade.class));

    // Verify that schema versions are updated for our custom features
    verify(schemaVersionTableManager, times(1)).updateSchemaVersion(1);
    verify(schemaVersionTableManager, times(1)).updateSchemaVersion(2);
  }

  /**
   * Tests the retrieval of registered features to ensure that the correct
   * layout features are returned according to the mocked values.
   */
  @Test
  public void testGetRegisteredFeaturesWithMockedValues() {
    // Fetch the registered features
    List<ReconLayoutFeature> registeredFeatures = layoutVersionManager.getRegisteredFeatures();

    // Verify that the registered features match the mocked ones
    ReconLayoutFeature feature1 = ReconLayoutFeature.values()[0];
    ReconLayoutFeature feature2 = ReconLayoutFeature.values()[1];
    List<ReconLayoutFeature> expectedFeatures = Arrays.asList(feature1, feature2);
    assertEquals(expectedFeatures, registeredFeatures);
  }

  /**
   * Tests the scenario where no layout features are present. Ensures that no schema
   * version updates are attempted when there are no features to finalize.
   */
  @Test
  public void testNoLayoutFeatures() throws SQLException {
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{});
    layoutVersionManager.finalizeLayoutFeatures(mock(
        ReconStorageContainerManagerFacade.class));
    verify(schemaVersionTableManager, never()).updateSchemaVersion(anyInt());
  }

  /**
   * Tests the scenario where an upgrade action fails. Ensures that if an upgrade action
   * throws an exception, the schema version is not updated.
   */
  @Test
  public void testUpgradeActionFailure() throws Exception {
    // Reset existing mocks and set up new features for this specific test
    mockedEnum.reset();

    // Mock ReconLayoutFeature instances
    ReconLayoutFeature feature1 = mock(ReconLayoutFeature.class);
    when(feature1.getVersion()).thenReturn(1);
    ReconUpgradeAction action1 = mock(ReconUpgradeAction.class);

    // Simulate an exception being thrown during the upgrade action execution
    doThrow(new RuntimeException("Upgrade failed")).when(action1).execute(mock(
        ReconStorageContainerManagerFacade.class));
    when(feature1.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action1));

    // Mock the static values method to return the custom feature
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature1});

    // Execute the layout feature finalization
    try {
      layoutVersionManager.finalizeLayoutFeatures(mock(
          ReconStorageContainerManagerFacade.class));
    } catch (Exception e) {
    }
    // Verify that schema version update was never called due to the exception
    verify(schemaVersionTableManager, never()).updateSchemaVersion(anyInt());
  }

  /**
   * Tests the order of execution for the upgrade actions to ensure that
   * they are executed sequentially according to their version numbers.
   */
  @Test
  public void testUpgradeActionExecutionOrder() throws Exception {
    // Reset the existing static mock for this specific test
    mockedEnum.reset();

    // Mock ReconLayoutFeature instances
    ReconLayoutFeature feature1 = mock(ReconLayoutFeature.class);
    when(feature1.getVersion()).thenReturn(1);
    ReconUpgradeAction action1 = mock(ReconUpgradeAction.class);
    when(feature1.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action1));

    ReconLayoutFeature feature2 = mock(ReconLayoutFeature.class);
    when(feature2.getVersion()).thenReturn(2);
    ReconUpgradeAction action2 = mock(ReconUpgradeAction.class);
    when(feature2.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action2));

    ReconLayoutFeature feature3 = mock(ReconLayoutFeature.class);
    when(feature3.getVersion()).thenReturn(3);
    ReconUpgradeAction action3 = mock(ReconUpgradeAction.class);
    when(feature3.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action3));

    // Mock the static values method to return custom features in a jumbled order
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature2, feature3, feature1});

    // Execute the layout feature finalization
    layoutVersionManager.finalizeLayoutFeatures(mock(
        ReconStorageContainerManagerFacade.class));

    // Verify that the actions were executed in the correct order using InOrder
    InOrder inOrder = inOrder(action1, action2, action3);
    inOrder.verify(action1).execute(mock(ReconStorageContainerManagerFacade.class)); // Should be executed first
    inOrder.verify(action2).execute(mock(ReconStorageContainerManagerFacade.class)); // Should be executed second
    inOrder.verify(action3).execute(mock(ReconStorageContainerManagerFacade.class)); // Should be executed third
  }

  /**
   * Tests the scenario where no upgrade actions are needed. Ensures that if the current
   * schema version matches the maximum layout version, no upgrade actions are executed.
   */
  @Test
  public void testNoUpgradeActionsNeeded() throws SQLException {
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(2);
    layoutVersionManager = new ReconLayoutVersionManager(schemaVersionTableManager, mock(ReconContext.class));
    layoutVersionManager.finalizeLayoutFeatures(mock(
        ReconStorageContainerManagerFacade.class));

    verify(schemaVersionTableManager, never()).updateSchemaVersion(anyInt());
  }

  /**
   * Tests the scenario where the first two features are finalized,
   * and then a third feature is introduced. Ensures that only the
   * newly introduced feature is finalized while the previously
   * finalized features are skipped.
   */
  @Test
  public void testFinalizingNewFeatureWithoutReFinalizingPreviousFeatures() throws Exception {
    // Step 1: Finalize the first two features.
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(0);

    // Mock the first two features.
    ReconLayoutFeature feature1 = mock(ReconLayoutFeature.class);
    when(feature1.getVersion()).thenReturn(1);
    ReconUpgradeAction action1 = mock(ReconUpgradeAction.class);
    when(feature1.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action1));

    ReconLayoutFeature feature2 = mock(ReconLayoutFeature.class);
    when(feature2.getVersion()).thenReturn(2);
    ReconUpgradeAction action2 = mock(ReconUpgradeAction.class);
    when(feature2.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action2));

    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature1, feature2});

    // Finalize the first two features.
    layoutVersionManager.finalizeLayoutFeatures(mock(
        ReconStorageContainerManagerFacade.class));

    // Verify that the schema versions for the first two features were updated.
    verify(schemaVersionTableManager, times(1)).updateSchemaVersion(1);
    verify(schemaVersionTableManager, times(1)).updateSchemaVersion(2);

    // Step 2: Introduce a new feature (Feature 3).
    ReconLayoutFeature feature3 = mock(ReconLayoutFeature.class);
    when(feature3.getVersion()).thenReturn(3);
    ReconUpgradeAction action3 = mock(ReconUpgradeAction.class);
    when(feature3.getAction(ReconUpgradeAction.UpgradeActionType.FINALIZE))
        .thenReturn(Optional.of(action3));

    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature1, feature2, feature3});

    // Update schema version to simulate that features 1 and 2 have already been finalized.
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(2);

    // Finalize again, but only feature 3 should be finalized.
    layoutVersionManager.finalizeLayoutFeatures(mock(
        ReconStorageContainerManagerFacade.class));

    // Verify that the schema version for feature 3 was updated.
    verify(schemaVersionTableManager, times(1)).updateSchemaVersion(3);

    // Verify that action1 and action2 were not executed again.
    verify(action1, times(1)).execute(mock(ReconStorageContainerManagerFacade.class));
    verify(action2, times(1)).execute(mock(ReconStorageContainerManagerFacade.class));

    // Verify that the upgrade action for feature 3 was executed.
    verify(action3, times(1)).execute(mock(ReconStorageContainerManagerFacade.class));
  }

}
