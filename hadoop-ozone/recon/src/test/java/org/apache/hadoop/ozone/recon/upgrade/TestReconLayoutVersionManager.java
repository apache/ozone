package org.apache.hadoop.ozone.recon.upgrade;

import org.apache.hadoop.ozone.recon.ReconSchemaVersionTableManager;
import org.junit.jupiter.api.*;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Tests for ReconLayoutVersionManager.
 */
public class TestReconLayoutVersionManager {

  private ReconSchemaVersionTableManager schemaVersionTableManager;
  private ReconLayoutVersionManager layoutVersionManager;
  private static MockedStatic<ReconLayoutFeature> mockedEnum;

  @BeforeEach
  public void setUp() {
    schemaVersionTableManager = mock(ReconSchemaVersionTableManager.class);
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(0);

    // Mocking ReconLayoutFeature.values() to return custom enum instances
    mockedEnum = mockStatic(ReconLayoutFeature.class);
    ReconLayoutFeature feature1 = mock(ReconLayoutFeature.class);
    when(feature1.getVersion()).thenReturn(1);
    when(feature1.getUpgradeAction()).thenReturn(() -> {
      // No-op for testing
    });
    ReconLayoutFeature feature2 = mock(ReconLayoutFeature.class);
    when(feature2.getVersion()).thenReturn(2);
    when(feature2.getUpgradeAction()).thenReturn(() -> {
      // No-op for testing
    });

    // Define the custom features to be returned
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature1, feature2});

    layoutVersionManager = new ReconLayoutVersionManager(schemaVersionTableManager);
  }

  @AfterEach
  public void tearDown() {
    // Close the static mock after each test to deregister it
    mockedEnum.close();
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
  public void testFinalizeLayoutFeaturesWithMockedValues() {
    layoutVersionManager.finalizeLayoutFeatures();

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
  public void testNoLayoutFeatures() {
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{});
    layoutVersionManager.finalizeLayoutFeatures();
    verify(schemaVersionTableManager, never()).updateSchemaVersion(anyInt());
  }

  /**
   * Handling Invalid Versions
   * Higher MLV than Available SLV: Test the behavior when the current MLV (Metadata Layout Version)
   * is set higher than the maximum version of ReconLayoutFeature. This scenario simulates
   * an inconsistent state where the system’s metadata versioning is beyond the known layout features.
   */
  @Test
  public void testHigherMLVThanSLV() {
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(5);
    layoutVersionManager = new ReconLayoutVersionManager(schemaVersionTableManager);
    assertEquals(2, layoutVersionManager.getCurrentSLV()); // Assuming 2 is the max SLV in mocked features
    assertEquals(5, layoutVersionManager.getCurrentMLV());
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
    doThrow(new RuntimeException("Upgrade failed")).when(action1).execute();
    when(feature1.getUpgradeAction()).thenReturn(action1);

    // Mock the static values method to return the custom feature
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature1});

    // Execute the layout feature finalization
    layoutVersionManager.finalizeLayoutFeatures();
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
    when(feature1.getUpgradeAction()).thenReturn(action1);

    ReconLayoutFeature feature2 = mock(ReconLayoutFeature.class);
    when(feature2.getVersion()).thenReturn(2);
    ReconUpgradeAction action2 = mock(ReconUpgradeAction.class);
    when(feature2.getUpgradeAction()).thenReturn(action2);

    ReconLayoutFeature feature3 = mock(ReconLayoutFeature.class);
    when(feature3.getVersion()).thenReturn(3);
    ReconUpgradeAction action3 = mock(ReconUpgradeAction.class);
    when(feature3.getUpgradeAction()).thenReturn(action3);

    // Mock the static values method to return custom features in a jumbled order
    mockedEnum.when(ReconLayoutFeature::values).thenReturn(new ReconLayoutFeature[]{feature2, feature3, feature1});

    // Execute the layout feature finalization
    layoutVersionManager.finalizeLayoutFeatures();

    // Verify that the actions were executed in the correct order using InOrder
    InOrder inOrder = inOrder(action1, action2, action3);
    inOrder.verify(action1).execute(); // Should be executed first
    inOrder.verify(action2).execute(); // Should be executed second
    inOrder.verify(action3).execute(); // Should be executed third
  }

  /**
   * Tests the scenario where no upgrade actions are needed. Ensures that if the current
   * schema version matches the maximum layout version, no upgrade actions are executed.
   */
  @Test
  public void testNoUpgradeActionsNeeded() {
    when(schemaVersionTableManager.getCurrentSchemaVersion()).thenReturn(2);
    layoutVersionManager = new ReconLayoutVersionManager(schemaVersionTableManager);
    layoutVersionManager.finalizeLayoutFeatures();

    verify(schemaVersionTableManager, never()).updateSchemaVersion(anyInt());
  }

}