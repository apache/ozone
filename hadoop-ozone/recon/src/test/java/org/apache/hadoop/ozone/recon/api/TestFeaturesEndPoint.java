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

package org.apache.hadoop.ozone.recon.api;

import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_ENABLE_KEY;
import static org.apache.hadoop.hdds.recon.ReconConfigKeys.OZONE_RECON_HEATMAP_PROVIDER_KEY;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.getTestReconOmMetadataManager;
import static org.apache.hadoop.ozone.recon.OMMetadataManagerTestUtils.initializeNewOmMetadataManager;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.server.OzoneStorageContainerManager;
import org.apache.hadoop.ozone.recon.ReconTestInjector;
import org.apache.hadoop.ozone.recon.api.types.FeatureProvider;
import org.apache.hadoop.ozone.recon.recovery.ReconOMMetadataManager;
import org.apache.hadoop.ozone.recon.scm.ReconStorageContainerManagerFacade;
import org.apache.hadoop.ozone.recon.spi.StorageContainerServiceProvider;
import org.apache.hadoop.ozone.recon.spi.impl.OzoneManagerServiceProviderImpl;
import org.apache.hadoop.ozone.recon.spi.impl.StorageContainerServiceProviderImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Unit tests for APIs in FeaturesEndPoint.
 */
public class TestFeaturesEndPoint {

  @TempDir
  private Path temporaryFolder;
  private FeaturesEndpoint featuresEndPoint;
  private boolean isSetupDone = false;
  private OzoneConfiguration ozoneConfiguration;

  private void initializeInjector() throws Exception {
    ozoneConfiguration = new OzoneConfiguration();
    ReconOMMetadataManager reconOMMetadataManager = getTestReconOmMetadataManager(
        initializeNewOmMetadataManager(Files.createDirectory(
            temporaryFolder.resolve("JunitOmDBDir")).toFile()),
        Files.createDirectory(temporaryFolder.resolve("NewDir")).toFile());

    ReconTestInjector reconTestInjector =
        new ReconTestInjector.Builder(temporaryFolder.toFile())
            .withReconSqlDb()
            .withReconOm(reconOMMetadataManager)
            .withOmServiceProvider(mock(OzoneManagerServiceProviderImpl.class))
            // No longer using mock reconSCM as we need nodeDB in Facade
            //  to establish datanode UUID to hostname mapping
            .addBinding(OzoneStorageContainerManager.class,
                ReconStorageContainerManagerFacade.class)
            .withContainerDB()
            .addBinding(StorageContainerServiceProvider.class,
                mock(StorageContainerServiceProviderImpl.class))
            .addBinding(FeaturesEndpoint.class)
            .build();

    featuresEndPoint = reconTestInjector.getInstance(
        FeaturesEndpoint.class);
  }

  @BeforeEach
  public void setUp() throws Exception {
    // The following setup runs only once
    if (!isSetupDone) {
      initializeInjector();
      isSetupDone = true;
    }
  }

  @Test
  public void testGetDisabledFeaturesGreaterThanZero() {
    ozoneConfiguration.set(OZONE_RECON_HEATMAP_PROVIDER_KEY, "");
    FeatureProvider.initFeatureSupport(ozoneConfiguration);
    Response disabledFeatures = featuresEndPoint.getDisabledFeatures();
    List<FeatureProvider.Feature> allDisabledFeatures =
        (List<FeatureProvider.Feature>) disabledFeatures.getEntity();
    assertNotNull(allDisabledFeatures);
    assertThat(allDisabledFeatures.size()).isGreaterThan(0);
    assertEquals(FeatureProvider.Feature.HEATMAP.getFeatureName(),
        allDisabledFeatures.get(0).getFeatureName());
  }

  @Test
  public void testNoDisabledFeatures() {
    ozoneConfiguration.set(OZONE_RECON_HEATMAP_PROVIDER_KEY,
        "org.apache.hadoop.ozone.recon.heatmap.TestHeatMapProviderImpl");
    ozoneConfiguration.setBoolean(OZONE_RECON_HEATMAP_ENABLE_KEY, true);
    FeatureProvider.initFeatureSupport(ozoneConfiguration);
    Response disabledFeatures = featuresEndPoint.getDisabledFeatures();
    List<FeatureProvider.Feature> allDisabledFeatures =
        (List<FeatureProvider.Feature>) disabledFeatures.getEntity();
    assertNotNull(allDisabledFeatures);
    assertEquals(0, allDisabledFeatures.size());
  }

  @Test
  public void testGetHeatMapInDisabledFeaturesListWhenHeatMapFlagIsFalse() {
    ozoneConfiguration.set(OZONE_RECON_HEATMAP_PROVIDER_KEY,
        "org.apache.hadoop.ozone.recon.heatmap.TestHeatMapProviderImpl");
    ozoneConfiguration.setBoolean(OZONE_RECON_HEATMAP_ENABLE_KEY, false);
    FeatureProvider.initFeatureSupport(ozoneConfiguration);
    Response disabledFeatures = featuresEndPoint.getDisabledFeatures();
    List<FeatureProvider.Feature> allDisabledFeatures =
        (List<FeatureProvider.Feature>) disabledFeatures.getEntity();
    assertNotNull(allDisabledFeatures);
    assertThat(allDisabledFeatures.size()).isGreaterThan(0);
    assertEquals(FeatureProvider.Feature.HEATMAP.getFeatureName(),
        allDisabledFeatures.get(0).getFeatureName());
  }

  @Test
  public void testGetHeatMapNotInDisabledFeaturesListWhenHeatMapFlagIsTrue() {
    ozoneConfiguration.set(OZONE_RECON_HEATMAP_PROVIDER_KEY,
        "org.apache.hadoop.ozone.recon.heatmap.TestHeatMapProviderImpl");
    ozoneConfiguration.setBoolean(OZONE_RECON_HEATMAP_ENABLE_KEY, true);
    FeatureProvider.initFeatureSupport(ozoneConfiguration);
    Response disabledFeatures = featuresEndPoint.getDisabledFeatures();
    List<FeatureProvider.Feature> allDisabledFeatures =
        (List<FeatureProvider.Feature>) disabledFeatures.getEntity();
    assertNotNull(allDisabledFeatures);
    assertEquals(0, allDisabledFeatures.size());
  }
}
