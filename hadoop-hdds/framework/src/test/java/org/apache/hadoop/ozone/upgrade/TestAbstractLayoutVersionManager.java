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

package org.apache.hadoop.ozone.upgrade;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

/**
 * Test generic layout management init and APIs.
 */
public class TestAbstractLayoutVersionManager {

  @Spy
  private AbstractLayoutVersionManager<LayoutFeature> versionManager;

  @BeforeEach
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @AfterEach
  public void close() {
    versionManager.close();
  }

  @Test
  public void testInitializationWithFeaturesToBeFinalized() throws  Exception {
    versionManager.init(1, getTestLayoutFeatures(3));

    assertEquals(3, versionManager.features.size());
    assertEquals(3, versionManager.featureMap.size());

    assertEquals(1, versionManager.getMetadataLayoutVersion());
    assertEquals(3, versionManager.getSoftwareLayoutVersion());

    assertTrue(versionManager.needsFinalization());

    Iterator<LayoutFeature> it =
        versionManager.unfinalizedFeatures().iterator();
    assertNotNull(it.next());
    assertNotNull(it.next());
  }

  @Test
  public void testInitializationWithUpToDateMetadataVersion() throws Exception {
    versionManager.init(2, getTestLayoutFeatures(2));

    assertEquals(2, versionManager.features.size());
    assertEquals(2, versionManager.featureMap.size());

    assertEquals(2, versionManager.getMetadataLayoutVersion());
    assertEquals(2, versionManager.getSoftwareLayoutVersion());

    assertFalse(versionManager.needsFinalization());
    assertFalse(versionManager.unfinalizedFeatures().iterator().hasNext());
  }

  @Test
  public void testInitFailsIfNotEnoughLayoutFeaturesForVersion() {

    assertThrowsExactly(IOException.class,
        () -> versionManager.init(3, getTestLayoutFeatures(2)),
        "Cannot initialize VersionManager.");
  }

  @Test
  public void testFeatureFinalization() throws Exception {
    LayoutFeature[] lfs = getTestLayoutFeatures(3);
    versionManager.init(1, lfs);

    versionManager.finalized(lfs[1]);

    assertEquals(3, versionManager.features.size());
    assertEquals(3, versionManager.featureMap.size());

    assertEquals(2, versionManager.getMetadataLayoutVersion());
    assertEquals(3, versionManager.getSoftwareLayoutVersion());

    assertTrue(versionManager.needsFinalization());

    Iterator<LayoutFeature> it =
        versionManager.unfinalizedFeatures().iterator();
    assertNotNull(it.next());
    assertFalse(it.hasNext());
  }

  @Test
  public void testFeatureFinalizationFailsIfTheFinalizedFeatureIsNotTheNext()
      throws IOException {
    LayoutFeature[] lfs = getTestLayoutFeatures(3);
    versionManager.init(1, lfs);

    assertThrows(IllegalArgumentException.class,
        () -> versionManager.finalized(lfs[2]));
  }

  @Test
  public void testFeatureFinalizationIfFeatureIsAlreadyFinalized()
      throws IOException {
    /*
     * Feature finalization call is idempotent, it should not have any
     * side effects even if it's executed again.
     */
    LayoutFeature[] lfs = getTestLayoutFeatures(3);
    versionManager.init(2, lfs);
    assertEquals(2, versionManager.getMetadataLayoutVersion());
    versionManager.finalized(lfs[0]);
    assertEquals(2, versionManager.getMetadataLayoutVersion());
    versionManager.finalized(lfs[1]);
    assertEquals(2, versionManager.getMetadataLayoutVersion());
  }

  @Test
  public void testUnfinalizedFeaturesAreNotAllowed() throws Exception {
    LayoutFeature[] lfs = getTestLayoutFeatures(3);
    versionManager.init(1, lfs);

    assertTrue(versionManager.isAllowed(lfs[0].name()));
    assertTrue(versionManager.isAllowed(lfs[0]));

    assertFalse(versionManager.isAllowed(lfs[1].name()));
    assertFalse(versionManager.isAllowed(lfs[1]));
    assertFalse(versionManager.isAllowed(lfs[2].name()));
    assertFalse(versionManager.isAllowed(lfs[2]));

    versionManager.finalized(lfs[1]);

    assertTrue(versionManager.isAllowed(lfs[0].name()));
    assertTrue(versionManager.isAllowed(lfs[0]));
    assertTrue(versionManager.isAllowed(lfs[1].name()));
    assertTrue(versionManager.isAllowed(lfs[1]));

    assertFalse(versionManager.isAllowed(lfs[2].name()));
    assertFalse(versionManager.isAllowed(lfs[2]));
  }

  @Test
  public void testJmx() throws Exception {
    final int numLayoutFeatures = 3;
    final int metadataLayoutVersion = 1;
    versionManager.init(metadataLayoutVersion,
        getTestLayoutFeatures(numLayoutFeatures));

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName bean = new ObjectName(
        "Hadoop:service=LayoutVersionManager," +
            "name=" + versionManager.getClass().getSimpleName());

    Object mlv = mbs.getAttribute(bean, "MetadataLayoutVersion");
    assertEquals(metadataLayoutVersion, mlv);
    Object slv = mbs.getAttribute(bean, "SoftwareLayoutVersion");
    assertEquals(numLayoutFeatures, slv);
  }

  private LayoutFeature[] getTestLayoutFeatures(int num) {
    LayoutFeature[] lfs = new LayoutFeature[num];
    int k = 0;
    for (int i = 1; i <= num; i++) {
      int finalI = i;
      lfs[k++] = new LayoutFeature() {
        @Override
        public String name() {
          return "LF-" + finalI;
        }

        @Override
        public int layoutVersion() {
          return finalI;
        }

        @Override
        public String description() {
          return null;
        }
      };
    }
    return lfs;
  }

}
