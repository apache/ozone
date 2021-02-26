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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Test generic layout management init and APIs.
 */
@RunWith(MockitoJUnitRunner.class)
public class TestAbstractLayoutVersionManager {

  @Spy
  private AbstractLayoutVersionManager<LayoutFeature> versionManager;

  @Rule
  public ExpectedException exception = ExpectedException.none();

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
  public void testInitFailsIfNotEnoughLayoutFeaturesForVersion()
      throws Exception {
    exception.expect(IOException.class);
    exception.expectMessage("Cannot initialize VersionManager.");

    versionManager.init(3, getTestLayoutFeatures(2));
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
    exception.expect(IllegalArgumentException.class);

    LayoutFeature[] lfs = getTestLayoutFeatures(3);
    versionManager.init(1, lfs);

    versionManager.finalized(lfs[2]);
  }

  @Test
  public void testFeatureFinalizationFailsIfFeatureIsAlreadyFinalized()
      throws IOException {
    exception.expect(IllegalArgumentException.class);

    LayoutFeature[] lfs = getTestLayoutFeatures(3);
    versionManager.init(1, lfs);

    versionManager.finalized(lfs[0]);
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
