/*
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

package org.apache.hadoop.ozone.debug;

import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaOneDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.recon.scm.ReconSCMDBDefinition;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ratis.util.function.CheckedSupplier;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;

/**
 * Simple factory unit test.
 */
public class TestDBDefinitionFactory {

  @Test
  public void testGetDefinition() {
    DBDefinition definition = DBDefinitionFactory.getDefinition(OMDBDefinition.get().getName());
    assertInstanceOf(OMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(SCMDBDefinition.get().getName());
    assertInstanceOf(SCMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(ReconSCMDBDefinition.get().getName());
    assertInstanceOf(ReconSCMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(
        RECON_OM_SNAPSHOT_DB + "_1");
    assertInstanceOf(OMDBDefinition.class, definition);

    definition = DBDefinitionFactory.getDefinition(
        RECON_CONTAINER_KEY_DB + "_1");
    assertInstanceOf(ReconDBDefinition.class, definition);

    DBDefinitionFactory.setDnDBSchemaVersion("V2");
    final Path dbPath = Paths.get("/tmp/test-container.db");
    final OzoneConfiguration conf = new OzoneConfiguration();
    definition = DBDefinitionFactory.getDefinition(dbPath, conf);
    assertInstanceOf(DatanodeSchemaTwoDBDefinition.class, definition);

    DBDefinitionFactory.setDnDBSchemaVersion("V1");
    definition = DBDefinitionFactory.getDefinition(dbPath, conf);
    assertInstanceOf(DatanodeSchemaOneDBDefinition.class, definition);

    DBDefinitionFactory.setDnDBSchemaVersion("V3");
    definition = DBDefinitionFactory.getDefinition(dbPath, conf);
    assertInstanceOf(DatanodeSchemaThreeDBDefinition.class, definition);
  }

  @Test
  public void testGetDefinitionWithOverride() {
    final OzoneConfiguration conf = new OzoneConfiguration();
    Path dbPath = Paths.get("another.db");
    DBDefinition definition = DBDefinitionFactory.getDefinition(dbPath, conf, OMDBDefinition.class.getName());
    assertInstanceOf(OMDBDefinition.class, definition);
  }

  /*
   * Test to ensure that any DBDefinition has a default constructor or a constructor with 1 parameter.
   * This is needed for ldb tools to run with arbitrary DB definitions.
   */
  @Test
  public void testAllDBDefinitionsHaveCorrectConstructor() {
    Set<Class<?>> subclasses = new HashSet<>();
    try {
      Reflections reflections = new Reflections("org.apache.hadoop");
      subclasses.addAll(reflections.getSubTypesOf(DBDefinition.class));
      subclasses.remove(Class.forName("org.apache.hadoop.hdds.utils.db.DBDefinition$WithMap"));
      subclasses.remove(Class.forName("org.apache.hadoop.hdds.utils.db.DBDefinition$WithMapInterface"));
      subclasses.remove(Class.forName(
          "org.apache.hadoop.ozone.container.metadata.AbstractDatanodeDBDefinition"));
    } catch (Exception e) {
      fail("Error while finding subclasses: " + e.getMessage());
    }
    assertFalse(subclasses.isEmpty(), "No classes found extending DBDefinition");

    for (Class<?> clazz : subclasses) {
      List<CheckedSupplier<DBDefinition, Exception>> factories =
          DBDefinitionFactory.getFactories(clazz, "testDbPath", new OzoneConfiguration());
      boolean hasValidFactory = factories.stream().anyMatch(factory -> {
        try {
          factory.get();
          return true;
        } catch (Exception e) {
          return false;
        }
      });

      assertTrue(hasValidFactory,
          "Class " + clazz.getName() + " does not have a valid constructor or factory method.");
    }
  }
}
