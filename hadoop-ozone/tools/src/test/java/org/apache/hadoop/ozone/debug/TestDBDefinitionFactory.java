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

package org.apache.hadoop.ozone.debug;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaOneDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaThreeDBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.recon.scm.ReconSCMDBDefinition;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition;
import org.junit.jupiter.api.Test;

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
}
