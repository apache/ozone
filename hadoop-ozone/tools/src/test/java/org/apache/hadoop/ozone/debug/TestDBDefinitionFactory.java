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

import java.nio.file.Paths;

import org.apache.hadoop.hdds.scm.metadata.SCMDBDefinition;
import org.apache.hadoop.hdds.utils.db.DBDefinition;
import org.apache.hadoop.ozone.container.metadata.DatanodeSchemaTwoDBDefinition;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.apache.hadoop.ozone.recon.scm.ReconSCMDBDefinition;
import org.apache.hadoop.ozone.recon.spi.impl.ReconDBDefinition;

import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_CONTAINER_KEY_DB;
import static org.apache.hadoop.ozone.recon.ReconConstants.RECON_OM_SNAPSHOT_DB;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

/**
 * Simple factory unit test.
 */
public class TestDBDefinitionFactory {

  @Test
  public void testGetDefinition() {
    DBDefinition definition =
        DBDefinitionFactory.getDefinition(new OMDBDefinition().getName());
    assertTrue(definition instanceof OMDBDefinition);

    definition = DBDefinitionFactory.getDefinition(
        new SCMDBDefinition().getName());
    assertTrue(definition instanceof SCMDBDefinition);

    definition = DBDefinitionFactory.getDefinition(
        new ReconSCMDBDefinition().getName());
    assertTrue(definition instanceof ReconSCMDBDefinition);

    definition = DBDefinitionFactory.getDefinition(
        RECON_OM_SNAPSHOT_DB + "_1");
    assertTrue(definition instanceof OMDBDefinition);

    definition = DBDefinitionFactory.getDefinition(
        RECON_CONTAINER_KEY_DB + "_1");
    assertTrue(definition instanceof ReconDBDefinition);

    definition =
        DBDefinitionFactory.getDefinition(Paths.get("/tmp/test-container.db"));
    assertTrue(definition instanceof DatanodeSchemaTwoDBDefinition);
  }
}