/**
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

package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.utils.db.DBColumnFamilyDefinition;
import org.apache.hadoop.hdds.utils.db.DBStore;
import org.apache.hadoop.ozone.om.codec.OMDBDefinition;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Test that all the tables are covered both by OMDBDefinition
 * as well as OmMetadataManagerImpl.
 */
public class TestOMDBDefinition {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testDBDefinition() throws Exception {
    OzoneConfiguration configuration = new OzoneConfiguration();
    File metaDir = folder.getRoot();
    DBStore store = OmMetadataManagerImpl.loadDB(configuration, metaDir);
    OMDBDefinition dbDef = new OMDBDefinition();

    // Get list of tables from DB Definitions
    DBColumnFamilyDefinition[] columnFamilyDefinitions =
        dbDef.getColumnFamilies();
    int countOmDefTables = columnFamilyDefinitions.length;
    ArrayList<String> missingDBDefTables = new ArrayList<>();

    // Get list of tables from the RocksDB Store
    Collection<String> missingOmDBTables =
        store.getTableNames().values();
    missingOmDBTables.remove("default");
    int countOmDBTables = missingOmDBTables.size();
    // Remove the file if it is found in both the datastructures
    for(DBColumnFamilyDefinition definition : columnFamilyDefinitions) {
      if (!missingOmDBTables.remove(definition.getName())) {
        missingDBDefTables.add(definition.getName());
      }
    }

    Assert.assertEquals("Tables in OmMetadataManagerImpl are:"
            + missingDBDefTables, 0, missingDBDefTables.size());
    Assert.assertEquals("Tables missing in OMDBDefinition are:"
        + missingOmDBTables, 0, missingOmDBTables.size());
    Assert.assertEquals(countOmDBTables, countOmDefTables);
  }
}
