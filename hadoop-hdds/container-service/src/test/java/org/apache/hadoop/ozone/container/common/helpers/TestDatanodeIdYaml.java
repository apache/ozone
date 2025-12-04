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

package org.apache.hadoop.ozone.container.common.helpers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature;
import org.apache.hadoop.ozone.container.common.DatanodeLayoutStorage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DatanodeIdYaml}.
 */
class TestDatanodeIdYaml {

  @Test
  void testWriteRead(@TempDir File dir) throws IOException {
    DatanodeDetails original = MockDatanodeDetails.randomDatanodeDetails();
    File file = new File(dir, "datanode.yaml");

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());

    DatanodeIdYaml.createDatanodeIdFile(original, file, conf);
    DatanodeDetails read = DatanodeIdYaml.readDatanodeIdFile(file);

    assertEquals(original, read);
    assertEquals(original.toDebugString(), read.toDebugString());
  }

  @Test
  void testWriteReadBeforeRatisDatastreamPortLayoutVersion(@TempDir File dir)
      throws IOException {
    DatanodeDetails original = MockDatanodeDetails.randomDatanodeDetails();
    File file = new File(dir, "datanode.yaml");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion());
    layoutStorage.initialize();

    DatanodeIdYaml.createDatanodeIdFile(original, file, conf);
    DatanodeDetails read = DatanodeIdYaml.readDatanodeIdFile(file);

    assertNotNull(original.getPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM));
    // if no separate admin/server/datastream port, return single Ratis one for
    // compat
    assertEquals(read.getPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM),
        read.getRatisPort());
  }

  @Test
  void testWriteReadAfterRatisDatastreamPortLayoutVersion(@TempDir File dir)
      throws IOException {
    DatanodeDetails original = MockDatanodeDetails.randomDatanodeDetails();

    File file = new File(dir, "datanode.yaml");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.RATIS_DATASTREAM_PORT_IN_DATANODEDETAILS
            .layoutVersion());
    layoutStorage.initialize();

    DatanodeIdYaml.createDatanodeIdFile(original, file, conf);
    DatanodeDetails read = DatanodeIdYaml.readDatanodeIdFile(file);

    assertNotNull(original.getPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM));
    assertEquals(original.getPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM),
        read.getPort(DatanodeDetails.Port.Name.RATIS_DATASTREAM));
  }

  @Test
  void testWriteReadBeforeWebUIPortLayoutVersion(@TempDir File dir)
      throws IOException {
    DatanodeDetails original = MockDatanodeDetails.randomDatanodeDetails();
    File file = new File(dir, "datanode.yaml");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.DATANODE_SCHEMA_V3.layoutVersion());
    layoutStorage.initialize();

    DatanodeIdYaml.createDatanodeIdFile(original, file, conf);
    DatanodeDetails read = DatanodeIdYaml.readDatanodeIdFile(file);

    assertNotNull(original.getPort(DatanodeDetails.Port.Name.HTTP));
    assertNotNull(original.getPort(DatanodeDetails.Port.Name.HTTPS));
    assertNull(read.getPort(DatanodeDetails.Port.Name.HTTP));
    assertNull(read.getPort(DatanodeDetails.Port.Name.HTTPS));
  }

  @Test
  void testWriteReadAfterWebUIPortLayoutVersion(@TempDir File dir)
      throws IOException {
    DatanodeDetails original = MockDatanodeDetails.randomDatanodeDetails();
    File file = new File(dir, "datanode.yaml");
    OzoneConfiguration conf = new OzoneConfiguration();
    conf.set(HddsConfigKeys.OZONE_METADATA_DIRS, dir.toString());
    DatanodeLayoutStorage layoutStorage = new DatanodeLayoutStorage(conf,
        UUID.randomUUID().toString(),
        HDDSLayoutFeature.WEBUI_PORTS_IN_DATANODEDETAILS.layoutVersion());
    layoutStorage.initialize();

    DatanodeIdYaml.createDatanodeIdFile(original, file, conf);
    DatanodeDetails read = DatanodeIdYaml.readDatanodeIdFile(file);

    assertNotNull(original.getPort(DatanodeDetails.Port.Name.HTTP));
    assertNotNull(original.getPort(DatanodeDetails.Port.Name.HTTPS));
    assertEquals(original.getPort(DatanodeDetails.Port.Name.HTTP),
        read.getPort(DatanodeDetails.Port.Name.HTTP));
    assertEquals(original.getPort(DatanodeDetails.Port.Name.HTTPS),
        read.getPort(DatanodeDetails.Port.Name.HTTPS));
  }

}
