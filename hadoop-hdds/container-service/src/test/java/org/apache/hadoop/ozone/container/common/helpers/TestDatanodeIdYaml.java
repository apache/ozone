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
package org.apache.hadoop.ozone.container.common.helpers;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.MockDatanodeDetails;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link DatanodeIdYaml}.
 */
class TestDatanodeIdYaml {

  @Test
  void testWriteRead(@TempDir File dir) throws IOException {
    DatanodeDetails original = MockDatanodeDetails.randomDatanodeDetails();
    File file = new File(dir, "datanode.yaml");

    DatanodeIdYaml.createDatanodeIdFile(original, file);
    DatanodeDetails read = DatanodeIdYaml.readDatanodeIdFile(file);

    assertEquals(original, read);
    assertEquals(original.toDebugString(), read.toDebugString());
  }

}
