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

package org.apache.hadoop.ozone.om;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.UUID;
import org.apache.hadoop.ozone.om.helpers.SnapshotInfo;
import org.apache.hadoop.util.Time;
import org.junit.jupiter.api.Test;

/**
 * Test for SnapshotListJSONServlet.
 */
public class TestSnapshotListJSONServlet {

  @Test
  public void testSerialization() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.addMixIn(SnapshotInfo.class,
        SnapshotListJSONServlet.SnapshotInfoMixin.class);

    String snapName = "snap1";
    SnapshotInfo snapshotInfo = SnapshotInfo.newInstance("vol1", "bucket1",
        snapName, UUID.randomUUID(), Time.now());

    String json = assertDoesNotThrow(() ->
        objectMapper.writeValueAsString(Collections.singletonList(snapshotInfo)));

    // Verify that the problematic fields are NOT in the JSON
    assertFalse(json.contains("protobuf"), "JSON should not contain protobuf field");
    assertFalse(json.contains("createTransactionInfo"), "JSON should not contain createTransactionInfo field");
    assertFalse(json.contains("lastTransactionInfo"), "JSON should not contain lastTransactionInfo field");

    // Verify that the expected fields ARE in the JSON
    assertTrue(json.contains("\"name\":\"" + snapName + "\""), "JSON should contain snapshot name");
  }
}
