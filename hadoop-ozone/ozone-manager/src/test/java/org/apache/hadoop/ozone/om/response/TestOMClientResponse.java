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

package org.apache.hadoop.ozone.om.response;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashSet;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMResponse;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Status;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.Type;
import org.junit.jupiter.api.Test;

/**
 * Tests generic OM client response behavior.
 */
public class TestOMClientResponse {

  @Test
  public void testCleanupTablesMergeTrackedAndResponseTables() {
    OMClientResponse response = new DummyOMClientResponse(
        OMResponse.newBuilder()
            .setStatus(Status.OK)
            .setCmdType(Type.CreateSnapshot)
            .build());

    response.addCleanupTables(new LinkedHashSet<>(Arrays.asList(
        "deletedTable", "snapshotRenamedTable")));
    response.addCleanupTables(new LinkedHashSet<>(
        Arrays.asList("snapshotInfoTable")));

    assertEquals(new LinkedHashSet<>(Arrays.asList("deletedTable",
        "snapshotRenamedTable", "snapshotInfoTable")),
        response.getCleanupTables());
  }
}
