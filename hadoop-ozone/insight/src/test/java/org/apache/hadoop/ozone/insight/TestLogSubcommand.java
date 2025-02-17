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

package org.apache.hadoop.ozone.insight;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Testing utility methods of the log subcommand test.
 */
public class TestLogSubcommand {

  @Test
  public void filterLog() {
    LogSubcommand logSubcommand = new LogSubcommand();
    String result = logSubcommand.processLogLine(
        "2020-03-12 20:57:54,001 [TRACE|org.apache.hadoop.hdds.scm.node"
            + ".SCMNodeManager|SCMNodeManager] HB is received from "
            + "[datanode=localhost]: <json>storageReport {\\n  storageUuid: "
            + "\"DS-f65eb957-fc2d-4b77-b4a3-e96ae2bd2ca6\"\\n  "
            + "storageLocation: \"/tmp/hadoop-neo/dfs/data\"\\n  capacity: "
            + "250438021120\\n  scmUsed: 16384\\n  remaining: 212041244672\\n  "
            + "storageType: DISK\\n  failed: false\\n}\\n</json>");
    assertEquals(10, result.split("\n").length);
  }
}
