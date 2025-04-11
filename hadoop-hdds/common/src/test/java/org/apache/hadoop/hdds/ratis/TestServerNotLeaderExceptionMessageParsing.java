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

package org.apache.hadoop.hdds.ratis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** Class to test {@link ServerNotLeaderException} parsing. **/
class TestServerNotLeaderExceptionMessageParsing {
  @Test
  public void testServerNotLeaderException() {

    // Test hostname with "."
    final String msg =
        "SCM Server:cf0bc565-a41b-4784-a24d-3048d5a5b013(172.16.102.111) is not the leader. "
            + "Suggested leader is Server:scm5-3.scm5.root.hwx.site:9863";
    ServerNotLeaderException snle = new ServerNotLeaderException(msg);
    assertEquals(snle.getSuggestedLeader(), "scm5-3.scm5.root.hwx" +
        ".site:9863");

    String message = "SCM Server:7fdd7170-75cc-4e11-b343-c2657c2f2f39(172.16.102.111) is not the " +
        "leader.Suggested leader is Server:scm5-3.scm5.root.hwx.site:9863 \n" +
        "at org.apache.hadoop.hdds.ratis.ServerNotLeaderException" +
        ".convertToNotLeaderException(ServerNotLeaderException.java:96)";
    snle = new ServerNotLeaderException(message);
    assertEquals("scm5-3.scm5.root.hwx.site:9863",
        snle.getSuggestedLeader());

    // Test hostname with out "."
    message = "SCM Server:7fdd7170-75cc-4e11-b343-c2657c2f2f39(172.16.102.111) is not the " +
        "leader.Suggested leader is Server:localhost:98634 \n" +
        "at org.apache.hadoop.hdds.ratis.ServerNotLeaderException" +
        ".convertToNotLeaderException(ServerNotLeaderException.java:96)";
    snle = new ServerNotLeaderException(message);
    assertEquals("localhost:98634",
        snle.getSuggestedLeader());

    message = "SCM Server:7fdd7170-75cc-4e11-b343-c2657c2f2f39(172.16.102.111) is not the " +
        "leader.Suggested leader is Server::98634 \n" +
        "at org.apache.hadoop.hdds.ratis.ServerNotLeaderException" +
        ".convertToNotLeaderException(ServerNotLeaderException.java:96)";
    snle = new ServerNotLeaderException(message);
    assertNull(snle.getSuggestedLeader());

    message = "SCM Server:7fdd7170-75cc-4e11-b343-c2657c2f2f39(172.16.102.111) is not the " +
        "leader.Suggested leader is Server:localhost:98634:8988 \n" +
        "at org.apache.hadoop.hdds.ratis.ServerNotLeaderException" +
        ".convertToNotLeaderException(ServerNotLeaderException.java:96)";
    snle = new ServerNotLeaderException(message);
    assertEquals("localhost:98634",
        snle.getSuggestedLeader());

    message = "SCM Server:7fdd7170-75cc-4e11-b343-c2657c2f2f39(172.16.102.111) is not the " +
        "leader.Suggested leader is Server:localhost \n" +
        "at org.apache.hadoop.hdds.ratis.ServerNotLeaderException" +
        ".convertToNotLeaderException(ServerNotLeaderException.java)";
    snle = new ServerNotLeaderException(message);
    assertNull(snle.getSuggestedLeader());
  }

}
