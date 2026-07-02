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

package org.apache.hadoop.ozone.om.exceptions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ServiceException;
import java.io.IOException;
import org.apache.hadoop.ipc_.RemoteException;
import org.apache.hadoop.ozone.om.ha.OMFailoverProxyProviderBase;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

/**
 * Round-trip tests for {@link OMNotLeaderException}'s message format and parser.
 * <p>
 * Hadoop RPC's exception protocol carries only (className, message); the
 * client side reconstructs via reflection through the {@code (String)} ctor
 * (see {@link RemoteException#unwrapRemoteException()}). Therefore the
 * suggested-leader hint must be embedded in the message string and re-parsed
 * in the {@code (String)} ctor. These tests cover the parser directly, the
 * producer-to-parser round trip, and the production round-trip path used by
 * the failover proxy provider.
 */
class TestOMNotLeaderExceptionMessageParsing {

  private static final RaftPeerId LOCAL = RaftPeerId.valueOf("om2");
  private static final RaftPeerId LEADER = RaftPeerId.valueOf("om1");

  @Test
  void testParsesIPv4HappyPath() {
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=10.0.0.5).";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862", parsed.getSuggestedLeaderAddress());
    assertEquals("10.0.0.5", parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesIPv6HostPortWithBrackets() {
    // IPv6 host:port is conventionally written as [fe80::1]:9862. The message
    // wrapper for the IP uses parentheses so the bracket form here does not
    // collide with the format delimiters.
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at [fe80::1]:9862 (ip=fe80::1).";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("[fe80::1]:9862", parsed.getSuggestedLeaderAddress());
    assertEquals("fe80::1", parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesUnresolvedSentinelAsNullIp() {
    // When the producer's startup-time DNS lookup failed, it ships the
    // sentinel `unresolved`; the parser must normalize it back to null.
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=unresolved).";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862", parsed.getSuggestedLeaderAddress());
    assertNull(parsed.getSuggestedLeaderIpAddress(),
        "ip=unresolved sentinel must round-trip back to null");
  }

  @Test
  void testNoLeaderFormLeavesAllSuggestedFieldsNull() {
    String msg = "OM:om2 is not the leader. Could not determine the leader node.";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertNull(parsed.getSuggestedLeaderNodeId());
    assertNull(parsed.getSuggestedLeaderAddress());
    assertNull(parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testMalformedMessageLeavesAllFieldsNull() {
    OMNotLeaderException parsed =
        new OMNotLeaderException("entirely unrelated text");
    assertNull(parsed.getSuggestedLeaderNodeId());
    assertNull(parsed.getSuggestedLeaderAddress());
    assertNull(parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testEmptyMessageLeavesAllFieldsNull() {
    OMNotLeaderException parsed = new OMNotLeaderException("");
    assertNull(parsed.getSuggestedLeaderNodeId());
    assertNull(parsed.getSuggestedLeaderAddress());
    assertNull(parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testTruncatedSuggestedLeaderLeavesAllFieldsNull() {
    // Has the prefix but is missing the (ip=.*) wrapper. A partial parse
    // would be worse than no parse — the consumer would set the next proxy
    // based on a half-recovered hint. Require all-or-nothing.
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at om1.cluster.local:9862.";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertNull(parsed.getSuggestedLeaderNodeId());
    assertNull(parsed.getSuggestedLeaderAddress());
    assertNull(parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesEvenWithoutLeadingPrefix() {
    // No `OM:<id> is not the leader.` gate: the suggested-leader fragment
    // alone is distinctive enough to parse. This reflects the deliberate
    // choice to be loose at both ends of the regex. A caller that
    // wraps the message with any prefix should still surface the hint to
    // the consumer rather than silently falling back to round-robin.
    String msg = "Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=10.0.0.5).";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862", parsed.getSuggestedLeaderAddress());
    assertEquals("10.0.0.5", parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesIPv4WithTrailingStackTrace() {
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=10.0.0.5).\n" +
        "\tat org.apache.hadoop.ozone.om.exceptions.OMNotLeaderException" +
        ".convertToOMNotLeaderException(OMNotLeaderException.java:128)\n" +
        "\tat org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer" +
        ".createOmResponseImpl(OzoneManagerRatisServer.java:597)";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862", parsed.getSuggestedLeaderAddress());
    assertEquals("10.0.0.5", parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesIPv6WithTrailingStackTrace() {
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at [fe80::1]:9862 (ip=fe80::1).\n" +
        "\tat org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer" +
        ".createOmResponseImpl(OzoneManagerRatisServer.java:597)";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("[fe80::1]:9862", parsed.getSuggestedLeaderAddress());
    assertEquals("fe80::1", parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesUnresolvedSentinelWithTrailingStackTrace() {
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=unresolved).\n" +
        "\tat org.apache.hadoop.ozone.om.ratis.OzoneManagerRatisServer.foo(...)";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862", parsed.getSuggestedLeaderAddress());
    assertNull(parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testParsesIPv4WithTrailingWhitespaceOnly() {
    // Whitespace-only tail also exercised, since the previous strict anchor
    // explicitly tolerated `\\s*$` — keep that behavior under the loose form.
    String msg = "OM:om2 is not the leader. " +
        "Suggested leader is OM:om1 at om1.cluster.local:9862 (ip=10.0.0.5).   \n\t  ";
    OMNotLeaderException parsed = new OMNotLeaderException(msg);
    assertEquals("om1", parsed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862", parsed.getSuggestedLeaderAddress());
    assertEquals("10.0.0.5", parsed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testProducerToParserRoundTripIPv4() {
    OMNotLeaderException original = new OMNotLeaderException(
        LOCAL, LEADER, "om1.cluster.local:9862", "10.0.0.5");
    OMNotLeaderException reconstructed =
        new OMNotLeaderException(original.getMessage());
    assertEquals(original.getSuggestedLeaderNodeId(),
        reconstructed.getSuggestedLeaderNodeId());
    assertEquals(original.getSuggestedLeaderAddress(),
        reconstructed.getSuggestedLeaderAddress());
    assertEquals(original.getSuggestedLeaderIpAddress(),
        reconstructed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testProducerToParserRoundTripIPv6() {
    OMNotLeaderException original = new OMNotLeaderException(
        LOCAL, LEADER, "[fe80::1]:9862", "fe80::1");
    OMNotLeaderException reconstructed =
        new OMNotLeaderException(original.getMessage());
    assertEquals("om1", reconstructed.getSuggestedLeaderNodeId());
    assertEquals("[fe80::1]:9862", reconstructed.getSuggestedLeaderAddress());
    assertEquals("fe80::1", reconstructed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testProducerToParserRoundTripNullIpRendersAsUnresolvedSentinel() {
    OMNotLeaderException original = new OMNotLeaderException(
        LOCAL, LEADER, "om1.cluster.local:9862", null);

    // Producer must serialize a null IP as the documented sentinel ...
    assertTrue(original.getMessage().contains("(ip=unresolved)"),
        "null IP should serialize as the `unresolved` sentinel; was: "
            + original.getMessage());

    // ... and the parser must turn the sentinel back into null.
    OMNotLeaderException reconstructed =
        new OMNotLeaderException(original.getMessage());
    assertEquals("om1", reconstructed.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862",
        reconstructed.getSuggestedLeaderAddress());
    assertNull(reconstructed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testProducerToParserRoundTripNoLeaderForm() {
    OMNotLeaderException original = new OMNotLeaderException(LOCAL);
    OMNotLeaderException reconstructed =
        new OMNotLeaderException(original.getMessage());
    assertNull(reconstructed.getSuggestedLeaderNodeId());
    assertNull(reconstructed.getSuggestedLeaderAddress());
    assertNull(reconstructed.getSuggestedLeaderIpAddress());
  }

  @Test
  void testRoundTripThroughRemoteExceptionUnwrap() throws IOException {
    OMNotLeaderException original = new OMNotLeaderException(
        LOCAL, LEADER, "om1.cluster.local:9862", "10.0.0.5");

    // Simulate the wire: Hadoop RPC reduces an exception to (className, message).
    RemoteException remote = new RemoteException(
        OMNotLeaderException.class.getName(), original.getMessage());

    IOException unwrapped = remote.unwrapRemoteException();
    assertInstanceOf(OMNotLeaderException.class, unwrapped,
        "unwrapRemoteException must reflectively rebuild an OMNotLeaderException");
    OMNotLeaderException recovered = (OMNotLeaderException) unwrapped;
    assertEquals("om1", recovered.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862",
        recovered.getSuggestedLeaderAddress());
    assertEquals("10.0.0.5", recovered.getSuggestedLeaderIpAddress());
  }

  @Test
  void testRoundTripThroughGetNotLeaderExceptionIPv6() {
    OMNotLeaderException original = new OMNotLeaderException(
        LOCAL, LEADER, "[fe80::1]:9862", "fe80::1");

    // Mimic the wire shape that OMFailoverProxyProviderBase consumes:
    //   ServiceException(RemoteException(className, message))
    ServiceException wireEx = new ServiceException(new RemoteException(
        OMNotLeaderException.class.getName(), original.getMessage()));

    OMNotLeaderException recovered =
        OMFailoverProxyProviderBase.getNotLeaderException(wireEx);
    assertNotNull(recovered,
        "getNotLeaderException must recognize the wire shape");
    assertEquals("om1", recovered.getSuggestedLeaderNodeId());
    assertEquals("[fe80::1]:9862", recovered.getSuggestedLeaderAddress());
    assertEquals("fe80::1", recovered.getSuggestedLeaderIpAddress());
  }

  @Test
  void testRoundTripThroughGetNotLeaderExceptionUnresolved() {
    // Stale-DNS scenario the user explicitly wanted catchable: the producer
    // could not resolve its own peer and shipped the sentinel.
    OMNotLeaderException original = new OMNotLeaderException(
        LOCAL, LEADER, "om1.cluster.local:9862", null);

    ServiceException wireEx = new ServiceException(new RemoteException(
        OMNotLeaderException.class.getName(), original.getMessage()));

    OMNotLeaderException recovered =
        OMFailoverProxyProviderBase.getNotLeaderException(wireEx);
    assertNotNull(recovered);
    assertEquals("om1", recovered.getSuggestedLeaderNodeId());
    assertEquals("om1.cluster.local:9862",
        recovered.getSuggestedLeaderAddress());
    assertNull(recovered.getSuggestedLeaderIpAddress(),
        "unresolved sentinel must surface as null IP on the consumer side");
  }

  @Test
  void testRoundTripThroughGetNotLeaderExceptionNoLeaderForm() {
    OMNotLeaderException original = new OMNotLeaderException(LOCAL);

    ServiceException wireEx = new ServiceException(new RemoteException(
        OMNotLeaderException.class.getName(), original.getMessage()));

    OMNotLeaderException recovered =
        OMFailoverProxyProviderBase.getNotLeaderException(wireEx);
    assertNotNull(recovered,
        "no-leader form must still unwrap to OMNotLeaderException");
    assertNull(recovered.getSuggestedLeaderNodeId());
    assertNull(recovered.getSuggestedLeaderAddress());
    assertNull(recovered.getSuggestedLeaderIpAddress());
  }
}
