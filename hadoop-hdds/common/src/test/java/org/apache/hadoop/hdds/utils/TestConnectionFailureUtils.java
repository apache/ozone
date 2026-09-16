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

package org.apache.hadoop.hdds.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.stream.Stream;
import org.apache.hadoop.security.AccessControlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies the connection-class exception classifier used to gate the
 * DNS-refresh-on-failure code path on both the OM and SCM failover
 * proxy providers and the DataNode heartbeat catch block.
 * <p>
 * The classifier must:
 * <ul>
 *   <li>Match every exception type that signals "the cached IP is no
 *       longer reachable" -- including the AWS EC2 / EKS silent-drop
 *       case which surfaces as {@link SocketTimeoutException}, the
 *       case the PR motivating this helper (HDDS-15514) is sold on. </li>
 *   <li>Reject application-level errors (NotLeader, AccessControl,
 *       protocol mismatch) so we don't add DNS load on logical
 *       failures where the cached IP is fine. </li>
 *   <li>Walk wrapped cause chains so a {@code RemoteException(...)} or
 *       {@code IOException(...)} carrying a connection-class cause is
 *       still classified correctly. </li>
 *   <li>Defend against pathological cycles in the cause chain. </li>
 * </ul>
 */
public class TestConnectionFailureUtils {

  static Stream<Arguments> connectionClassExceptions() {
    return Stream.of(
        Arguments.of(new ConnectException("refused"),         "ConnectException"),
        Arguments.of(new SocketTimeoutException("EC2 drop"),  "SocketTimeoutException (AWS silent drop)"),
        Arguments.of(new NoRouteToHostException("gone"),      "NoRouteToHostException"),
        Arguments.of(new UnknownHostException("dns failed"),  "UnknownHostException"),
        Arguments.of(new EOFException("LB closed"),           "EOFException"),
        Arguments.of(new SocketException("Connection reset"), "SocketException")
    );
  }

  @ParameterizedTest(name = "isConnectionFailure detects {1}")
  @MethodSource("connectionClassExceptions")
  public void testDetectsBareConnectionClass(Throwable t, String label) {
    assertTrue(ConnectionFailureUtils.isConnectionFailure(t),
        label + " must be classified as a connection failure");
  }

  @ParameterizedTest(name = "isConnectionFailure walks IOException wrap of {1}")
  @MethodSource("connectionClassExceptions")
  public void testDetectsThroughIOExceptionWrap(Throwable t, String label) {
    IOException wrapped = new IOException("rpc failed", t);
    assertTrue(ConnectionFailureUtils.isConnectionFailure(wrapped),
        "IOException wrapping " + label + " must still be classified");
  }

  @Test
  public void testDeeplyNestedChainStillClassified() {
    // ConnectException three levels deep, the way Hadoop RPC's RetriableException
    // wraps ServiceException wraps IOException wraps the real cause.
    Throwable deep = new RuntimeException("outer",
        new IOException("middle",
            new IOException("inner", new ConnectException("dead"))));
    assertTrue(ConnectionFailureUtils.isConnectionFailure(deep));
  }

  static Stream<Arguments> applicationLevelExceptions() {
    return Stream.of(
        Arguments.of(new AccessControlException("denied"),
            "AccessControlException"),
        Arguments.of(new IllegalArgumentException("bad request"),
            "IllegalArgumentException"),
        Arguments.of(new IOException("application error: not leader"),
            "plain IOException without connection-class cause"),
        Arguments.of(new RuntimeException("retry, please"),
            "plain RuntimeException")
    );
  }

  @ParameterizedTest(name = "isConnectionFailure rejects {1}")
  @MethodSource("applicationLevelExceptions")
  public void testRejectsApplicationLevel(Throwable t, String label) {
    assertFalse(ConnectionFailureUtils.isConnectionFailure(t),
        label + " is an application error, refresh must NOT trigger");
  }

  @Test
  public void testNullIsNotAConnectionFailure() {
    assertFalse(ConnectionFailureUtils.isConnectionFailure(null));
  }

  /**
   * {@code Throwable.initCause} contractually rejects setting cause to
   * the throwable itself, but cycles of length 2+ have appeared in
   * practice (proxy frameworks and faulty initCause callers can
   * construct them). The walk must terminate within the configured
   * depth bound rather than looping forever.
   * <p>
   * We build the length-2 cycle through {@link Throwable#initCause}
   * (no reflection) -- the no-arg ctor leaves cause uninitialized
   * (cause==this sentinel), so a single initCause call on each side
   * is permitted and lets us close the cycle.
   */
  @Test
  public void testCycleOfLengthTwoTerminates() {
    Throwable a = new IOException();
    Throwable b = new IOException();
    a.initCause(b);
    b.initCause(a);
    // Neither a nor b is a connection-class type. The walk must return
    // false (not loop forever and not throw).
    assertFalse(ConnectionFailureUtils.isConnectionFailure(a),
        "length-2 cycle must terminate cleanly");
  }

  /**
   * Defense against an unbounded chain of non-connection-class
   * exceptions: the depth bound must kick in.
   * <p>
   * Built using {@link Throwable#initCause} on freshly-constructed
   * exceptions (no-arg ctor leaves cause uninitialized) so the test
   * does not depend on JDK-internal reflective access to the
   * {@code cause} field, which fails on JDK 16+ without
   * {@code --add-opens java.base/java.lang=ALL-UNNAMED}.
   */
  @Test
  public void testUnboundedChainOfNonMatchingTerminates() {
    Throwable head = new RuntimeException();
    Throwable cursor = head;
    for (int i = 1; i < 1024; i++) {
      Throwable next = new RuntimeException();
      cursor.initCause(next);
      cursor = next;
    }
    assertFalse(ConnectionFailureUtils.isConnectionFailure(head));
  }
}
