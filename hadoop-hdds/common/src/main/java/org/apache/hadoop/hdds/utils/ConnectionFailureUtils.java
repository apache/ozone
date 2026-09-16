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

import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hdds.scm.container.common.helpers.StorageContainerException;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;

/**
 * Shared classifier for exceptions where the cached peer IP is no longer
 * reachable and DNS re-resolution is the only plausible recovery path.
 * <p>
 * Used by both {@code SCMFailoverProxyProviderBase} and
 * {@code OMFailoverProxyProviderBase} to gate the DNS-refresh-on-failure
 * code path so that application-level errors (NotLeader, AccessControl,
 * OMException, RetryAction) do not trigger spurious DNS lookups.
 * <p>
 * The classifier must match the failure shapes seen in production
 * Kubernetes deployments where the peer pod has been rescheduled to a
 * new IP under a stable hostname:
 * <ul>
 *   <li>{@link ConnectException} -- the TCP SYN was refused. Seen on
 *       OpenStack / fast-RST environments. </li>
 *   <li>{@link SocketTimeoutException} (and its IPC subclass
 *       {@code ConnectTimeoutException}) -- the SYN was dropped silently.
 *       This is the dominant failure shape on AWS EC2 / EKS where the
 *       network silently drops packets to a defunct pod IP. The PR that
 *       introduced this helper (HDDS-15514) is sold on this case; it
 *       must be in the filter. </li>
 *   <li>{@link NoRouteToHostException} -- routing table no longer
 *       reaches the cached IP. </li>
 *   <li>{@link UnknownHostException} -- the hostname itself failed to
 *       resolve at the time the IPC layer reconstructed the address. </li>
 *   <li>{@link EOFException} -- a load balancer or iptables RST closed
 *       the half-open connection cleanly. Common in Kubernetes when an
 *       IP is reassigned to an unrelated pod that rejects the RPC
 *       handshake. </li>
 *   <li>{@link SocketException} (e.g. "Connection reset") -- the peer
 *       sent RST mid-stream. </li>
 * </ul>
 * The walk is bounded to {@value #MAX_CAUSE_DEPTH} levels to defend
 * against cause chains that have been constructed (in violation of
 * {@code Throwable.initCause}'s contract) into a cycle of length &gt; 1.
 */
public final class ConnectionFailureUtils {

  /**
   * Maximum depth of the {@code Throwable.getCause()} chain we walk
   * before giving up. Matches Hadoop's own walkers in
   * {@code RemoteException} handling.
   */
  static final int MAX_CAUSE_DEPTH = 16;

  private ConnectionFailureUtils() {
  }

  /**
   * Returns true when any link in {@code t}'s cause chain (up to
   * {@link #MAX_CAUSE_DEPTH} levels) is one of the connection-class
   * exceptions documented on this class.
   *
   * @param t the throwable to classify. {@code null} returns false.
   */
  public static boolean isConnectionFailure(Throwable t) {
    Throwable cause = t;
    for (int depth = 0; cause != null && depth < MAX_CAUSE_DEPTH; depth++) {
      // ConnectException and NoRouteToHostException both extend
      // SocketException, so the SocketException check below already matches
      // them. They remain listed in this class's Javadoc as connection-
      // failure shapes for documentation.
      if (cause instanceof SocketTimeoutException
          || cause instanceof UnknownHostException
          || cause instanceof EOFException
          || cause instanceof SocketException) {
        return true;
      }
      Throwable next = cause.getCause();
      if (next == cause) {
        break;
      }
      cause = next;
    }
    return false;
  }

  /**
   * Returns the first {@link StorageContainerException} or
   * {@link TimeoutIOException} in {@code ex}'s cause chain (through
   * {@link ExecutionException} and nested {@link IOException} wrappers).
   */
  public static IOException unwrapCause(IOException ex) {
    Throwable t = ex;
    while (t != null) {
      if (t instanceof TimeoutIOException || t instanceof StorageContainerException) {
        return (IOException) t;
      }
      if (t instanceof ExecutionException && t.getCause() != null) {
        t = t.getCause();
        continue;
      }
      if (t.getCause() instanceof IOException) {
        t = t.getCause();
        continue;
      }
      break;
    }
    return ex;
  }
}
