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

package org.apache.hadoop.hdds.scm.net;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Objects;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.net.NetUtils;

/**
 * A class for host and port.
 * It also has an address which can be updated from time to time.
 */
public class HostAndPort {
  private final String host;
  private final int port;
  private final String hostAndPortString;
  private final int hash;
  /** The address can be updated from time to time. */
  private volatile InetSocketAddress address;

  public HostAndPort(String host, int port) {
    this.host = host;
    this.port = port;
    this.hostAndPortString = HddsUtils.getHostPortString(host, port);
    this.hash = host.hashCode() ^ Integer.hashCode(port);
    this.address = NetUtils.createSocketAddr(hostAndPortString);
  }

  public String getHostName() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public String getHostAndPortString() {
    return hostAndPortString;
  }

  public InetSocketAddress getAddress() {
    return address;
  }

  /** Re-resolves host:port and returns the new address if its IP changed, else null. No mutation. */
  public InetSocketAddress resolveLatest() {
    final InetSocketAddress latest = NetUtils.createSocketAddr(hostAndPortString);
    final InetAddress latestIp = latest.getAddress();
    if (latestIp == null || latestIp.equals(address.getAddress())) {
      return null;
    }
    return latest;
  }

  /** Commits an address re-resolved via {@link #resolveLatest()}. */
  public void setAddress(InetSocketAddress newAddress) {
    this.address = Objects.requireNonNull(newAddress, "newAddress == null");
  }

  @Override
  public int hashCode() {
    return hash;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    } else if (!(obj instanceof HostAndPort)) {
      return false;
    }
    final HostAndPort that = (HostAndPort) obj;
    // address must not be compared
    return this.hash == that.hash
        && this.port == that.port
        && this.host.equals(that.host);
  }

  @Override
  public String toString() {
    final InetSocketAddress a = getAddress();
    final Object resolved = a != null && a.getAddress() != null ? a.getAddress() : "<unresolved>";
    return hostAndPortString + "/" + resolved;
  }
}
