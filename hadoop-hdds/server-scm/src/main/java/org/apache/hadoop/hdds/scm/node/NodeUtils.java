/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

/**
 * Util class for Node operations.
 */
public final class NodeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(NodeUtils.class);

  private NodeUtils() {
  }

  public static List<DatanodeDetails> mapHostnamesToDatanodes(
      NodeManager nodeManager, List<String> hosts, boolean useHostnames)
      throws InvalidHostStringException {
    List<DatanodeDetails> results = new LinkedList<>();
    for (String hostString : hosts) {
      HostDefinition host = new HostDefinition(hostString);
      InetAddress addr;
      try {
        addr = InetAddress.getByName(host.getHostname());
      } catch (UnknownHostException e) {
        throw new InvalidHostStringException("Unable to resolve host "
            + host.getRawHostname(), e);
      }
      String dnsName;
      if (useHostnames) {
        dnsName = addr.getHostName();
      } else {
        dnsName = addr.getHostAddress();
      }
      List<DatanodeDetails> found = nodeManager.getNodesByAddress(dnsName);
      if (found.size() == 0) {
        throw new InvalidHostStringException("Host " + host.getRawHostname()
            + " (" + dnsName + ") is not running any datanodes registered"
            + " with SCM."
            + " Please check the host name.");
      } else if (found.size() == 1) {
        if (host.getPort() != -1 &&
            !validateDNPortMatch(host.getPort(), found.get(0))) {
          throw new InvalidHostStringException("Host " + host.getRawHostname()
              + " is running a datanode registered with SCM,"
              + " but the port number doesn't match."
              + " Please check the port number.");
        }
        results.add(found.get(0));
      } else if (found.size() > 1) {
        DatanodeDetails match = null;
        for (DatanodeDetails dn : found) {
          if (validateDNPortMatch(host.getPort(), dn)) {
            match = dn;
            break;
          }
        }
        if (match == null) {
          throw new InvalidHostStringException("Host " + host.getRawHostname()
              + " is running multiple datanodes registered with SCM,"
              + " but no port numbers match."
              + " Please check the port number.");
        }
        results.add(match);
      }
    }
    return results;
  }

  /**
   * Check if the passed port is used by the given DatanodeDetails object. If
   * it is, return true, otherwise return false.
   * @param port Port number to check if it is used by the datanode
   * @param dn Datanode to check if it is using the given port
   * @return True if port is used by the datanode. False otherwise.
   */
  private static boolean validateDNPortMatch(int port, DatanodeDetails dn) {
    for (DatanodeDetails.Port p : dn.getPorts()) {
      if (p.getValue() == port) {
        return true;
      }
    }
    return false;
  }
  static class HostDefinition {
    private String rawHostname;
    private String hostname;
    private int port;

    HostDefinition(String hostname) throws InvalidHostStringException {
      this.rawHostname = hostname;
      parseHostname();
    }

    public String getRawHostname() {
      return rawHostname;
    }

    public String getHostname() {
      return hostname;
    }

    public int getPort() {
      return port;
    }

    private void parseHostname() throws InvalidHostStringException {
      try {
        // A URI *must* have a scheme, so just create a fake one
        URI uri = new URI("empty://" + rawHostname.trim());
        this.hostname = uri.getHost();
        this.port = uri.getPort();

        if (this.hostname == null) {
          throw new InvalidHostStringException("The string " + rawHostname +
              " does not contain a value hostname or hostname:port definition");
        }
      } catch (URISyntaxException e) {
        throw new InvalidHostStringException(
            "Unable to parse the hoststring " + rawHostname, e);
      }
    }
  }
}
