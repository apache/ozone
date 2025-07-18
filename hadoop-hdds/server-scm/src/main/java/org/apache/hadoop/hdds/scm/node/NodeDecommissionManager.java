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

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManager;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.replication.ReplicationManager;
import org.apache.hadoop.hdds.scm.ha.SCMContext;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to manage datanodes scheduled for maintenance or decommission.
 */
public class NodeDecommissionManager {

  private final ScheduledExecutorService executor;
  private final DatanodeAdminMonitor monitor;

  private final NodeManager nodeManager;
  private ContainerManager containerManager;
  private final SCMContext scmContext;
  private final boolean useHostnames;
  private Integer maintenanceReplicaMinimum;
  private Integer maintenanceRemainingRedundancy;

  // Decommissioning and Maintenance mode progress related metrics.
  private final NodeDecommissionMetrics metrics;

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeDecommissionManager.class);

  static final class HostDefinition {
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
            "Unable to parse the host string " + rawHostname, e);
      }
    }
  }

  private List<DatanodeDetails> mapHostnamesToDatanodes(List<String> hosts,
      List<DatanodeAdminError> errors) {
    List<DatanodeDetails> results = new LinkedList<>();
    HostDefinition host;
    InetAddress addr;
    String msg;
    for (String hostString : hosts) {
      try {
        host = new HostDefinition(hostString);
        addr = InetAddress.getByName(host.getHostname());
      } catch (InvalidHostStringException | UnknownHostException e) {
        LOG.warn("Unable to resolve host {} ", hostString, e);
        errors.add(new DatanodeAdminError(hostString,
            e.getMessage()));
        continue;
      }
      String dnsName;
      if (useHostnames) {
        dnsName = addr.getHostName();
      } else {
        dnsName = addr.getHostAddress();
      }
      List<DatanodeDetails> found = nodeManager.getNodesByAddress(dnsName);
      if (found.isEmpty()) {
        msg = "Host " + host.getRawHostname()
            + " (" + dnsName + ") is not running any datanodes registered"
            + " with SCM. Please check the host name.";
        LOG.warn(msg);
        errors.add(new DatanodeAdminError(host.getRawHostname(), msg));
      } else if (found.size() == 1) {
        if (host.getPort() != -1 &&
            !validateDNPortMatch(host.getPort(), found.get(0))) {
          msg = "Host " + host.getRawHostname()
              + " is running a datanode registered with SCM,"
              + " but the port number doesn't match."
              + " Please check the port number.";
          LOG.warn(msg);
          errors.add(new DatanodeAdminError(host.getRawHostname(), msg));
          continue;
        }
        results.add(found.get(0));
      } else {
        // Here we either have multiple DNs on the same host / IP, and they
        // should have different ports. Or, we have a case where a DN was
        // registered from a host, then stopped and formatted, changing its
        // UUID and registered again. In that case, the ports of all hosts
        // should be the same, and we should just use the one with the most
        // recent heartbeat.
        if (host.getPort() != -1) {
          HostDefinition finalHost = host;
          found.removeIf(dn -> !validateDNPortMatch(finalHost.getPort(), dn));
        }
        if (found.isEmpty()) {
          msg = "Host " + host.getRawHostname()
              + " is running multiple datanodes registered with SCM,"
              + " but no port numbers match."
              + " Please check the port number.";
          LOG.warn(msg);
          errors.add(new DatanodeAdminError(host.getRawHostname(), msg));
          continue;
        } else if (found.size() == 1) {
          results.add(found.get(0));
          continue;
        }
        // Here we have at least 2 DNs matching the passed in port, or no port
        // was passed so we may have all the same ports in SCM or a mix of
        // ports.
        if (allPortsMatch(found)) {
          // All ports match, so just use the most recent heartbeat as it is
          // not possible for a host to have 2 DNs coming from the same port.
          DatanodeDetails mostRecent = findDnWithMostRecentHeartbeat(found);
          if (mostRecent == null) {
            msg = "Host " + host.getRawHostname()
                + " has multiple datanodes registered with SCM."
                + " All have identical ports, but none have a newest"
                + " heartbeat.";
            LOG.warn(msg);
            errors.add(new DatanodeAdminError(host.getRawHostname(), msg));
            continue;
          }
          results.add(mostRecent);
        } else {
          // We have no passed in port or the ports in SCM do not all match, so
          // we cannot decide which DN to use.
          msg = "Host " + host.getRawHostname()
              + " is running multiple datanodes registered with SCM,"
              + " but no port numbers match."
              + " Please check the port number.";
          LOG.warn(msg);
          errors.add(new DatanodeAdminError(host.getRawHostname(), msg));
        }
      }
    }
    return results;
  }

  private boolean allPortsMatch(List<DatanodeDetails> dns) {
    if (dns.size() < 2) {
      return true;
    }
    int port = dns.get(0).getRatisPort().getValue();
    for (int i = 1; i < dns.size(); i++) {
      if (dns.get(i).getRatisPort().getValue()
          != port) {
        return false;
      }
    }
    return true;
  }

  private DatanodeDetails findDnWithMostRecentHeartbeat(
      List<DatanodeDetails> dns) {
    if (dns.size() < 2) {
      return dns.isEmpty() ? null : dns.get(0);
    }
    List<Pair<DatanodeDetails, Long>> dnsWithHeartbeat = dns.stream()
        .map(dn -> Pair.of(dn, nodeManager.getLastHeartbeat(dn)))
        .sorted(Comparator.comparingLong(Pair::getRight))
        .collect(Collectors.toList());
    // The last element should have the largest (newest) heartbeat. But also
    // check it is not identical to the last but 1 element, as then we cannot
    // determine which node to decommission.
    Pair<DatanodeDetails, Long> last = dnsWithHeartbeat.get(
        dnsWithHeartbeat.size() - 1);
    if (last.getRight() > dnsWithHeartbeat.get(
        dnsWithHeartbeat.size() - 2).getRight()) {
      return last.getLeft();
    }
    return null;
  }

  /**
   * Check if the passed port is used by the given DatanodeDetails object. If
   * it is, return true, otherwise return false.
   * @param port Port number to check if it is used by the datanode
   * @param dn Datanode to check if it is using the given port
   * @return True if port is used by the datanode. False otherwise.
   */
  private boolean validateDNPortMatch(int port, DatanodeDetails dn) {
    return dn.hasPort(port);
  }

  public NodeDecommissionManager(OzoneConfiguration config, NodeManager nm, ContainerManager cm,
             SCMContext scmContext,
             EventPublisher eventQueue, ReplicationManager rm) {
    this.nodeManager = nm;
    this.containerManager = cm;
    this.scmContext = scmContext;

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setNameFormat(
                scmContext.threadNamePrefix() + "DatanodeAdminManager-%d")
            .setDaemon(true)
            .build()
    );

    useHostnames = config.getBoolean(
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME,
        HddsConfigKeys.HDDS_DATANODE_USE_DN_HOSTNAME_DEFAULT);

    long monitorIntervalMs = config.getOrFixDuration(
        LOG,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT,
        TimeUnit.MILLISECONDS);

    setMaintenanceConfigs(config.getInt("hdds.scm.replication.maintenance.replica.minimum", 2),
        config.getInt("hdds.scm.replication.maintenance.remaining.redundancy", 1));

    monitor = new DatanodeAdminMonitorImpl(config, eventQueue, nodeManager,
        rm);
    this.metrics = NodeDecommissionMetrics.create();
    monitor.setMetrics(this.metrics);
    executor.scheduleAtFixedRate(monitor, monitorIntervalMs, monitorIntervalMs,
        TimeUnit.MILLISECONDS);
  }

  public Map<String, List<ContainerID>> getContainersPendingReplication(DatanodeDetails dn)
      throws NodeNotFoundException {
    return getMonitor().getContainersPendingReplication(dn);
  }

  @VisibleForTesting
  public DatanodeAdminMonitor getMonitor() {
    return monitor;
  }

  public synchronized List<DatanodeAdminError> decommissionNodes(
      List<String> nodes, boolean force) {
    List<DatanodeAdminError> errors = new ArrayList<>();
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes, errors);
    // add check for fail-early if force flag is not set
    if (!force) {
      LOG.info("Force flag = {}. Checking if decommission is possible for dns: {}", force, dns);
      boolean decommissionPossible = checkIfDecommissionPossible(dns, errors);
      if (!decommissionPossible) {
        LOG.error("Cannot decommission nodes as sufficient node are not available.");
        return errors;
      }
    } else {
      LOG.info("Force flag = {}. Skip checking if decommission is possible for dns: {}", force, dns);
    }
    for (DatanodeDetails dn : dns) {
      try {
        startDecommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to " +
            "decommission it", dn.getHostName());
        errors.add(new DatanodeAdminError(dn.getHostName(),
            "The host was not found in SCM"));
      } catch (InvalidNodeStateException e) {
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }

  /**
   * If a SCM is restarted, then upon re-registration the datanode will already
   * be in DECOMMISSIONING or ENTERING_MAINTENANCE state. In that case, it
   * needs to be added back into the monitor to track its progress.
   * @param dn Datanode to add back to tracking.
   */
  public synchronized void continueAdminForNode(DatanodeDetails dn)
      throws NodeNotFoundException {
    if (!scmContext.isLeader()) {
      LOG.info("follower SCM ignored continue admin for datanode {}", dn);
      return;
    }
    NodeOperationalState opState = getNodeStatus(dn).getOperationalState();
    if (opState == NodeOperationalState.DECOMMISSIONING
        || opState == NodeOperationalState.ENTERING_MAINTENANCE
        || opState == NodeOperationalState.IN_MAINTENANCE) {
      LOG.info("Continue admin for datanode {}", dn);
      monitor.startMonitoring(dn);
    }
  }

  public synchronized void startDecommission(DatanodeDetails dn)
      throws NodeNotFoundException, InvalidNodeStateException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState == NodeOperationalState.IN_SERVICE) {
      LOG.info("Starting Decommission for node {}", dn);
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.DECOMMISSIONING);
      monitor.startMonitoring(dn);
    } else if (nodeStatus.isDecommission()) {
      LOG.info("Start Decommission called on node {} in state {}. Nothing to " +
          "do.", dn, opState);
    } else {
      LOG.error("Cannot decommission node {} in state {}", dn, opState);
      throw new InvalidNodeStateException("Cannot decommission node " +
          dn + " in state " + opState);
    }
  }

  private synchronized boolean checkIfDecommissionPossible(List<DatanodeDetails> dns, List<DatanodeAdminError> errors) {
    int numDecom = dns.size();
    List<DatanodeDetails> validDns = new ArrayList<>(dns);
    int inServiceTotal = nodeManager.getNodeCount(NodeStatus.inServiceHealthy());
    for (DatanodeDetails dn : dns) {
      try {
        NodeStatus nodeStatus = getNodeStatus(dn);
        NodeOperationalState opState = nodeStatus.getOperationalState();
        if (opState != NodeOperationalState.IN_SERVICE) {
          numDecom--;
          validDns.remove(dn);
          LOG.warn("Cannot decommission {} because it is not IN-SERVICE", dn.getHostName());
        }
      } catch (NodeNotFoundException ex) {
        numDecom--;
        validDns.remove(dn);
        LOG.warn("Cannot decommission {} because it is not found in SCM", dn.getHostName());
      }
    }

    for (DatanodeDetails dn : validDns) {
      Set<ContainerID> containers;
      try {
        containers = nodeManager.getContainers(dn);
      } catch (NodeNotFoundException ex) {
        LOG.warn("The host {} was not found in SCM. Ignoring the request to " +
            "decommission it", dn.getHostName());
        continue; // ignore the DN and continue to next one
      }

      for (ContainerID cid : containers) {
        ContainerInfo cif;
        try {
          cif = containerManager.getContainer(cid);
        } catch (ContainerNotFoundException ex) {
          LOG.warn("Could not find container info for container {}.", cid);
          continue; // ignore the container and continue to next one
        }
        synchronized (cif) {
          if (cif.getState().equals(HddsProtos.LifeCycleState.DELETED) ||
              cif.getState().equals(HddsProtos.LifeCycleState.DELETING)) {
            continue;
          }
          int reqNodes = cif.getReplicationConfig().getRequiredNodes();
          if ((inServiceTotal - numDecom) < reqNodes) {
            final int unHealthyTotal = nodeManager.getAllNodeCount() - inServiceTotal;
            String errorMsg = "Insufficient nodes. Tried to decommission " + dns.size() +
                " nodes out of " + inServiceTotal + " IN-SERVICE HEALTHY and " + unHealthyTotal +
                " not IN-SERVICE or not HEALTHY nodes. Cannot decommission as a minimum of " + reqNodes +
                " IN-SERVICE HEALTHY nodes are required to maintain replication after decommission. ";
            LOG.info(errorMsg + "Failing due to datanode : {}, container : {}", dn, cid);
            errors.add(new DatanodeAdminError("AllHosts", errorMsg));
            return false;
          }
        }
      }
    }
    return true;
  }

  public synchronized List<DatanodeAdminError> recommissionNodes(
      List<String> nodes) {
    List<DatanodeAdminError> errors = new ArrayList<>();
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes, errors);
    for (DatanodeDetails dn : dns) {
      try {
        recommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("Host {} was not found in SCM. Ignoring the request to " +
            "recommission it.", dn.getHostName());
        errors.add(new DatanodeAdminError(dn.getHostName(),
            "The host was not found in SCM"));
      }
    }
    return errors;
  }

  public synchronized void recommission(DatanodeDetails dn)
      throws NodeNotFoundException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState != NodeOperationalState.IN_SERVICE) {
      // The node will be set back to IN_SERVICE when it is processed by the
      // monitor
      monitor.stopMonitoring(dn);
      LOG.info("Queued node {} for recommission", dn);
    } else {
      LOG.info("Recommission called on node {} with state {}. " +
          "Nothing to do.", dn, opState);
    }
  }

  public synchronized List<DatanodeAdminError> startMaintenanceNodes(
      List<String> nodes, int endInHours, boolean force) {
    List<DatanodeAdminError> errors = new ArrayList<>();
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes, errors);
    // add check for fail-early if force flag is not set
    if (!force) {
      LOG.info("Force flag = {}. Checking if maintenance is possible for dns: {}", force, dns);
      boolean maintenancePossible = checkIfMaintenancePossible(dns, errors);
      if (!maintenancePossible) {
        LOG.error("Cannot put nodes to maintenance as sufficient node are not available.");
        return errors;
      }
    } else {
      LOG.info("Force flag = {}. Skip checking if maintenance is possible for dns: {}", force, dns);
    }
    for (DatanodeDetails dn : dns) {
      try {
        startMaintenance(dn, endInHours);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to " +
            "start maintenance on it", dn.getHostName());
      } catch (InvalidNodeStateException e) {
        errors.add(new DatanodeAdminError(dn.getHostName(), e.getMessage()));
      }
    }
    return errors;
  }

  // TODO - If startMaintenance is called on a host already in maintenance,
  //        then we should update the end time?
  public synchronized void startMaintenance(DatanodeDetails dn, int endInHours)
      throws NodeNotFoundException, InvalidNodeStateException {
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();

    long maintenanceEnd = 0;
    if (endInHours != 0) {
      maintenanceEnd =
          (System.currentTimeMillis() / 1000L) + (endInHours * 60L * 60L);
    }
    if (opState == NodeOperationalState.IN_SERVICE) {
      nodeManager.setNodeOperationalState(
          dn, NodeOperationalState.ENTERING_MAINTENANCE, maintenanceEnd);
      monitor.startMonitoring(dn);
      LOG.info("Starting Maintenance for node {}", dn);
    } else if (nodeStatus.isMaintenance()) {
      LOG.info("Starting Maintenance called on node {} with state {}. " +
          "Nothing to do.", dn, opState);
    } else {
      LOG.error("Cannot start maintenance on node {} in state {}", dn, opState);
      throw new InvalidNodeStateException("Cannot start maintenance on node " +
          dn + " in state " + opState);
    }
  }

  private synchronized boolean checkIfMaintenancePossible(List<DatanodeDetails> dns, List<DatanodeAdminError> errors) {
    int numMaintenance = dns.size();
    List<DatanodeDetails> validDns = dns.stream().collect(Collectors.toList());
    Collections.copy(validDns, dns);
    int inServiceTotal = nodeManager.getNodeCount(NodeStatus.inServiceHealthy());
    for (DatanodeDetails dn : dns) {
      try {
        NodeStatus nodeStatus = getNodeStatus(dn);
        NodeOperationalState opState = nodeStatus.getOperationalState();
        if (opState != NodeOperationalState.IN_SERVICE) {
          numMaintenance--;
          validDns.remove(dn);
          LOG.warn("{} cannot enter maintenance because it is not IN-SERVICE", dn.getHostName());
        }
      } catch (NodeNotFoundException ex) {
        numMaintenance--;
        validDns.remove(dn);
        LOG.warn("{} cannot enter maintenance because it is not found in SCM", dn.getHostName());
      }
    }

    for (DatanodeDetails dn : validDns) {
      Set<ContainerID> containers;
      try {
        containers = nodeManager.getContainers(dn);
      } catch (NodeNotFoundException ex) {
        LOG.warn("The host {} was not found in SCM. Ignoring the request to " +
            "enter maintenance", dn.getHostName());
        errors.add(new DatanodeAdminError(dn.getHostName(),
            "The host was not found in SCM"));
        continue; // ignore the DN and continue to next one
      }

      for (ContainerID cid : containers) {
        ContainerInfo cif;
        try {
          cif = containerManager.getContainer(cid);
        } catch (ContainerNotFoundException ex) {
          continue; // ignore the container and continue to next one
        }
        synchronized (cif) {
          if (cif.getState().equals(HddsProtos.LifeCycleState.DELETED) ||
              cif.getState().equals(HddsProtos.LifeCycleState.DELETING)) {
            continue;
          }

          int minInService;
          HddsProtos.ReplicationType replicationType = cif.getReplicationType();
          if (replicationType.equals(HddsProtos.ReplicationType.EC)) {
            int reqNodes = cif.getReplicationConfig().getRequiredNodes();
            int data = ((ECReplicationConfig)cif.getReplicationConfig()).getData();
            minInService = Math.min((data + maintenanceRemainingRedundancy), reqNodes);
          } else {
            minInService = maintenanceReplicaMinimum;
          }
          if ((inServiceTotal - numMaintenance) < minInService) {
            final int unHealthyTotal = nodeManager.getAllNodeCount() - inServiceTotal;
            String errorMsg = "Insufficient nodes. Tried to start maintenance for " + dns.size() +
                " nodes out of " + inServiceTotal + " IN-SERVICE HEALTHY and " + unHealthyTotal +
                " not IN-SERVICE or not HEALTHY nodes. Cannot enter maintenance mode as a minimum of " + minInService +
                " IN-SERVICE HEALTHY nodes are required to maintain replication after maintenance. ";
            LOG.info(errorMsg + "Failing due to datanode : {}, container : {}", dn, cid);
            errors.add(new DatanodeAdminError("AllHosts", errorMsg));
            return false;
          }
        }
      }
    }
    return true;
  }

  /**
   *  Stops the decommission monitor from running when SCM is shutdown.
   */
  public void stop() {
    metrics.unRegister();
    if (executor != null) {
      executor.shutdown();
    }
  }

  private NodeStatus getNodeStatus(DatanodeDetails dn)
      throws NodeNotFoundException {
    return nodeManager.getNodeStatus(dn);
  }

  /**
   * Called in SCMStateMachine#notifyLeaderChanged when current SCM becomes
   *  leader.
   */
  public void onBecomeLeader() {
    nodeManager.getAllNodes().forEach(datanodeDetails -> {
      try {
        continueAdminForNode(datanodeDetails);
      } catch (NodeNotFoundException e) {
        // Should not happen, as the node has just registered to call this event
        // handler.
        LOG.warn("NodeNotFound when adding the node to the decommissionManager",
            e);
      }
    });
  }

  @VisibleForTesting
  public void setMaintenanceConfigs(int replicaMinimum, int remainingRedundancy) {
    synchronized (this) {
      maintenanceRemainingRedundancy = remainingRedundancy;
      maintenanceReplicaMinimum = replicaMinimum;
    }
  }
}
