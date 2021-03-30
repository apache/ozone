/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.DatanodeAdminError;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ReplicationManager;
import org.apache.hadoop.hdds.scm.node.states.NodeNotFoundException;
import org.apache.hadoop.hdds.server.events.EventPublisher;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Class used to manage datanodes scheduled for maintenance or decommission.
 */
public class NodeDecommissionManager {

  private ScheduledExecutorService executor;
  private DatanodeAdminMonitor monitor;

  private NodeManager nodeManager;
  //private ContainerManagerV2 containerManager;
  private EventPublisher eventQueue;
  private ReplicationManager replicationManager;
  private OzoneConfiguration conf;
  private boolean useHostnames;
  private long monitorInterval;

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeDecommissionManager.class);

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

    private void parseHostname() throws InvalidHostStringException{
      try {
        // A URI *must* have a scheme, so just create a fake one
        URI uri = new URI("empty://"+rawHostname.trim());
        this.hostname = uri.getHost();
        this.port = uri.getPort();

        if (this.hostname == null) {
          throw new InvalidHostStringException("The string "+rawHostname+
              " does not contain a value hostname or hostname:port definition");
        }
      } catch (URISyntaxException e) {
        throw new InvalidHostStringException(
            "Unable to parse the hoststring "+rawHostname, e);
      }
    }
  }

  private List<DatanodeDetails> mapHostnamesToDatanodes(List<String> hosts)
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
        for(DatanodeDetails dn : found) {
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
  private boolean validateDNPortMatch(int port, DatanodeDetails dn) {
    for (DatanodeDetails.Port p : dn.getPorts()) {
      if (p.getValue() == port) {
        return true;
      }
    }
    return false;
  }

  public NodeDecommissionManager(OzoneConfiguration config, NodeManager nm,
      ContainerManagerV2 containerManager,
      EventPublisher eventQueue, ReplicationManager rm) {
    this.nodeManager = nm;
    conf = config;
    //this.containerManager = containerManager;
    this.eventQueue = eventQueue;
    this.replicationManager = rm;

    executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder().setNameFormat("DatanodeAdminManager-%d")
            .setDaemon(true).build());

    useHostnames = conf.getBoolean(
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME,
        DFSConfigKeys.DFS_DATANODE_USE_DN_HOSTNAME_DEFAULT);

    monitorInterval = conf.getTimeDuration(
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
        ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT,
        TimeUnit.SECONDS);
    if (monitorInterval <= 0) {
      LOG.warn("{} must be greater than zero, defaulting to {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT);
      conf.set(ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT);
      monitorInterval = conf.getTimeDuration(
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL,
          ScmConfigKeys.OZONE_SCM_DATANODE_ADMIN_MONITOR_INTERVAL_DEFAULT,
          TimeUnit.SECONDS);
    }

    monitor = new DatanodeAdminMonitorImpl(conf, eventQueue, nodeManager,
        replicationManager);

    executor.scheduleAtFixedRate(monitor, monitorInterval, monitorInterval,
        TimeUnit.SECONDS);
  }

  @VisibleForTesting
  public DatanodeAdminMonitor getMonitor() {
    return monitor;
  }

  public synchronized List<DatanodeAdminError> decommissionNodes(
      List<String> nodes) throws InvalidHostStringException {
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes);
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        startDecommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to "+
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
   * @throws NodeNotFoundException
   */
  public synchronized void continueAdminForNode(DatanodeDetails dn)
      throws NodeNotFoundException {
    NodeOperationalState opState = getNodeStatus(dn).getOperationalState();
    if (opState == NodeOperationalState.DECOMMISSIONING
        || opState == NodeOperationalState.ENTERING_MAINTENANCE
        || opState == NodeOperationalState.IN_MAINTENANCE) {
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
      LOG.info("Start Decommission called on node {} in state {}. Nothing to "+
          "do.", dn, opState);
    } else {
      LOG.error("Cannot decommission node {} in state {}", dn, opState);
      throw new InvalidNodeStateException("Cannot decommission node "+
          dn +" in state "+ opState);
    }
  }

  public synchronized List<DatanodeAdminError> recommissionNodes(
      List<String> nodes) throws InvalidHostStringException {
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes);
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        recommission(dn);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("Host {} was not found in SCM. Ignoring the request to "+
            "recommission it.", dn.getHostName());
        errors.add(new DatanodeAdminError(dn.getHostName(),
            "The host was not found in SCM"));
      }
    }
    return errors;
  }

  public synchronized void recommission(DatanodeDetails dn)
      throws NodeNotFoundException{
    NodeStatus nodeStatus = getNodeStatus(dn);
    NodeOperationalState opState = nodeStatus.getOperationalState();
    if (opState != NodeOperationalState.IN_SERVICE) {
      // The node will be set back to IN_SERVICE when it is processed by the
      // monitor
      monitor.stopMonitoring(dn);
      LOG.info("Queued node {} for recommission", dn);
    } else {
      LOG.info("Recommission called on node {} with state {}. "+
          "Nothing to do.", dn, opState);
    }
  }

  public synchronized List<DatanodeAdminError> startMaintenanceNodes(
      List<String> nodes, int endInHours) throws InvalidHostStringException {
    List<DatanodeDetails> dns = mapHostnamesToDatanodes(nodes);
    List<DatanodeAdminError> errors = new ArrayList<>();
    for (DatanodeDetails dn : dns) {
      try {
        startMaintenance(dn, endInHours);
      } catch (NodeNotFoundException e) {
        // We already validated the host strings and retrieved the DnDetails
        // object from the node manager. Therefore we should never get a
        // NodeNotFoundException here expect if the node is remove in the
        // very short window between validation and starting decom. Therefore
        // log a warning and ignore the exception
        LOG.warn("The host {} was not found in SCM. Ignoring the request to "+
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
      LOG.info("Starting Maintenance called on node {} with state {}. "+
          "Nothing to do.", dn, opState);
    } else {
      LOG.error("Cannot start maintenance on node {} in state {}", dn, opState);
      throw new InvalidNodeStateException("Cannot start maintenance on node "+
          dn +" in state "+ opState);
    }
  }

  /**
   *  Stops the decommission monitor from running when SCM is shutdown.
   */
  public void stop() {
    if (executor != null) {
      executor.shutdown();
    }
  }

  private NodeStatus getNodeStatus(DatanodeDetails dn)
      throws NodeNotFoundException {
    return nodeManager.getNodeStatus(dn);
  }

}
