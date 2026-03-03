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

package org.apache.hadoop.ozone.om.helpers;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeType;
import org.apache.hadoop.ozone.OzoneFsServerDefaults;
import org.apache.hadoop.ozone.OzoneManagerVersion;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.OMRoleInfo;
import org.apache.hadoop.ozone.protocol.proto.OzoneManagerProtocolProtos.ServicePort;

/**
 * ServiceInfo holds the config details of Ozone services.
 */
public final class ServiceInfo {

  /**
   * Type of node/service.
   */
  private NodeType nodeType;
  /**
   * Hostname of the node in which the service is running.
   */
  private String hostname;

  /**
   * Protocol version for client.
   */
  private OzoneManagerVersion omVersion;

  /**
   * List of ports the service listens to.
   */
  private Map<ServicePort.Type, Integer> ports;
  
  private OMRoleInfo omRoleInfo;
  private OzoneFsServerDefaults serverDefaults;

  /**
   * Default constructor for JSON deserialization.
   */
  public ServiceInfo() { }

  /**
   * Constructs the ServiceInfo for the {@code nodeType}.
   * @param nodeType type of node/service
   * @param hostname hostname of the service
   * @param portList list of ports the service listens to
   */
  private ServiceInfo(NodeType nodeType,
                      String hostname,
                      List<ServicePort> portList,
                      OzoneManagerVersion omVersion,
                      OMRoleInfo omRole) {
    this(nodeType, hostname, portList, omVersion, omRole, null);
  }

  /**
   * Constructs the ServiceInfo for the {@code nodeType}.
   * @param nodeType type of node/service
   * @param hostname hostname of the service
   * @param portList list of ports the service listens to
   * @param omVersion Om Version
   * @param omRole OM role Ino
   * @param keyProviderUri KMS provider URI
   */
  private ServiceInfo(NodeType nodeType,
                      String hostname,
                      List<ServicePort> portList,
                      OzoneManagerVersion omVersion,
                      OMRoleInfo omRole,
                      OzoneFsServerDefaults serverDefaults) {
    Objects.requireNonNull(nodeType, "nodeType == null");
    Objects.requireNonNull(hostname, "hostname == null");
    this.nodeType = nodeType;
    this.hostname = hostname;
    this.omVersion = omVersion;
    this.ports = new HashMap<>();
    for (ServicePort port : portList) {
      ports.put(port.getType(), port.getValue());
    }
    this.omRoleInfo = omRole;
    this.serverDefaults = serverDefaults;
  }

  /**
   * Returns the type of node/service.
   * @return node type
   */
  public NodeType getNodeType() {
    return nodeType;
  }

  /**
   * Returns the hostname of the service.
   * @return hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * Returns ServicePort.Type to port mappings.
   * @return ports
   */
  public Map<ServicePort.Type, Integer> getPorts() {
    return ports;
  }

  /**
   * Returns the port for given type.
   *
   * @param type the type of port.
   *             ex: RPC, HTTP, HTTPS, etc..
   * @throws NullPointerException if the service doesn't support the given type
   */
  @JsonIgnore
  public int getPort(ServicePort.Type type) {
    return ports.get(type);
  }

  /**
   * Returns the address of the service (hostname with port of the given type).
   * @param portType the type of port, eg. RPC, HTTP, etc.
   * @return service address (hostname with port of the given type)
   */
  @JsonIgnore
  public String getServiceAddress(ServicePort.Type portType) {
    return hostname + ":" + getPort(portType);
  }

  /**
   * Returns the OM role info - node id and ratis server role.
   * @return OmRoleInfo
   */
  @JsonIgnore
  public OMRoleInfo getOmRoleInfo() {
    return omRoleInfo;
  }

  /**
   * Returns the Ozone Server default configuration.
   * @return OmRoleInfo
   */
  @JsonIgnore
  public OzoneFsServerDefaults getServerDefaults() {
    return serverDefaults;
  }

  /**
   * Converts {@link ServiceInfo} to OzoneManagerProtocolProtos.ServiceInfo.
   *
   * @return OzoneManagerProtocolProtos.ServiceInfo
   */
  @JsonIgnore
  public OzoneManagerProtocolProtos.ServiceInfo getProtobuf() {
    OzoneManagerProtocolProtos.ServiceInfo.Builder builder =
        OzoneManagerProtocolProtos.ServiceInfo.newBuilder();

    List<ServicePort> servicePorts = new ArrayList<>();
    for (Map.Entry<ServicePort.Type, Integer>
        entry : ports.entrySet()) {
      servicePorts.add(ServicePort.newBuilder()
          .setType(entry.getKey())
          .setValue(entry.getValue()).build());
    }

    builder.setNodeType(nodeType)
        .setHostname(hostname)
        .addAllServicePorts(servicePorts);
    if (omVersion != null) {
      builder.setOMVersion(omVersion.serialize());
    }
    if (nodeType == NodeType.OM && omRoleInfo != null) {
      builder.setOmRole(omRoleInfo);
    }
    if (serverDefaults != null) {
      builder.setServerDefaults(serverDefaults.getProtobuf());
    }
    return builder.build();
  }

  /**
   * Converts OzoneManagerProtocolProtos.ServiceInfo to {@link ServiceInfo}.
   *
   * @return {@link ServiceInfo}
   */
  @JsonIgnore
  public static ServiceInfo getFromProtobuf(
      OzoneManagerProtocolProtos.ServiceInfo serviceInfo) {
    return new ServiceInfo(serviceInfo.getNodeType(),
        serviceInfo.getHostname(),
        serviceInfo.getServicePortsList(),
        OzoneManagerVersion.deserialize(serviceInfo.getOMVersion()),
        serviceInfo.hasOmRole() ? serviceInfo.getOmRole() : null,
        serviceInfo.hasServerDefaults() ? OzoneFsServerDefaults.getFromProtobuf(
            serviceInfo.getServerDefaults()) : null);
  }

  /**
   * Creates a new builder to build {@link ServiceInfo}.
   * @return {@link ServiceInfo.Builder}
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder used to build/construct {@link ServiceInfo}.
   */
  public static class Builder {

    private NodeType node;
    private String host;
    private List<ServicePort> portList = new ArrayList<>();
    private OMRoleInfo omRoleInfo;
    private OzoneManagerVersion omVersion;
    private OzoneFsServerDefaults serverDefaults;

    /**
     * Gets the Om Client Protocol Version.
     * @return om client protocol version as a string.
     */
    public OzoneManagerVersion getOmVersion() {
      return omVersion;
    }

    /**
     * Sets the Om Client Protocol Version.
     * @param omClientProtocolVer the client protocol version supported.
     */
    public Builder setOmVersion(OzoneManagerVersion omClientProtocolVer) {
      this.omVersion = omClientProtocolVer;
      return this;
    }

    /**
     * Sets the node/service type.
     * @param nodeType type of node
     * @return the builder
     */
    public Builder setNodeType(NodeType nodeType) {
      node = nodeType;
      return this;
    }

    /**
     * Sets the hostname of the service.
     * @param hostname service hostname
     * @return the builder
     */
    public Builder setHostname(String hostname) {
      host = hostname;
      return this;
    }

    /**
     * Adds the service port to the service port list.
     * @param servicePort RPC port
     * @return the builder
     */
    public Builder addServicePort(ServicePort servicePort) {
      portList.add(servicePort);
      return this;
    }

    public Builder setOmRoleInfo(OMRoleInfo omRole) {
      omRoleInfo = omRole;
      return this;
    }

    public Builder setServerDefaults(OzoneFsServerDefaults defaults) {
      serverDefaults = defaults;
      return this;
    }

    /**
     * Builds and returns {@link ServiceInfo} with the set values.
     * @return {@link ServiceInfo}
     */
    public ServiceInfo build() {
      return new ServiceInfo(node,
          host,
          portList,
          omVersion,
          omRoleInfo,
          serverDefaults);
    }
  }

}
