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

package org.apache.hadoop.hdds.protocol;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.HADOOP_PRC_PORTS_IN_DATANODEDETAILS;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.RATIS_DATASTREAM_PORT_IN_DATANODEDETAILS;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.WEBUI_PORTS_IN_DATANODEDETAILS;
import static org.apache.hadoop.ozone.ClientVersion.VERSION_HANDLES_UNKNOWN_DN_PORTS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hdds.HDDSVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NetUtils;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.upgrade.BelongsToHDDSLayoutVersion;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.apache.hadoop.ozone.util.StringWithByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DatanodeDetails class contains details about DataNode like:
 * - UUID of the DataNode.
 * - IP and Hostname details.
 * - Port details to which the DataNode will be listening.
 * and may also include some extra info like:
 * - version of the DataNode
 * - setup time etc.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DatanodeDetails extends NodeImpl implements Comparable<DatanodeDetails> {

  private static final Logger LOG = LoggerFactory.getLogger(DatanodeDetails.class);

  private static final Codec<DatanodeDetails> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(ExtendedDatanodeDetailsProto.getDefaultInstance()),
      DatanodeDetails::getFromProtoBuf,
      DatanodeDetails::getExtendedProtoBufMessage,
      DatanodeDetails.class);

  /**
   * DataNode's unique identifier in the cluster.
   */
  private final DatanodeID id;
  private final String threadNamePrefix;
  private StringWithByteString ipAddress;
  private StringWithByteString hostName;
  private final List<Port> ports;
  private String certSerialId;
  private String version;
  private long setupTime;
  private String revision;
  private volatile HddsProtos.NodeOperationalState persistedOpState;
  private volatile long persistedOpStateExpiryEpochSec;
  private int initialVersion;
  private int currentVersion;

  private DatanodeDetails(Builder b) {
    super(b.hostName, b.networkLocation, NetConstants.NODE_COST_DEFAULT);
    id = b.id;
    threadNamePrefix = HddsUtils.threadNamePrefix(id.toString());
    ipAddress = b.ipAddress;
    hostName = b.hostName;
    ports = b.ports;
    certSerialId = b.certSerialId;
    version = b.version;
    setupTime = b.setupTime;
    revision = b.revision;
    persistedOpState = b.persistedOpState;
    persistedOpStateExpiryEpochSec = b.persistedOpStateExpiryEpochSec;
    initialVersion = b.initialVersion;
    currentVersion = b.currentVersion;
    if (b.networkName != null) {
      setNetworkName(b.networkName);
    }
    if (b.level > 0) {
      setLevel(b.level);
    }
  }

  public DatanodeDetails(DatanodeDetails datanodeDetails) {
    super(datanodeDetails.getHostNameAsByteString(), datanodeDetails.getNetworkLocationAsByteString(),
        datanodeDetails.getParent(), datanodeDetails.getLevel(),
        datanodeDetails.getCost());
    this.id = datanodeDetails.id;
    threadNamePrefix = HddsUtils.threadNamePrefix(id.toString());
    this.ipAddress = datanodeDetails.ipAddress;
    this.hostName = datanodeDetails.hostName;
    this.ports = datanodeDetails.ports;
    this.certSerialId = datanodeDetails.certSerialId;
    this.setNetworkName(datanodeDetails.getNetworkNameAsByteString());
    this.setParent(datanodeDetails.getParent());
    this.version = datanodeDetails.version;
    this.setupTime = datanodeDetails.setupTime;
    this.revision = datanodeDetails.revision;
    this.persistedOpState = datanodeDetails.getPersistedOpState();
    this.persistedOpStateExpiryEpochSec =
        datanodeDetails.getPersistedOpStateExpiryEpochSec();
    this.initialVersion = datanodeDetails.getInitialVersion();
    this.currentVersion = datanodeDetails.getCurrentVersion();
  }

  public static Codec<DatanodeDetails> getCodec() {
    return CODEC;
  }

  public DatanodeID getID() {
    return id;
  }

  /**
   * Returns the DataNode UUID.
   *
   * @return UUID of DataNode
   */
  // TODO: Remove this in follow-up Jira (HDDS-12015)
  @Deprecated
  public UUID getUuid() {
    return id.getUuid();
  }

  /**
   * Returns the string representation of DataNode UUID.
   *
   * @return UUID of DataNode
   */
  public String getUuidString() {
    return id.toString();
  }

  /**
   * Sets the IP address of Datanode.
   *
   * @param ip IP Address
   */
  public void setIpAddress(String ip) {
    this.ipAddress = StringWithByteString.valueOf(ip);
  }

  /**
   * Resolves and validates the IP address of the datanode based on its hostname.
   * If the resolved IP address differs from the current IP address,
   * it updates the IP address to the newly resolved value.
   */
  public void validateDatanodeIpAddress() {
    final String oldIP = getIpAddress();
    final String hostname = getHostName();
    if (StringUtils.isBlank(hostname)) {
      LOG.warn("Could not resolve IP address of datanode '{}'", this);
      return;
    }

    try {
      final String newIP = InetAddress.getByName(hostname).getHostAddress();
      if (StringUtils.isBlank(newIP)) {
        throw new UnknownHostException("New IP address is invalid: " + newIP);
      }

      if (!newIP.equals(oldIP)) {
        LOG.info("Updating IP address of datanode {} to {}", this, newIP);
        setIpAddress(newIP);
      }
    } catch (UnknownHostException e) {
      LOG.warn("Could not resolve IP address of datanode '{}'", this, e);
    }
  }

  /**
   * Returns IP address of DataNode.
   *
   * @return IP address
   */
  public String getIpAddress() {
    return ipAddress == null ? null : ipAddress.getString();
  }

  /**
   * Returns IP address of DataNode as a StringWithByteString object.
   *
   * @return IP address as ByteString
   */
  public StringWithByteString getIpAddressAsByteString() {
    return ipAddress;
  }

  /**
   * Sets the Datanode hostname.
   *
   * @param host hostname
   */
  public void setHostName(String host) {
    this.hostName = StringWithByteString.valueOf(host);
  }

  /**
   * Returns Hostname of DataNode.
   *
   * @return Hostname
   */
  public String getHostName() {
    return hostName == null ? null : hostName.getString();
  }

  /**
   * Returns IP address of DataNode as a StringWithByteString object.
   *
   * @return Hostname
   */
  public StringWithByteString getHostNameAsByteString() {
    return hostName;
  }

  /**
   * Sets a DataNode Port.
   *
   * @param port DataNode port
   */
  public synchronized void setPort(Port port) {
    // If the port is already in the list remove it first and add the
    // new/updated port value.
    ports.remove(port);
    ports.add(port);
  }

  public synchronized void setPort(Name name, int port) {
    setPort(new Port(name, port));
  }

  public void setRatisPort(int port) {
    setPort(Name.RATIS, port);
  }

  public void setRestPort(int port) {
    setPort(Name.REST, port);
  }

  public void setStandalonePort(int port) {
    setPort(Name.STANDALONE, port);
  }

  /**
   * Returns all the Ports used by DataNode.
   *
   * @return DataNode Ports
   */
  public synchronized List<Port> getPorts() {
    return new ArrayList<>(ports);
  }

  public synchronized boolean hasPort(int port) {
    for (Port p : ports) {
      if (p.getValue() == port) {
        return true;
      }
    }
    return false;
  }

  /**
   * Return the persistedOpState. If the stored value is null, return the
   * default value of IN_SERVICE.
   *
   * @return The OperationalState persisted on the datanode.
   */
  public HddsProtos.NodeOperationalState getPersistedOpState() {
    if (persistedOpState == null) {
      return HddsProtos.NodeOperationalState.IN_SERVICE;
    } else {
      return persistedOpState;
    }
  }

  /**
   * @return true if the node is or being decommissioned
   */
  public boolean isDecommissioned() {
    return isDecommission(getPersistedOpState());
  }

  public static boolean isDecommission(HddsProtos.NodeOperationalState state) {
    return state == HddsProtos.NodeOperationalState.DECOMMISSIONED ||
        state == HddsProtos.NodeOperationalState.DECOMMISSIONING;
  }

  /**
   * @return true if node is in or entering maintenance
   */
  public boolean isMaintenance() {
    return isMaintenance(getPersistedOpState());
  }

  public static boolean isMaintenance(HddsProtos.NodeOperationalState state) {
    return state == HddsProtos.NodeOperationalState.IN_MAINTENANCE ||
        state == HddsProtos.NodeOperationalState.ENTERING_MAINTENANCE;
  }

  /**
   * Set the persistedOpState for this instance.
   *
   * @param state The new operational state.
   */
  public void setPersistedOpState(HddsProtos.NodeOperationalState state) {
    this.persistedOpState = state;
  }

  /**
   * Get the persistedOpStateExpiryEpochSec for the instance.
   * @return Seconds from the epoch when the operational state should expire.
   */
  public long getPersistedOpStateExpiryEpochSec() {
    return persistedOpStateExpiryEpochSec;
  }

  /**
   * Set persistedOpStateExpiryEpochSec.
   * @param expiry The number of second after the epoch the operational state
   *               should expire.
   */
  public void setPersistedOpStateExpiryEpochSec(long expiry) {
    this.persistedOpStateExpiryEpochSec = expiry;
  }

  /**
   * Given the name returns port number, null if the asked port is not found.
   *
   * @param name Name of the port
   *
   * @return Port
   */
  public synchronized Port getPort(Port.Name name) {
    Port ratisPort = null;
    for (Port port : ports) {
      if (port.getName().equals(name)) {
        return port;
      }
      if (port.getName().equals(Name.RATIS)) {
        ratisPort = port;
      }
    }
    // if no separate admin/server/datastream port,
    // return single Ratis one for compatibility
    if (name == Name.RATIS_ADMIN || name == Name.RATIS_SERVER ||
        name == Name.RATIS_DATASTREAM) {
      return ratisPort;
    }
    return null;
  }

  /**
   * Helper method to get the Ratis port.
   * 
   * @return Port
   */
  public Port getRatisPort() {
    return getPort(Name.RATIS);
  }

  /**
   * Helper method to get the REST port.
   *
   * @return Port
   */
  public Port getRestPort() {
    return getPort(Name.REST);
  }

  /**
   * Helper method to get the Standalone port.
   *
   * @return Port
   */
  public Port getStandalonePort() {
    return getPort(Name.STANDALONE);
  }

  /**
   * Starts building a new DatanodeDetails from the protobuf input.
   *
   * @param datanodeDetailsProto protobuf message
   */
  public static DatanodeDetails.Builder newBuilder(
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto) {
    DatanodeDetails.Builder builder = newBuilder();

    if (datanodeDetailsProto.hasId()) {
      builder.setID(DatanodeID.fromProto(datanodeDetailsProto.getId()));
      // The else parts are for backward compatibility.
    } else if (datanodeDetailsProto.hasUuid128()) {
      HddsProtos.UUID uuid = datanodeDetailsProto.getUuid128();
      builder.setID(DatanodeID.of(new UUID(
          uuid.getMostSigBits(), uuid.getLeastSigBits())));
    } else if (datanodeDetailsProto.hasUuid()) {
      builder.setID(DatanodeID.fromUuidString(datanodeDetailsProto.getUuid()));
    }

    if (datanodeDetailsProto.hasIpAddress()) {
      builder.setIpAddress(datanodeDetailsProto.getIpAddress(), datanodeDetailsProto.getIpAddressBytes());
    }
    if (datanodeDetailsProto.hasHostName()) {
      builder.setHostName(datanodeDetailsProto.getHostName(), datanodeDetailsProto.getHostNameBytes());
    }
    if (datanodeDetailsProto.hasCertSerialId()) {
      builder.setCertSerialId(datanodeDetailsProto.getCertSerialId());
    }
    for (HddsProtos.Port port : datanodeDetailsProto.getPortsList()) {
      try {
        builder.addPort(Port.fromProto(port));
      } catch (IllegalArgumentException ignored) {
        // ignore unknown port type
      }
    }
    if (datanodeDetailsProto.hasNetworkName()) {
      builder.setNetworkName(
          datanodeDetailsProto.getNetworkName(), datanodeDetailsProto.getNetworkNameBytes());
    }
    if (datanodeDetailsProto.hasNetworkLocation()) {
      builder.setNetworkLocation(
          datanodeDetailsProto.getNetworkLocation(), datanodeDetailsProto.getNetworkLocationBytes());
    }
    if (datanodeDetailsProto.hasLevel()) {
      builder.setLevel(datanodeDetailsProto.getLevel());
    }
    if (datanodeDetailsProto.hasPersistedOpState()) {
      builder.setPersistedOpState(datanodeDetailsProto.getPersistedOpState());
    }
    if (datanodeDetailsProto.hasPersistedOpStateExpiry()) {
      builder.setPersistedOpStateExpiry(
          datanodeDetailsProto.getPersistedOpStateExpiry());
    }
    if (datanodeDetailsProto.hasCurrentVersion()) {
      builder.setCurrentVersion(datanodeDetailsProto.getCurrentVersion());
    } else {
      // fallback to version 1 if not present
      builder.setCurrentVersion(HDDSVersion.SEPARATE_RATIS_PORTS_AVAILABLE.serialize());
    }
    return builder;
  }

  /**
   * Returns a DatanodeDetails from the protocol buffers.
   *
   * @param datanodeDetailsProto - protoBuf Message
   * @return DatanodeDetails
   */
  public static DatanodeDetails getFromProtoBuf(
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto) {
    return newBuilder(datanodeDetailsProto).build();
  }

  /**
   * Returns a ExtendedDatanodeDetails from the protocol buffers.
   *
   * @param extendedDetailsProto - protoBuf Message
   * @return DatanodeDetails
   */
  public static DatanodeDetails getFromProtoBuf(
      ExtendedDatanodeDetailsProto extendedDetailsProto) {
    DatanodeDetails.Builder builder;
    if (extendedDetailsProto.hasDatanodeDetails()) {
      builder = newBuilder(extendedDetailsProto.getDatanodeDetails());
    } else {
      builder = newBuilder();
    }
    if (extendedDetailsProto.hasVersion()) {
      builder.setVersion(extendedDetailsProto.getVersion());
    }
    if (extendedDetailsProto.hasSetupTime()) {
      builder.setSetupTime(extendedDetailsProto.getSetupTime());
    }
    if (extendedDetailsProto.hasRevision()) {
      builder.setRevision(extendedDetailsProto.getRevision());
    }
    return builder.build();
  }

  /**
   * Returns a DatanodeDetails protobuf message from a datanode ID.
   * @return HddsProtos.DatanodeDetailsProto
   */
  @JsonIgnore
  public HddsProtos.DatanodeDetailsProto getProtoBufMessage() {
    return toProto(ClientVersion.CURRENT.serialize());
  }

  public HddsProtos.DatanodeDetailsProto toProto(int clientVersion) {
    return toProtoBuilder(clientVersion, Collections.emptySet()).build();
  }

  public HddsProtos.DatanodeDetailsProto toProto(int clientVersion, Set<Port.Name> filterPorts) {
    return toProtoBuilder(clientVersion, filterPorts).build();
  }

  /**
   * Converts the current DatanodeDetails instance into a proto {@link HddsProtos.DatanodeDetailsProto.Builder} object.
   *
   * @param clientVersion - The client version.
   * @param filterPorts   - A set of {@link Port.Name} specifying ports to include.
   *                        If empty, all available ports will be included.
   * @return A {@link HddsProtos.DatanodeDetailsProto.Builder} Object.
   */
  public HddsProtos.DatanodeDetailsProto.Builder toProtoBuilder(
      int clientVersion, Set<Port.Name> filterPorts) {

    final HddsProtos.DatanodeIDProto idProto = id.toProto();
    final HddsProtos.DatanodeDetailsProto.Builder builder =
        HddsProtos.DatanodeDetailsProto.newBuilder();

    builder.setId(idProto);
    // Both are deprecated.
    builder.setUuid128(idProto.getUuid());
    builder.setUuidBytes(id.getByteString());

    if (ipAddress != null) {
      builder.setIpAddressBytes(ipAddress.getBytes());
    }
    if (hostName != null) {
      builder.setHostNameBytes(hostName.getBytes());
    }
    if (certSerialId != null) {
      builder.setCertSerialId(certSerialId);
    }
    if (!Strings.isNullOrEmpty(getNetworkName())) {
      builder.setNetworkNameBytes(getNetworkNameAsByteString().getBytes());
    }
    if (!Strings.isNullOrEmpty(getNetworkLocation())) {
      builder.setNetworkLocationBytes(getNetworkLocationAsByteString().getBytes());
    }
    if (getLevel() > 0) {
      builder.setLevel(getLevel());
    }
    if (persistedOpState != null) {
      builder.setPersistedOpState(persistedOpState);
    }
    builder.setPersistedOpStateExpiry(persistedOpStateExpiryEpochSec);

    final boolean handlesUnknownPorts =
        ClientVersion.deserialize(clientVersion)
        .compareTo(VERSION_HANDLES_UNKNOWN_DN_PORTS) >= 0;
    final int requestedPortCount = filterPorts.size();
    final boolean maySkip = requestedPortCount > 0;
    for (Port port : ports) {
      if (maySkip && !filterPorts.contains(port.getName())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip adding {} port {} to proto message",
                  port.getName(), port.getValue());
        }
      } else if (handlesUnknownPorts || Name.V0_PORTS.contains(port.getName())) {
        builder.addPorts(port.toProto());
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip adding {} port {} to proto message for client v{}",
                  port.getName(), port.getValue(), clientVersion);
        }
      }
      if (maySkip && builder.getPortsCount() == requestedPortCount) {
        break;
      }
    }

    builder.setCurrentVersion(currentVersion);

    return builder;
  }

  /**
   * Returns a ExtendedDatanodeDetails protobuf message from a datanode ID.
   * @return ExtendedDatanodeDetailsProto
   */
  @JsonIgnore
  public ExtendedDatanodeDetailsProto getExtendedProtoBufMessage() {
    final ExtendedDatanodeDetailsProto.Builder extendedBuilder
        = ExtendedDatanodeDetailsProto.newBuilder()
            .setDatanodeDetails(getProtoBufMessage());

    if (!Strings.isNullOrEmpty(getVersion())) {
      extendedBuilder.setVersion(getVersion());
    }

    extendedBuilder.setSetupTime(getSetupTime());

    if (!Strings.isNullOrEmpty(getRevision())) {
      extendedBuilder.setRevision(getRevision());
    }

    return extendedBuilder.build();
  }

  /**
   * Note: Datanode initial version is not passed to the client due to no use case. See HDDS-9884
   * @return the version this datanode was initially created with
   */
  public int getInitialVersion() {
    return initialVersion;
  }

  public void setInitialVersion(int initialVersion) {
    this.initialVersion = initialVersion;
  }

  /**
   * @return the version this datanode was last started with
   */
  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  @Override
  public String toString() {
    return id + "(" + hostName + "/" + ipAddress + ")";
  }

  public String toDebugString() {
    return id + "{" +
        "ip: " +
        ipAddress +
        ", host: " +
        hostName +
        ", ports: " + ports +
        ", networkLocation: " +
        getNetworkLocation() +
        ", certSerialId: " + certSerialId +
        ", persistedOpState: " + persistedOpState +
        ", persistedOpStateExpiryEpochSec: " + persistedOpStateExpiryEpochSec +
        "}";
  }

  @Override
  public int compareTo(DatanodeDetails that) {
    return this.id.compareTo(that.id);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DatanodeDetails &&
        id.equals(((DatanodeDetails) obj).id);
  }

  /**
   * Checks hostname, ipAddress and port of the 2 nodes are the same.
   * @param datanodeDetails dnDetails object to compare with.
   * @return true if the values match otherwise false.
   */
  public boolean compareNodeValues(DatanodeDetails datanodeDetails) {
    if (this == datanodeDetails || super.equals(datanodeDetails)) {
      return true;
    }
    return Objects.equals(ipAddress, datanodeDetails.ipAddress)
        && Objects.equals(hostName, datanodeDetails.hostName) && Objects.equals(ports, datanodeDetails.ports);
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  /**
   * Returns DatanodeDetails.Builder instance.
   *
   * @return DatanodeDetails.Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  @JsonIgnore
  public String threadNamePrefix() {
    return threadNamePrefix;
  }

  /**
   * Builder class for building DatanodeDetails.
   */
  public static final class Builder {
    private DatanodeID id;
    private StringWithByteString ipAddress;
    private StringWithByteString hostName;
    private StringWithByteString networkName;
    private StringWithByteString networkLocation;
    private int level;
    private List<Port> ports;
    private String certSerialId;
    private String version;
    private long setupTime;
    private String revision;
    private HddsProtos.NodeOperationalState persistedOpState;
    private long persistedOpStateExpiryEpochSec = 0;
    private int initialVersion;
    private int currentVersion = HDDSVersion.CURRENT_VERSION;

    /**
     * Default private constructor. To create Builder instance use
     * DatanodeDetails#newBuilder.
     */
    private Builder() {
      ports = new ArrayList<>();
    }

    /**
     * Initialize with DatanodeDetails.
     *
     * @param details DatanodeDetails
     * @return DatanodeDetails.Builder
     */
    public Builder setDatanodeDetails(DatanodeDetails details) {
      this.id = details.id;
      this.ipAddress = details.getIpAddressAsByteString();
      this.hostName = details.getHostNameAsByteString();
      this.networkName = details.getHostNameAsByteString();
      this.networkLocation = details.getNetworkLocationAsByteString();
      this.level = details.getLevel();
      this.ports = details.getPorts();
      this.certSerialId = details.getCertSerialId();
      this.version = details.getVersion();
      this.setupTime = details.getSetupTime();
      this.revision = details.getRevision();
      this.persistedOpState = details.getPersistedOpState();
      this.persistedOpStateExpiryEpochSec =
          details.getPersistedOpStateExpiryEpochSec();
      return this;
    }

    /**
     * Sets the DatanodeUuid.
     *
     * @param uuid DatanodeUuid
     * @return DatanodeDetails.Builder
     */
    public Builder setUuid(UUID uuid) {
      this.id = DatanodeID.of(uuid);
      return this;
    }

    public Builder setID(DatanodeID dnId) {
      this.id = dnId;
      return this;
    }

    /**
     * Sets the IP address of DataNode.
     *
     * @param ip address
     * @return DatanodeDetails.Builder
     */
    public Builder setIpAddress(String ip) {
      this.ipAddress = StringWithByteString.valueOf(ip);
      return this;
    }

    /**
     * Sets the IP address of DataNode.
     *
     * @param ip address
     * @param ipBytes address in Bytes
     * @return DatanodeDetails.Builder
     */
    public Builder setIpAddress(String ip, ByteString ipBytes) {
      this.ipAddress = new StringWithByteString(ip, ipBytes);
      return this;
    }

    /**
     * Sets the hostname of DataNode.
     *
     * @param host hostname
     * @return DatanodeDetails.Builder
     */
    public Builder setHostName(String host) {
      this.hostName = StringWithByteString.valueOf(host);
      return this;
    }

    /**
     * Sets the hostname of DataNode.
     *
     * @param host hostname
     * @param hostBytes hostname
     * @return DatanodeDetails.Builder
     */
    public Builder setHostName(String host, ByteString hostBytes) {
      this.hostName = new StringWithByteString(host, hostBytes);
      return this;
    }

    /**
     * Sets the network name of DataNode.
     *
     * @param name network name
     * @param nameBytes network name
     * @return DatanodeDetails.Builder
     */
    public Builder setNetworkName(String name, ByteString nameBytes) {
      this.networkName = new StringWithByteString(name, nameBytes);
      return this;
    }

    /**
     * Sets the network location of DataNode.
     *
     * @param loc location
     * @return DatanodeDetails.Builder
     */
    public Builder setNetworkLocation(String loc) {
      return setNetworkLocation(loc, null);
    }

    public Builder setNetworkLocation(String loc, ByteString locBytes) {
      final String normalized = NetUtils.normalize(loc);
      this.networkLocation = normalized.equals(loc) && locBytes != null
          ? new StringWithByteString(normalized, locBytes)
          : StringWithByteString.valueOf(normalized);
      return this;
    }

    public Builder setLevel(int level) {
      this.level = level;
      return this;
    }

    /**
     * Adds a DataNode Port.
     *
     * @param port DataNode port
     *
     * @return DatanodeDetails.Builder
     */
    public Builder addPort(Port port) {
      this.ports.add(port);
      return this;
    }

    /**
     * Adds certificate serial id.
     *
     * @param certId Serial id of SCM issued certificate.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setCertSerialId(String certId) {
      this.certSerialId = certId;
      return this;
    }

    /**
     * Sets the DataNode version.
     *
     * @param ver the version of DataNode.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setVersion(String ver) {
      this.version = ver;
      return this;
    }

    /**
     * Sets the DataNode revision.
     *
     * @param rev the revision of DataNode.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setRevision(String rev) {
      this.revision = rev;
      return this;
    }

    /**
     * Sets the DataNode setup time.
     *
     * @param time the setup time of DataNode.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setSetupTime(long time) {
      this.setupTime = time;
      return this;
    }

    /*
     * Adds persistedOpState.
     *
     * @param state The operational state persisted on the datanode
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setPersistedOpState(HddsProtos.NodeOperationalState state) {
      this.persistedOpState = state;
      return this;
    }

    /*
     * Adds persistedOpStateExpiryEpochSec.
     *
     * @param expiry The seconds after the epoch the operational state should
     *              expire.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setPersistedOpStateExpiry(long expiry) {
      this.persistedOpStateExpiryEpochSec = expiry;
      return this;
    }

    public Builder setInitialVersion(int v) {
      this.initialVersion = v;
      return this;
    }

    public Builder setCurrentVersion(int v) {
      this.currentVersion = v;
      return this;
    }

    /**
     * Builds and returns DatanodeDetails instance.
     *
     * @return DatanodeDetails
     */
    public DatanodeDetails build() {
      Objects.requireNonNull(id, "id == null");
      if (networkLocation == null || networkLocation.getString().isEmpty()) {
        networkLocation = NetConstants.BYTE_STRING_DEFAULT_RACK;
      }
      return new DatanodeDetails(this);
    }
  }

  /**
   * Constructs a new Port with name and value.
   *
   * @param name Name of the port
   * @param value Port number
   *
   * @return {@code Port} instance
   */
  public static Port newPort(Port.Name name, Integer value) {
    return new Port(name, value);
  }

  /**
   * Constructs a new Ratis Port with the given port number.
   *
   * @param portNumber Port number
   * @return the {@link Port} instance
   */
  public static Port newRatisPort(Integer portNumber) {
    return newPort(Name.RATIS, portNumber);
  }

  /**
   * Constructs a new REST Port with the given port number.
   *
   * @param portNumber Port number
   * @return the {@link Port} instance
   */
  public static Port newRestPort(Integer portNumber) {
    return newPort(Name.REST, portNumber);
  }

  /**
   * Constructs a new Standalone Port with the given port number.
   *
   * @param portNumber Port number
   * @return the {@link Port} instance
   */
  public static Port newStandalonePort(Integer portNumber) {
    return newPort(Name.STANDALONE, portNumber);
  }

  /**
   * Container to hold DataNode Port details.
   */
  public static final class Port {
    private final Name name;
    private final Integer value;

    /**
     * Ports that are supported in DataNode.
     */
    public enum Name {
      STANDALONE, RATIS, REST, REPLICATION, RATIS_ADMIN, RATIS_SERVER,
      @BelongsToHDDSLayoutVersion(RATIS_DATASTREAM_PORT_IN_DATANODEDETAILS)
      RATIS_DATASTREAM,
      @BelongsToHDDSLayoutVersion(WEBUI_PORTS_IN_DATANODEDETAILS)
      HTTP,
      @BelongsToHDDSLayoutVersion(WEBUI_PORTS_IN_DATANODEDETAILS)
      HTTPS,
      @BelongsToHDDSLayoutVersion(HADOOP_PRC_PORTS_IN_DATANODEDETAILS)
      CLIENT_RPC;

      public static final Set<Name> ALL_PORTS = ImmutableSet.copyOf(
          Name.values());
      public static final Set<Name> V0_PORTS = ImmutableSet.copyOf(
          EnumSet.of(STANDALONE, RATIS, REST));

      public static final Set<Name> IO_PORTS = ImmutableSet.copyOf(
          EnumSet.of(STANDALONE, RATIS, RATIS_DATASTREAM));
    }

    /**
     * Private constructor for constructing Port object. Use
     * DatanodeDetails#newPort to create a new Port object.
     */
    private Port(Name name, Integer value) {
      this.name = name;
      this.value = value;
    }

    /**
     * Returns the name of the port.
     *
     * @return Port name
     */
    public Name getName() {
      return name;
    }

    /**
     * Returns the port number.
     *
     * @return Port number
     */
    public Integer getValue() {
      return value;
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    /**
     * Ports are considered equal if they have the same name.
     *
     * @param anObject
     *          The object to compare this {@code Port} against
     * @return {@code true} if the given object represents a {@code Port}
               and has the same name, {@code false} otherwise
     */
    @Override
    public boolean equals(Object anObject) {
      if (this == anObject) {
        return true;
      }
      if (anObject instanceof Port) {
        return name.equals(((Port) anObject).name);
      }
      return false;
    }

    @Override
    public String toString() {
      return name + "=" + value;
    }

    public HddsProtos.Port toProto() {
      return HddsProtos.Port.newBuilder()
          .setName(name.name())
          .setValue(value)
          .build();
    }

    public static Port fromProto(HddsProtos.Port proto) {
      Port.Name name = Port.Name.valueOf(proto.getName().toUpperCase());
      return new Port(name, proto.getValue());
    }
  }

  /**
   * Returns serial id of SCM issued certificate.
   *
   * @return certificate serial id
   */
  public String getCertSerialId() {
    return certSerialId;
  }

  /**
   * Set certificate serial id of SCM issued certificate.
   *
   */
  public void setCertSerialId(String certSerialId) {
    this.certSerialId = certSerialId;
  }

  /**
   * Returns the DataNode version.
   *
   * @return DataNode version
   */
  public String getVersion() {
    return version;
  }

  /**
   * Set DataNode version.
   *
   * @param version DataNode version
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Returns the DataNode setup time.
   *
   * @return DataNode setup time
   */
  public long getSetupTime() {
    return setupTime;
  }

  /**
   * Set DataNode setup time.
   *
   * @param setupTime DataNode setup time
   */
  public void setSetupTime(long setupTime) {
    this.setupTime = setupTime;
  }

  /**
   * Returns the DataNode revision.
   *
   * @return DataNode revision
   */
  public String getRevision() {
    return revision;
  }

  /**
   * Set DataNode revision.
   *
   * @param rev DataNode revision
   */
  public void setRevision(String rev) {
    this.revision = rev;
  }

  @Override
  public HddsProtos.NetworkNode toProtobuf(
      int clientVersion) {
    return HddsProtos.NetworkNode.newBuilder()
        .setDatanodeDetails(toProtoBuilder(clientVersion, Collections.emptySet()).build())
        .build();
  }
}
