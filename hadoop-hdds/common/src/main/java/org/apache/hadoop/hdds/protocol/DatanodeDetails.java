/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hdds.protocol;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.hdds.DatanodeVersion;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedDatanodeDetailsProto;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NodeImpl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.hdds.upgrade.BelongsToHDDSLayoutVersion;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.ozone.ClientVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.HADOOP_PRC_PORTS_IN_DATANODEDETAILS;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.WEBUI_PORTS_IN_DATANODEDETAILS;
import static org.apache.hadoop.hdds.upgrade.HDDSLayoutFeature.RATIS_DATASTREAM_PORT_IN_DATANODEDETAILS;
import static org.apache.hadoop.ozone.ClientVersion.VERSION_HANDLES_UNKNOWN_DN_PORTS;

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
public class DatanodeDetails extends NodeImpl implements
    Comparable<DatanodeDetails> {

  private static final Logger LOG =
      LoggerFactory.getLogger(DatanodeDetails.class);

  private static final Codec<DatanodeDetails> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(ExtendedDatanodeDetailsProto.getDefaultInstance()),
      DatanodeDetails::getFromProtoBuf,
      DatanodeDetails::getExtendedProtoBufMessage);

  public static Codec<DatanodeDetails> getCodec() {
    return CODEC;
  }

  /**
   * DataNode's unique identifier in the cluster.
   */
  private final UUID uuid;
  private final String uuidString;
  private final String threadNamePrefix;

  private String ipAddress;
  private String hostName;
  private final List<Port> ports;
  private String certSerialId;
  private String version;
  private long setupTime;
  private String revision;
  private String buildDate;
  private volatile HddsProtos.NodeOperationalState persistedOpState;
  private volatile long persistedOpStateExpiryEpochSec;
  private int initialVersion;
  private int currentVersion;

  private DatanodeDetails(Builder b) {
    super(b.hostName, b.networkLocation, NetConstants.NODE_COST_DEFAULT);
    uuid = b.id;
    uuidString = uuid.toString();
    threadNamePrefix = HddsUtils.threadNamePrefix(uuidString);
    ipAddress = b.ipAddress;
    hostName = b.hostName;
    ports = b.ports;
    certSerialId = b.certSerialId;
    version = b.version;
    setupTime = b.setupTime;
    revision = b.revision;
    buildDate = b.buildDate;
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
    super(datanodeDetails.getHostName(), datanodeDetails.getNetworkLocation(),
        datanodeDetails.getParent(), datanodeDetails.getLevel(),
        datanodeDetails.getCost());
    this.uuid = datanodeDetails.uuid;
    this.uuidString = uuid.toString();
    threadNamePrefix = HddsUtils.threadNamePrefix(uuidString);
    this.ipAddress = datanodeDetails.ipAddress;
    this.hostName = datanodeDetails.hostName;
    this.ports = datanodeDetails.ports;
    this.certSerialId = datanodeDetails.certSerialId;
    this.setNetworkName(datanodeDetails.getNetworkName());
    this.setParent(datanodeDetails.getParent());
    this.version = datanodeDetails.version;
    this.setupTime = datanodeDetails.setupTime;
    this.revision = datanodeDetails.revision;
    this.buildDate = datanodeDetails.buildDate;
    this.persistedOpState = datanodeDetails.getPersistedOpState();
    this.persistedOpStateExpiryEpochSec =
        datanodeDetails.getPersistedOpStateExpiryEpochSec();
    this.initialVersion = datanodeDetails.getInitialVersion();
    this.currentVersion = datanodeDetails.getCurrentVersion();
  }

  /**
   * Returns the DataNode UUID.
   *
   * @return UUID of DataNode
   */
  public UUID getUuid() {
    return uuid;
  }

  /**
   * Returns the string representation of DataNode UUID.
   *
   * @return UUID of DataNode
   */
  public String getUuidString() {
    return uuidString;
  }

  /**
   * Sets the IP address of Datanode.
   *
   * @param ip IP Address
   */
  public void setIpAddress(String ip) {
    this.ipAddress = ip;
  }

  /**
   * Returns IP address of DataNode.
   *
   * @return IP address
   */
  public String getIpAddress() {
    return ipAddress;
  }

  /**
   * Sets the Datanode hostname.
   *
   * @param host hostname
   */
  public void setHostName(String host) {
    this.hostName = host;
  }

  /**
   * Returns Hostname of DataNode.
   *
   * @return Hostname
   */
  public String getHostName() {
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

  /**
   * Returns all the Ports used by DataNode.
   *
   * @return DataNode Ports
   */
  public synchronized List<Port> getPorts() {
    return ports;
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
    for (Port port : ports) {
      if (port.getName().equals(name)) {
        return port;
      }
    }
    // if no separate admin/server/datastream port, return single Ratis one for
    // compat
    if (name == Name.RATIS_ADMIN || name == Name.RATIS_SERVER ||
        name == Name.RATIS_DATASTREAM) {
      return getPort(Name.RATIS);
    }
    return null;
  }

  /**
   * Starts building a new DatanodeDetails from the protobuf input.
   *
   * @param datanodeDetailsProto protobuf message
   */
  public static DatanodeDetails.Builder newBuilder(
      HddsProtos.DatanodeDetailsProto datanodeDetailsProto) {
    DatanodeDetails.Builder builder = newBuilder();
    if (datanodeDetailsProto.hasUuid128()) {
      HddsProtos.UUID uuid = datanodeDetailsProto.getUuid128();
      builder.setUuid(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
    } else if (datanodeDetailsProto.hasUuid()) {
      builder.setUuid(UUID.fromString(datanodeDetailsProto.getUuid()));
    }

    if (datanodeDetailsProto.hasIpAddress()) {
      builder.setIpAddress(datanodeDetailsProto.getIpAddress());
    }
    if (datanodeDetailsProto.hasHostName()) {
      builder.setHostName(datanodeDetailsProto.getHostName());
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
      builder.setNetworkName(datanodeDetailsProto.getNetworkName());
    }
    if (datanodeDetailsProto.hasNetworkLocation()) {
      builder.setNetworkLocation(datanodeDetailsProto.getNetworkLocation());
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
    if (extendedDetailsProto.hasBuildDate()) {
      builder.setBuildDate(extendedDetailsProto.getBuildDate());
    }
    return builder.build();
  }

  /**
   * Returns a DatanodeDetails protobuf message from a datanode ID.
   * @return HddsProtos.DatanodeDetailsProto
   */
  @JsonIgnore
  public HddsProtos.DatanodeDetailsProto getProtoBufMessage() {
    return toProto(ClientVersion.CURRENT_VERSION);
  }

  public HddsProtos.DatanodeDetailsProto toProto(int clientVersion) {
    return toProtoBuilder(clientVersion).build();
  }

  public HddsProtos.DatanodeDetailsProto.Builder toProtoBuilder(
      int clientVersion) {

    HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
        .setMostSigBits(uuid.getMostSignificantBits())
        .setLeastSigBits(uuid.getLeastSignificantBits())
        .build();

    HddsProtos.DatanodeDetailsProto.Builder builder =
        HddsProtos.DatanodeDetailsProto.newBuilder()
            .setUuid128(uuid128);

    builder.setUuid(getUuidString());

    if (ipAddress != null) {
      builder.setIpAddress(ipAddress);
    }
    if (hostName != null) {
      builder.setHostName(hostName);
    }
    if (certSerialId != null) {
      builder.setCertSerialId(certSerialId);
    }
    if (!Strings.isNullOrEmpty(getNetworkName())) {
      builder.setNetworkName(getNetworkName());
    }
    if (!Strings.isNullOrEmpty(getNetworkLocation())) {
      builder.setNetworkLocation(getNetworkLocation());
    }
    if (getLevel() > 0) {
      builder.setLevel(getLevel());
    }
    if (persistedOpState != null) {
      builder.setPersistedOpState(persistedOpState);
    }
    builder.setPersistedOpStateExpiry(persistedOpStateExpiryEpochSec);

    final boolean handlesUnknownPorts =
        ClientVersion.fromProtoValue(clientVersion)
        .compareTo(VERSION_HANDLES_UNKNOWN_DN_PORTS) >= 0;
    for (Port port : ports) {
      if (handlesUnknownPorts || Name.V0_PORTS.contains(port.getName())) {
        builder.addPorts(port.toProto());
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Skip adding {} port {} to proto message for client v{}",
              port.getName(), port.getValue(), clientVersion);
        }
      }
    }

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
    if (!Strings.isNullOrEmpty(getBuildDate())) {
      extendedBuilder.setBuildDate(getBuildDate());
    }

    return extendedBuilder.build();
  }

  /**
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
    return uuidString + "(" + hostName + "/" + ipAddress + ")";
  }

  public String toDebugString() {
    return uuid.toString() + "{" +
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
    return this.getUuid().compareTo(that.getUuid());
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof DatanodeDetails &&
        uuid.equals(((DatanodeDetails) obj).uuid);
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
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
    private UUID id;
    private String ipAddress;
    private String hostName;
    private String networkName;
    private String networkLocation;
    private int level;
    private List<Port> ports;
    private String certSerialId;
    private String version;
    private long setupTime;
    private String revision;
    private String buildDate;
    private HddsProtos.NodeOperationalState persistedOpState;
    private long persistedOpStateExpiryEpochSec = 0;
    private int initialVersion;
    private int currentVersion = DatanodeVersion.CURRENT_VERSION;

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
      this.id = details.getUuid();
      this.ipAddress = details.getIpAddress();
      this.hostName = details.getHostName();
      this.networkName = details.getNetworkName();
      this.networkLocation = details.getNetworkLocation();
      this.level = details.getLevel();
      this.ports = details.getPorts();
      this.certSerialId = details.getCertSerialId();
      this.version = details.getVersion();
      this.setupTime = details.getSetupTime();
      this.revision = details.getRevision();
      this.buildDate = details.getBuildDate();
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
      this.id = uuid;
      return this;
    }

    /**
     * Sets the IP address of DataNode.
     *
     * @param ip address
     * @return DatanodeDetails.Builder
     */
    public Builder setIpAddress(String ip) {
      this.ipAddress = ip;
      return this;
    }

    /**
     * Sets the hostname of DataNode.
     *
     * @param host hostname
     * @return DatanodeDetails.Builder
     */
    public Builder setHostName(String host) {
      this.hostName = host;
      return this;
    }

    /**
     * Sets the network name of DataNode.
     *
     * @param name network name
     * @return DatanodeDetails.Builder
     */
    public Builder setNetworkName(String name) {
      this.networkName = name;
      return this;
    }

    /**
     * Sets the network location of DataNode.
     *
     * @param loc location
     * @return DatanodeDetails.Builder
     */
    public Builder setNetworkLocation(String loc) {
      this.networkLocation = loc;
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
     * Sets the DataNode build date.
     *
     * @param date the build date of DataNode.
     *
     * @return DatanodeDetails.Builder
     */
    public Builder setBuildDate(String date) {
      this.buildDate = date;
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
      Preconditions.checkNotNull(id);
      if (networkLocation == null) {
        networkLocation = NetConstants.DEFAULT_RACK;
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
   * Container to hold DataNode Port details.
   */
  public static final class Port {

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
    }

    private final Name name;
    private final Integer value;

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

  /**
   * Returns the DataNode build date.
   *
   * @return DataNode build date
   */
  public String getBuildDate() {
    return buildDate;
  }

  /**
   * Set DataNode build date.
   *
   * @param date DataNode build date
   */
  public void setBuildDate(String date) {
    this.buildDate = date;
  }

  @Override
  public HddsProtos.NetworkNode toProtobuf(
      int clientVersion) {
    return HddsProtos.NetworkNode.newBuilder()
        .setDatanodeDetails(toProtoBuilder(clientVersion).build())
        .build();
  }
}
