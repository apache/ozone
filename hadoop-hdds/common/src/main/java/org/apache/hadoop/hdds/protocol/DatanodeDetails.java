/**
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
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hdds.annotation.InterfaceAudience;
import org.apache.hadoop.hdds.annotation.InterfaceStability;
import org.apache.hadoop.hdds.protocol.DatanodeDetails.Port.Name;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NodeImpl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

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
  /**
   * DataNode's unique identifier in the cluster.
   */
  private final UUID uuid;
  private final String uuidString;

  private String ipAddress;
  private String hostName;
  private List<Port> ports;
  private String certSerialId;
  private String version;
  private long setupTime;
  private String revision;
  private String buildDate;
  private HddsProtos.NodeOperationalState persistedOpState;
  private long persistedOpStateExpiryEpochSec = 0;

  /**
   * Constructs DatanodeDetails instance. DatanodeDetails.Builder is used
   * for instantiating DatanodeDetails.
   * @param uuid DataNode's UUID
   * @param ipAddress IP Address of this DataNode
   * @param hostName DataNode's hostname
   * @param networkLocation DataNode's network location path
   * @param ports Ports used by the DataNode
   * @param certSerialId serial id from SCM issued certificate.
   * @param version DataNode's version
   * @param setupTime the setup time of DataNode
   * @param revision DataNodes's revision
   * @param buildDate DataNodes's build timestamp
   * @param persistedOpState Operational State stored on DN.
   * @param persistedOpStateExpiryEpochSec Seconds after the epoch the stored
   *                                       state should expire.
   */
  @SuppressWarnings("parameternumber")
  private DatanodeDetails(UUID uuid, String ipAddress, String hostName,
      String networkLocation, List<Port> ports, String certSerialId,
      String version, long setupTime, String revision, String buildDate,
      HddsProtos.NodeOperationalState persistedOpState,
      long persistedOpStateExpiryEpochSec) {
    super(hostName, networkLocation, NetConstants.NODE_COST_DEFAULT);
    this.uuid = uuid;
    this.uuidString = uuid.toString();
    this.ipAddress = ipAddress;
    this.hostName = hostName;
    this.ports = ports;
    this.certSerialId = certSerialId;
    this.version = version;
    this.setupTime = setupTime;
    this.revision = revision;
    this.buildDate = buildDate;
    this.persistedOpState = persistedOpState;
    this.persistedOpStateExpiryEpochSec = persistedOpStateExpiryEpochSec;
  }

  public DatanodeDetails(DatanodeDetails datanodeDetails) {
    super(datanodeDetails.getHostName(), datanodeDetails.getNetworkLocation(),
        datanodeDetails.getCost());
    this.uuid = datanodeDetails.uuid;
    this.uuidString = uuid.toString();
    this.ipAddress = datanodeDetails.ipAddress;
    this.hostName = datanodeDetails.hostName;
    this.ports = datanodeDetails.ports;
    this.setNetworkName(datanodeDetails.getNetworkName());
    this.setParent(datanodeDetails.getParent());
    this.version = datanodeDetails.version;
    this.setupTime = datanodeDetails.setupTime;
    this.revision = datanodeDetails.revision;
    this.buildDate = datanodeDetails.buildDate;
    this.persistedOpState = datanodeDetails.getPersistedOpState();
    this.persistedOpStateExpiryEpochSec =
        datanodeDetails.getPersistedOpStateExpiryEpochSec();
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
  public void setPort(Port port) {
    // If the port is already in the list remove it first and add the
    // new/updated port value.
    ports.remove(port);
    ports.add(port);
  }

  public void setPort(Name name, int port) {
    setPort(new Port(name, port));
  }

  /**
   * Returns all the Ports used by DataNode.
   *
   * @return DataNode Ports
   */
  public List<Port> getPorts() {
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
  public Port getPort(Port.Name name) {
    for (Port port : ports) {
      if (port.getName().equals(name)) {
        return port;
      }
    }
    return null;
  }

  /**
   * Returns a DatanodeDetails from the protocol buffers.
   *
   * @param datanodeDetailsProto - protoBuf Message
   * @return DatanodeDetails
   */
  public static DatanodeDetails getFromProtoBuf(
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
      builder.addPort(newPort(
          Port.Name.valueOf(port.getName().toUpperCase()), port.getValue()));
    }
    if (datanodeDetailsProto.hasNetworkName()) {
      builder.setNetworkName(datanodeDetailsProto.getNetworkName());
    }
    if (datanodeDetailsProto.hasNetworkLocation()) {
      builder.setNetworkLocation(datanodeDetailsProto.getNetworkLocation());
    }
    if (datanodeDetailsProto.hasPersistedOpState()) {
      builder.setPersistedOpState(datanodeDetailsProto.getPersistedOpState());
    }
    if (datanodeDetailsProto.hasPersistedOpStateExpiry()) {
      builder.setPersistedOpStateExpiry(
          datanodeDetailsProto.getPersistedOpStateExpiry());
    }
    return builder.build();
  }

  /**
   * Returns a ExtendedDatanodeDetails from the protocol buffers.
   *
   * @param extendedDetailsProto - protoBuf Message
   * @return DatanodeDetails
   */
  public static DatanodeDetails getFromProtoBuf(
      HddsProtos.ExtendedDatanodeDetailsProto extendedDetailsProto) {
    DatanodeDetails.Builder builder = newBuilder();
    if (extendedDetailsProto.hasDatanodeDetails()) {
      DatanodeDetails datanodeDetails = getFromProtoBuf(
          extendedDetailsProto.getDatanodeDetails());
      builder.setDatanodeDetails(datanodeDetails);
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
  public HddsProtos.DatanodeDetailsProto getProtoBufMessage() {
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
    if (persistedOpState != null) {
      builder.setPersistedOpState(persistedOpState);
    }
    builder.setPersistedOpStateExpiry(persistedOpStateExpiryEpochSec);

    for (Port port : ports) {
      builder.addPorts(HddsProtos.Port.newBuilder()
          .setName(port.getName().toString())
          .setValue(port.getValue())
          .build());
    }

    return builder.build();
  }

  /**
   * Returns a ExtendedDatanodeDetails protobuf message from a datanode ID.
   * @return HddsProtos.ExtendedDatanodeDetailsProto
   */
  public HddsProtos.ExtendedDatanodeDetailsProto getExtendedProtoBufMessage() {
    HddsProtos.DatanodeDetailsProto datanodeDetailsProto = getProtoBufMessage();

    HddsProtos.ExtendedDatanodeDetailsProto.Builder extendedBuilder =
        HddsProtos.ExtendedDatanodeDetailsProto.newBuilder()
            .setDatanodeDetails(datanodeDetailsProto);

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

  @Override
  public String toString() {
    return uuid.toString() + "{" +
        "ip: " +
        ipAddress +
        ", host: " +
        hostName +
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

  /**
   * Builder class for building DatanodeDetails.
   */
  public static final class Builder {
    private UUID id;
    private String ipAddress;
    private String hostName;
    private String networkName;
    private String networkLocation;
    private List<Port> ports;
    private String certSerialId;
    private String version;
    private long setupTime;
    private String revision;
    private String buildDate;
    private HddsProtos.NodeOperationalState persistedOpState;
    private long persistedOpStateExpiryEpochSec = 0;

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
    public Builder setPersistedOpState(HddsProtos.NodeOperationalState state){
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
    public Builder setPersistedOpStateExpiry(long expiry){
      this.persistedOpStateExpiryEpochSec = expiry;
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
      DatanodeDetails dn = new DatanodeDetails(id, ipAddress, hostName,
          networkLocation, ports, certSerialId, version, setupTime, revision,
          buildDate, persistedOpState, persistedOpStateExpiryEpochSec);
      if (networkName != null) {
        dn.setNetworkName(networkName);
      }
      return dn;
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
      STANDALONE, RATIS, REST, REPLICATION
    }

    private Name name;
    private Integer value;

    /**
     * Private constructor for constructing Port object. Use
     * DatanodeDetails#newPort to create a new Port object.
     *
     * @param name
     * @param value
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
}
