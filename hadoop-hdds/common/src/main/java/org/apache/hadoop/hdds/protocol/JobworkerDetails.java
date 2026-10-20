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

import com.google.common.base.Strings;
import jakarta.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.ExtendedJobWorkDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerDetailsProto;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerPort;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.JobworkerPortType;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos.NodeOperationalState;
import org.apache.hadoop.hdds.scm.net.NetConstants;
import org.apache.hadoop.hdds.scm.net.NodeImpl;
import org.apache.hadoop.hdds.upgrade.JobworkerVersion;
import org.apache.hadoop.hdds.utils.db.Codec;
import org.apache.hadoop.hdds.utils.db.DelegatedCodec;
import org.apache.hadoop.hdds.utils.db.Proto2Codec;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobworkerDetails class contains details about Jobworker like:
 * - UUID of the Jobworker.
 * - IP and Hostname details.
 * - Port details to which the Jobworker will be listening.
 * and may also include some extra info like:
 * - version of the Jobworker
 * - setup time etc.
 */
public class JobworkerDetails extends NodeImpl implements
    Comparable<JobworkerDetails> {

  private static final Logger LOG =
      LoggerFactory.getLogger(JobworkerDetails.class);

  private static final Codec<JobworkerDetails> CODEC = new DelegatedCodec<>(
      Proto2Codec.get(ExtendedJobWorkDetailsProto.getDefaultInstance()),
      JobworkerDetails::getFromProtoBuf,
      JobworkerDetails::getExtendedProtoBufMessage,
      JobworkerDetails.class);

  /**
   * Get the codec for JobworkerDetails.
   *
   * @return The Codec instance
   */
  public static Codec<JobworkerDetails> getCodec() {
    return CODEC;
  }

  /**
   * Jobworker's unique identifier in the cluster.
   */
  private final UUID uuid;
  private String ipAddress;
  private String hostName;
  private Map<JobworkerPortType, Integer> ports;
  private String version;
  private long setupTime;
  private String buildDate;
  private volatile HddsProtos.NodeOperationalState operationalState;
  private String revision;
  private JobworkerVersion jobworkerVersion;

  /**
   * Constructs JobworkerDetails instance. JobworkerDetails.Builder is used
   * for instantiating JobworkerDetails.
   *
   * @param uuid             Jobworker's UUID
   * @param ipAddress        IP Address of this Jobworker
   * @param hostName         Jobworker's hostname
   * @param networkLocation  Jobworker's network location path
   * @param ports            Ports used by the Jobworker
   * @param version          Jobworker's version
   * @param setupTime        the setup time of Jobworker
   * @param buildDate        Jobworker's build timestamp
   * @param opState          Jobworker's NodeOperationalState
   * @param jobworkerVersion  Jobworker's JobworkerVersion
   */
  @SuppressWarnings("checkstyle:ParameterNumber")
  private JobworkerDetails(UUID uuid, String ipAddress, String hostName,
                           String networkLocation, Map<JobworkerPortType, Integer> ports,
                           String version, long setupTime, String buildDate,
                           NodeOperationalState opState, String revision,
                           JobworkerVersion jobworkerVersion) {
    super(hostName, networkLocation, NetConstants.NODE_COST_DEFAULT);
    this.uuid = uuid;
    this.ipAddress = ipAddress;
    this.hostName = hostName;
    this.ports = ports;
    this.version = version;
    this.setupTime = setupTime;
    this.buildDate = buildDate;
    this.operationalState = opState;
    this.revision = revision;
    this.jobworkerVersion = jobworkerVersion;
  }

  /**
   * Copy constructor for JobworkerDetails.
   *
   * @param jobworkerDetails JobworkerDetails to copy
   */
  public JobworkerDetails(JobworkerDetails jobworkerDetails) {
    super(jobworkerDetails.getHostName(), jobworkerDetails.getNetworkLocation(),
        jobworkerDetails.getParent(), jobworkerDetails.getLevel(),
        jobworkerDetails.getCost());
    this.uuid = jobworkerDetails.uuid;
    this.ipAddress = jobworkerDetails.ipAddress;
    this.hostName = jobworkerDetails.hostName;
    this.ports = jobworkerDetails.ports;
    this.setNetworkName(jobworkerDetails.getNetworkName());
    this.setParent(jobworkerDetails.getParent());
    this.version = jobworkerDetails.version;
    this.setupTime = jobworkerDetails.setupTime;
    this.buildDate = jobworkerDetails.buildDate;
    this.operationalState = jobworkerDetails.operationalState;
    this.revision = jobworkerDetails.revision;
    this.jobworkerVersion = jobworkerDetails.jobworkerVersion;
  }

  /**
   * Returns the Jobworker UUID.
   *
   * @return UUID of Jobworker
   */
  public UUID getUuid() {
    return uuid;
  }

  /**
   * Returns the string representation of Jobworker UUID.
   *
   * @return UUID of Jobworker as String
   */
  public String getUuidString() {
    return uuid.toString();
  }

  /**
   * Sets the IP address of Jobworker.
   *
   * @param ip IP Address
   */
  public void setIpAddress(String ip) {
    this.ipAddress = ip;
  }

  /**
   * Returns IP address of Jobworker.
   *
   * @return IP address
   */
  public String getIpAddress() {
    return ipAddress;
  }

  /**
   * Sets the Jobworker hostname.
   *
   * @param host hostname
   */
  public void setHostName(String host) {
    this.hostName = host;
  }

  /**
   * Returns Hostname of Jobworker.
   *
   * @return Hostname
   */
  public String getHostName() {
    return hostName;
  }

  /**
   * Returns all Ports used by Jobworker.
   *
   * @return Jobworker Ports
   */
  public Map<JobworkerPortType, Integer> getPorts() {
    return ports;
  }

  /**
   * Return the Jobworker's NodeOperationalState.
   *
   * @return Jobworker Ports
   */
  public NodeOperationalState getOperationalState() {
    return operationalState;
  }

  /**
   * Returns the Jobworker's JobworkerVersion.
   *
   * @return Jobworker's JobworkerVersion
   */
  public JobworkerVersion getJobworkerVersion() {
    return jobworkerVersion;
  }

  /**
   * Starts building a new JobworkerDetails from the protobuf input.
   *
   * @param jobworkerDetailsProto protobuf message
   */
  public static Builder newBuilder(JobworkerDetailsProto jobworkerDetailsProto) {
    Builder builder = newBuilder();
    if (jobworkerDetailsProto.hasUuid128()) {
      HddsProtos.UUID uuid = jobworkerDetailsProto.getUuid128();
      builder.setUuid(new UUID(uuid.getMostSigBits(), uuid.getLeastSigBits()));
    }
    if (jobworkerDetailsProto.hasIpAddress()) {
      builder.setIpAddress(jobworkerDetailsProto.getIpAddress());
    }
    if (jobworkerDetailsProto.hasHostName()) {
      builder.setHostName(jobworkerDetailsProto.getHostName());
    }
    for (JobworkerPort port : jobworkerDetailsProto.getPortsList()) {
      builder.addPort(port.getType(), port.getValue());
    }
    if (jobworkerDetailsProto.hasNetworkName()) {
      builder.setNetworkName(jobworkerDetailsProto.getNetworkName());
    }
    if (jobworkerDetailsProto.hasNetworkLocation()) {
      builder.setNetworkLocation(jobworkerDetailsProto.getNetworkLocation());
    }
    if (jobworkerDetailsProto.hasNodeOperationalState()) {
      builder.setOperationalState(jobworkerDetailsProto.getNodeOperationalState());
    }
    if (jobworkerDetailsProto.hasJobworkerVersion()) {
      builder.setJobworkerVersion(JobworkerVersion.fromProtoValue(
          jobworkerDetailsProto.getJobworkerVersion()));
    }
    return builder;
  }

  /**
   * Returns a JobworkerDetails from the protocol buffers.
   *
   * @param jobWorkDetailsProto - protoBuf Message
   * @return JobworkerDetail
   */
  public static JobworkerDetails getFromProtoBuf(
      JobworkerDetailsProto jobWorkDetailsProto) {
    return newBuilder(jobWorkDetailsProto).build();
  }

  /**
   * Returns a JobworkerDetails from the protocol buffers.
   *
   * @param extendedDetailsProto - protoBuf Message
   * @return JobworkerDetails
   */
  public static JobworkerDetails getFromProtoBuf(
      ExtendedJobWorkDetailsProto extendedDetailsProto) {
    Builder builder;
    if (extendedDetailsProto.hasJobworkerDetails()) {
      builder = newBuilder(extendedDetailsProto.getJobworkerDetails());
    } else {
      builder = newBuilder();
    }
    if (extendedDetailsProto.hasVersion()) {
      builder.setVersion(extendedDetailsProto.getVersion());
    }
    if (extendedDetailsProto.hasSetupTime()) {
      builder.setSetupTime(extendedDetailsProto.getSetupTime());
    }
    if (extendedDetailsProto.hasBuildDate()) {
      builder.setBuildDate(extendedDetailsProto.getBuildDate());
    }
    if (extendedDetailsProto.hasRevision()) {
      builder.setRevision(extendedDetailsProto.getRevision());
    }
    return builder.build();
  }

  /**
   * Returns a JobworkerDetailsProto protobuf message.
   *
   * @return JobworkerDetailsProto
   */
  public JobworkerDetailsProto getProtoBufMessage() {
    HddsProtos.UUID uuid128 = HddsProtos.UUID.newBuilder()
        .setMostSigBits(uuid.getMostSignificantBits())
        .setLeastSigBits(uuid.getLeastSignificantBits())
        .build();
    JobworkerDetailsProto.Builder builder =
        JobworkerDetailsProto.newBuilder()
            .setUuid128(uuid128);
    if (ipAddress != null) {
      builder.setIpAddress(ipAddress);
    }
    if (hostName != null) {
      builder.setHostName(hostName);
    }
    if (!Strings.isNullOrEmpty(getNetworkName())) {
      builder.setNetworkName(getNetworkName());
    }
    if (!Strings.isNullOrEmpty(getNetworkLocation())) {
      builder.setNetworkLocation(getNetworkLocation());
    }
    if (operationalState != null) {
      builder.setNodeOperationalState(operationalState);
    }
    for (JobworkerPortType portType : ports.keySet()) {
      builder.addPorts(JobworkerPort.newBuilder()
          .setType(portType)
          .setValue(ports.get(portType))
          .build());
    }
    if (jobworkerVersion != null) {
      builder.setJobworkerVersion(jobworkerVersion.toProtoValue());
    }
    return builder.build();
  }

  /**
   * Returns a ExtendedJobWorkDetailsProto protobuf message.
   *
   * @return ExtendedJobWorkDetailsProto
   */
  public ExtendedJobWorkDetailsProto getExtendedProtoBufMessage() {
    ExtendedJobWorkDetailsProto.Builder extendedBuilder =
        ExtendedJobWorkDetailsProto.newBuilder()
            .setJobworkerDetails(getProtoBufMessage());
    if (!Strings.isNullOrEmpty(getVersion())) {
      extendedBuilder.setVersion(getVersion());
    }
    extendedBuilder.setSetupTime(getSetupTime());
    if (!Strings.isNullOrEmpty(getBuildDate())) {
      extendedBuilder.setBuildDate(getBuildDate());
    }
    if (!Strings.isNullOrEmpty(getRevision())) {
      extendedBuilder.setRevision(getRevision());
    }
    return extendedBuilder.build();
  }

  @Override
  public String toString() {
    return uuid + "(" + hostName + "/" + ipAddress + ")";
  }

  public String toDebugString() {
    return uuid.toString() + "{" +
        "version: " + getVersion() +
        ", ip: " + getIpAddress() +
        ", host: " + getHostName() +
        ", ports: " + getPorts() +
        ", networkLocation: " + getNetworkLocation() +
        ", networkName: " + getNetworkName() +
        ", setupTime: " + Time.formatTime(getSetupTime()) +
        ", operationalState: " + getOperationalState() +
        ", jobworkerVersion: " + getJobworkerVersion() +
        "}";
  }

  @Override
  public int compareTo(@Nonnull JobworkerDetails that) {
    return this.getUuid().compareTo(that.getUuid());
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof JobworkerDetails &&
        uuid.equals(((JobworkerDetails) obj).uuid);
  }

  @Override
  public int hashCode() {
    return uuid.hashCode();
  }

  public int getSignature() {
    return Objects.hash(uuid, ipAddress, hostName, ports,
        version, setupTime, buildDate);
  }

  /**
   * Returns JobworkerDetails.Builder instance.
   *
   * @return JobworkerDetails.Builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Returns the Jobworker version.
   *
   * @return Jobworker version
   */
  public String getVersion() {
    return version;
  }

  /**
   * Set Jobworker version.
   *
   * @param version Jobworker version
   */
  public void setVersion(String version) {
    this.version = version;
  }

  /**
   * Returns the Jobworker setup time.
   *
   * @return Jobworker setup time
   */
  public long getSetupTime() {
    return setupTime;
  }

  /**
   * Set Jobworker setup time.
   *
   * @param setupTime Jobworker setup time
   */
  public void setSetupTime(long setupTime) {
    this.setupTime = setupTime;
  }

  /**
   * Returns the Jobworker build date.
   *
   * @return Jobworker build date
   */
  public String getBuildDate() {
    return buildDate;
  }

  /**
   * Set Jobworker build date.
   *
   * @param date Jobworker build date
   */
  public void setBuildDate(String date) {
    this.buildDate = date;
  }

  /**
   * Returns the Jobworker revision.
   *
   * @return Jobworker revision
   */
  public String getRevision() {
    return revision;
  }

  /**
   * Set Jobworker revision.
   *
   * @param rev Jobworker revision
   */
  public void setRevision(String rev) {
    this.revision = rev;
  }

  /**
   * Builder class for building JobworkerDetails.
   */
  public static final class Builder {
    private UUID uuid;
    private String ipAddress;
    private String hostName;
    private String networkName;
    private String networkLocation;
    private final Map<JobworkerPortType, Integer> ports;
    private String version;
    private long setupTime;
    private String buildDate;
    private HddsProtos.NodeOperationalState opState;
    private String revision;
    private JobworkerVersion jobworkerVersion;

    /**
     * Default private constructor. To create Builder instance use
     * JobworkerDetails#newBuilder.
     */
    private Builder() {
      ports = new HashMap<>();
    }

    /**
     * Sets the JobworkerUuid.
     *
     * @param uuid JobworkerUuid
     * @return JobworkerDetails.Builder
     */
    public Builder setUuid(UUID uuid) {
      this.uuid = uuid;
      return this;
    }

    /**
     * Sets the IP address of Jobworker.
     *
     * @param ip address
     * @return JobworkerDetails.Builder
     */
    public Builder setIpAddress(String ip) {
      this.ipAddress = ip;
      return this;
    }

    /**
     * Sets the hostname of Jobworker.
     *
     * @param host hostname
     * @return JobworkerDetails.Builder
     */
    public Builder setHostName(String host) {
      this.hostName = host;
      return this;
    }

    /**
     * Sets the network name of Jobworker.
     *
     * @param name network name
     * @return JobworkerDetails.Builder
     */
    public Builder setNetworkName(String name) {
      this.networkName = name;
      return this;
    }

    /**
     * Sets the network location of Jobworker.
     *
     * @param loc location
     * @return JobworkerDetails.Builder
     */
    public Builder setNetworkLocation(String loc) {
      this.networkLocation = loc;
      return this;
    }

    /**
     * Adds a Jobworker Port.
     *
     * @param portType Jobworker port Type
     * @param value    port value
     * @return JobworkerDetails.Builder
     */
    public Builder addPort(JobworkerPortType portType, int value) {
      this.ports.put(portType, value);
      return this;
    }

    /**
     * Sets the Jobworker version.
     *
     * @param ver the version of Jobworker.
     * @return JobworkerDetails.Builder
     */
    public Builder setVersion(String ver) {
      this.version = ver;
      return this;
    }

    /**
     * Sets the Jobworker build date.
     *
     * @param date the build date of Jobworker.
     * @return JobworkerDetails.Builder
     */
    public Builder setBuildDate(String date) {
      this.buildDate = date;
      return this;
    }

    /**
     * Sets the Jobworker setup time.
     *
     * @param time the setup time of Jobworker.
     * @return JobworkerDetails.Builder
     */
    public Builder setSetupTime(long time) {
      this.setupTime = time;
      return this;
    }

    public Builder setOperationalState(HddsProtos.NodeOperationalState state) {
      this.opState = state;
      return this;
    }

    /**
     * Sets the JobworkerVersion.
     * @param jobworkerVersion the JobworkerVersion of Jobworker.
     * @return JobworkerDetails.Builder
     */
    public Builder setJobworkerVersion(JobworkerVersion jobworkerVersion) {
      this.jobworkerVersion = jobworkerVersion;
      return this;
    }

    /**
     * Sets the jobworker revision.
     *
     * @param rev the revision of jobworker.
     *
     * @return JobworkerDetails.Builder
     */
    public JobworkerDetails.Builder setRevision(String rev) {
      this.revision = rev;
      return this;
    }

    /**
     * Builds and returns JobworkerDetails instance.
     *
     * @return JobworkerDetails
     */
    public JobworkerDetails build() {
      Objects.requireNonNull(uuid);
      if (networkLocation == null) {
        networkLocation = NetConstants.DEFAULT_RACK;
      }
      JobworkerDetails jw = new JobworkerDetails(uuid, ipAddress, hostName,
          networkLocation, ports, version, setupTime,
          buildDate, opState, revision, jobworkerVersion);
      if (networkName != null) {
        jw.setNetworkName(networkName);
      }
      return jw;
    }
  }
}
